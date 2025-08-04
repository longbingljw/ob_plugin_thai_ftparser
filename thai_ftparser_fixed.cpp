/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <new>
#include <Python.h>
#include <pthread.h>

#include "oceanbase/ob_plugin_ftparser.h"

/**
 * @defgroup ThaiFtParser Thai Fulltext Parser Plugin
 * @brief This is a Thai language fulltext parser plugin using thai-tokenizer
 * @{
 */

namespace oceanbase {
namespace thai {

// 全局静态变量，确保线程安全
static pthread_mutex_t g_python_mutex = PTHREAD_MUTEX_INITIALIZER;
static bool g_python_initialized = false;
static PyObject* g_pModule = nullptr;
static PyObject* g_pTokenizerClass = nullptr;
static int g_ref_count = 0;

class ObThaiFTParser final
{
public:
  ObThaiFTParser() = default;
  virtual ~ObThaiFTParser();

  int init(ObPluginFTParserParamPtr param);
  void reset();
  int get_next_token(
      const char *&word,
      int64_t &word_len,
      int64_t &char_len,
      int64_t &word_freq);

private:
  int initialize_python_global();
  int tokenize_text();
  int tokenize_with_spaces();
  int is_thai_text(const char* text, int64_t len);
  void cleanup_python_global();
  
  ObPluginDatum  cs_   = 0;
  const char *   start_     = nullptr;
  const char *   next_      = nullptr;
  const char *   end_       = nullptr;
  bool           is_inited_ = false;
  
  // Python相关 - 现在每个实例都有自己的tokenizer
  PyObject* pTokenizer_ = nullptr;
  PyObject* pSplitFunc_ = nullptr;
  
  // 分词结果
  char** tokens_ = nullptr;
  int token_count_ = 0;
  int current_token_index_ = 0;
};

ObThaiFTParser::~ObThaiFTParser()
{
  reset();
}

void ObThaiFTParser::reset()
{
  cs_ = 0;
  start_ = nullptr;
  next_ = nullptr;
  end_ = nullptr;
  is_inited_ = false;
  current_token_index_ = 0;
  
  // 清理tokens
  if (tokens_) {
    for (int i = 0; i < token_count_; i++) {
      if (tokens_[i]) {
        free(tokens_[i]);
      }
    }
    free(tokens_);
    tokens_ = nullptr;
  }
  token_count_ = 0;
  
  cleanup_python_global();
}

int ObThaiFTParser::init(ObPluginFTParserParamPtr param)
{
  int ret = OBP_SUCCESS;
  const char *fulltext = obp_ftparser_fulltext(param);
  int64_t ft_length = obp_ftparser_fulltext_length(param);
  ObPluginCharsetInfoPtr cs = obp_ftparser_charset_info(param);

  if (is_inited_) {
    ret = OBP_INIT_TWICE;
    OBP_LOG_WARN("init twice. ret=%d, param=%p, this=%p", ret, param, this);
  } else if (0 == param
      || 0 == cs
      || nullptr == fulltext
      || 0 >= ft_length) {
    ret = OBP_INVALID_ARGUMENT;
    OBP_LOG_WARN("invalid arguments, ret=%d, param=%p", ret, param);
  } else {
    cs_ = cs;
    start_ = fulltext;
    next_ = start_;
    end_ = start_ + ft_length;
    is_inited_ = true;
    current_token_index_ = 0;
    
    // 检查是否为泰语文本
    if (is_thai_text(fulltext, ft_length)) {
      OBP_LOG_INFO("Detected Thai text, initializing Python tokenizer");
      ret = initialize_python_global();
      if (ret == OBP_SUCCESS) {
        OBP_LOG_INFO("Python initialized successfully, tokenizing text");
        ret = tokenize_text();
      } else {
        // Python初始化失败，使用空格分词作为回退
        OBP_LOG_WARN("Python initialization failed, falling back to space tokenization");
        ret = tokenize_with_spaces();
      }
    } else {
      OBP_LOG_INFO("Non-Thai text detected, using space tokenization");
      ret = tokenize_with_spaces();
    }
  }
  if (ret != OBP_SUCCESS && !is_inited_) {
    reset();
  }
  OBP_LOG_INFO("thai ftparser init done. ret=%d", ret);
  return ret;
}

int ObThaiFTParser::initialize_python_global()
{
  pthread_mutex_lock(&g_python_mutex);
  
  try {
    // 初始化全局Python环境（只做一次）
    if (!g_python_initialized) {
      if (!Py_IsInitialized()) {
        Py_Initialize();
        if (!Py_IsInitialized()) {
          OBP_LOG_WARN("Failed to initialize Python interpreter");
          pthread_mutex_unlock(&g_python_mutex);
          return OBP_PLUGIN_ERROR;
        }
      }
      
      // 导入thai_tokenizer模块
      g_pModule = PyImport_ImportModule("thai_tokenizer");
      if (!g_pModule) {
        OBP_LOG_WARN("Failed to import thai_tokenizer module");
        PyErr_Print(); // 打印Python错误信息
        pthread_mutex_unlock(&g_python_mutex);
        return OBP_PLUGIN_ERROR;
      }
      
      // 获取Tokenizer类
      g_pTokenizerClass = PyObject_GetAttrString(g_pModule, "Tokenizer");
      if (!g_pTokenizerClass) {
        OBP_LOG_WARN("Failed to get Tokenizer class");
        PyErr_Print();
        Py_DECREF(g_pModule);
        g_pModule = nullptr;
        pthread_mutex_unlock(&g_python_mutex);
        return OBP_PLUGIN_ERROR;
      }
      
      g_python_initialized = true;
    }
    
    // 为当前实例创建Tokenizer实例
    PyGILState_STATE gstate = PyGILState_Ensure();
    
    pTokenizer_ = PyObject_CallObject(g_pTokenizerClass, nullptr);
    if (!pTokenizer_) {
      OBP_LOG_WARN("Failed to create Tokenizer instance");
      PyErr_Print();
      PyGILState_Release(gstate);
      pthread_mutex_unlock(&g_python_mutex);
      return OBP_PLUGIN_ERROR;
    }
    
    // 获取split方法
    pSplitFunc_ = PyObject_GetAttrString(pTokenizer_, "split");
    if (!pSplitFunc_) {
      OBP_LOG_WARN("Failed to get split method");
      PyErr_Print();
      Py_DECREF(pTokenizer_);
      pTokenizer_ = nullptr;
      PyGILState_Release(gstate);
      pthread_mutex_unlock(&g_python_mutex);
      return OBP_PLUGIN_ERROR;
    }
    
    g_ref_count++;
    PyGILState_Release(gstate);
    pthread_mutex_unlock(&g_python_mutex);
    return OBP_SUCCESS;
    
  } catch (const std::exception& e) {
    OBP_LOG_WARN("Python initialization failed: %s", e.what());
    pthread_mutex_unlock(&g_python_mutex);
    return OBP_PLUGIN_ERROR;
  } catch (...) {
    OBP_LOG_WARN("Python initialization failed with unknown exception");
    pthread_mutex_unlock(&g_python_mutex);
    return OBP_PLUGIN_ERROR;
  }
}

int ObThaiFTParser::tokenize_text()
{
  if (!g_python_initialized || !is_inited_ || !pTokenizer_ || !pSplitFunc_) {
    return OBP_PLUGIN_ERROR;
  }
  
  PyGILState_STATE gstate = PyGILState_Ensure();
  
  try {
    // 创建Python字符串
    PyObject* pText = PyUnicode_FromStringAndSize(start_, (Py_ssize_t)(end_ - start_));
    if (!pText) {
      OBP_LOG_WARN("Failed to create Python string");
      PyErr_Print();
      PyGILState_Release(gstate);
      return OBP_PLUGIN_ERROR;
    }
    
    // 调用split函数
    PyObject* pArgs = PyTuple_Pack(1, pText);
    if (!pArgs) {
      OBP_LOG_WARN("Failed to create arguments tuple");
      Py_DECREF(pText);
      PyGILState_Release(gstate);
      return OBP_PLUGIN_ERROR;
    }
    
    PyObject* pResult = PyObject_CallObject(pSplitFunc_, pArgs);
    
    Py_DECREF(pText);
    Py_DECREF(pArgs);
    
    if (!pResult) {
      OBP_LOG_WARN("Failed to call split function");
      PyErr_Print();
      PyGILState_Release(gstate);
      return OBP_PLUGIN_ERROR;
    }
    
    // 解析结果
    if (!PyList_Check(pResult)) {
      OBP_LOG_WARN("Split result is not a list");
      Py_DECREF(pResult);
      PyGILState_Release(gstate);
      return OBP_PLUGIN_ERROR;
    }
    
    Py_ssize_t size = PyList_Size(pResult);
    token_count_ = (int)size;
    
    // 分配内存
    tokens_ = (char**)calloc(size, sizeof(char*)); // 使用calloc自动初始化为nullptr
    if (!tokens_) {
      OBP_LOG_WARN("Failed to allocate memory for tokens");
      Py_DECREF(pResult);
      PyGILState_Release(gstate);
      return OBP_PLUGIN_ERROR;
    }
    
    for (Py_ssize_t i = 0; i < size; i++) {
      PyObject* pItem = PyList_GetItem(pResult, i);
      if (PyUnicode_Check(pItem)) {
        Py_ssize_t str_len;
        const char* str = PyUnicode_AsUTF8AndSize(pItem, &str_len);
        if (str && str_len > 0) {
          tokens_[i] = (char*)malloc(str_len + 1);
          if (tokens_[i]) {
            memcpy(tokens_[i], str, str_len);
            tokens_[i][str_len] = '\0';
            OBP_LOG_INFO("Token[%d]: '%s' (len=%d)", (int)i, tokens_[i], (int)str_len);
          } else {
            OBP_LOG_WARN("Failed to allocate memory for token %d", (int)i);
          }
        }
      }
    }
    
    Py_DECREF(pResult);
    PyGILState_Release(gstate);
    return OBP_SUCCESS;
    
  } catch (const std::exception& e) {
    OBP_LOG_WARN("Exception in tokenize_text: %s", e.what());
    PyGILState_Release(gstate);
    return OBP_PLUGIN_ERROR;
  } catch (...) {
    OBP_LOG_WARN("Unknown exception in tokenize_text");
    PyGILState_Release(gstate);
    return OBP_PLUGIN_ERROR;
  }
}

int ObThaiFTParser::tokenize_with_spaces()
{
  // 简单的空格分词，作为fallback
  tokens_ = nullptr;
  token_count_ = 0;
  
  const char* current = start_;
  const char* end = end_;
  int count = 0;
  
  // 计算token数量
  while (current < end) {
    while (current < end && (*current == ' ' || *current == '\t' || *current == '\n')) {
      current++;
    }
    
    if (current >= end) break;
    
    const char* word_start = current;
    
    while (current < end && *current != ' ' && *current != '\t' && *current != '\n') {
      current++;
    }
    
    if (current > word_start) {
      count++;
    }
  }
  
  if (count == 0) {
    return OBP_SUCCESS;
  }
  
  // 分配内存
  tokens_ = (char**)calloc(count, sizeof(char*));
  if (!tokens_) {
    return OBP_PLUGIN_ERROR;
  }
  
  // 重新扫描并存储tokens
  current = start_;
  int index = 0;
  
  while (current < end && index < count) {
    while (current < end && (*current == ' ' || *current == '\t' || *current == '\n')) {
      current++;
    }
    
    if (current >= end) break;
    
    const char* word_start = current;
    
    while (current < end && *current != ' ' && *current != '\t' && *current != '\n') {
      current++;
    }
    
    if (current > word_start) {
      int64_t word_len = current - word_start;
      tokens_[index] = (char*)malloc(word_len + 1);
      if (tokens_[index]) {
        memcpy(tokens_[index], word_start, word_len);
        tokens_[index][word_len] = '\0';
        index++;
      }
    }
  }
  
  token_count_ = index;
  return OBP_SUCCESS;
}

int ObThaiFTParser::is_thai_text(const char* text, int64_t len)
{
  if (!text || len <= 0) {
    return 0;
  }
  
  int thai_char_count = 0;
  int total_char_count = 0;
  
  for (int64_t i = 0; i < len; i++) {
    unsigned char c = (unsigned char)text[i];
    
    // 跳过ASCII控制字符和空白字符
    if (c < 32 || c == ' ' || c == '\t' || c == '\n' || c == '\r') {
      continue;
    }
    
    total_char_count++;
    
    // 检查是否为泰语字符范围 (UTF-8编码)
    // 泰语Unicode范围: U+0E00-U+0E7F
    if (i + 2 < len && c == 0xE0 && 
        (unsigned char)text[i+1] >= 0xB8 && (unsigned char)text[i+1] <= 0xBB) {
      thai_char_count++;
      i += 2; // 跳过UTF-8的后续字节
    }
  }
  
  // 如果泰语字符占比超过30%，认为是泰语文本
  if (total_char_count > 0 && (thai_char_count * 100 / total_char_count) > 30) {
    return 1;
  }
  
  return 0;
}

void ObThaiFTParser::cleanup_python_global()
{
  PyGILState_STATE gstate = PyGILState_Ensure();
  
  // 清理实例级别的Python对象
  if (pSplitFunc_) {
    Py_DECREF(pSplitFunc_);
    pSplitFunc_ = nullptr;
  }
  
  if (pTokenizer_) {
    Py_DECREF(pTokenizer_);
    pTokenizer_ = nullptr;
  }
  
  PyGILState_Release(gstate);
  
  pthread_mutex_lock(&g_python_mutex);
  g_ref_count--;
  
  // 只有当没有实例在使用时才清理全局资源
  if (g_ref_count <= 0) {
    PyGILState_STATE gstate2 = PyGILState_Ensure();
    
    if (g_pTokenizerClass) {
      Py_DECREF(g_pTokenizerClass);
      g_pTokenizerClass = nullptr;
    }
    
    if (g_pModule) {
      Py_DECREF(g_pModule);
      g_pModule = nullptr;
    }
    
    g_python_initialized = false;
    g_ref_count = 0;
    
    PyGILState_Release(gstate2);
  }
  
  pthread_mutex_unlock(&g_python_mutex);
}

int ObThaiFTParser::get_next_token(
    const char *&word,
    int64_t &word_len,
    int64_t &char_len,
    int64_t &word_freq)
{
  int ret = OBP_SUCCESS;
  word = nullptr;
  word_len = 0;
  char_len = 0;
  word_freq = 0;
  
  if (!is_inited_) {
    ret = OBP_PLUGIN_ERROR;
    OBP_LOG_WARN("thai ft parser isn't initialized. ret=%d, is_inited=%d", ret, is_inited_);
  } else if (tokens_ && current_token_index_ < token_count_) {
    // 使用Python分词结果
    while (current_token_index_ < token_count_ && !tokens_[current_token_index_]) {
      current_token_index_++; // 跳过nullptr的token
    }
    
    if (current_token_index_ < token_count_ && tokens_[current_token_index_]) {
      word = tokens_[current_token_index_];
      word_len = strlen(tokens_[current_token_index_]);
      char_len = word_len;
      word_freq = 1;
      OBP_LOG_INFO("Returning token[%d]: '%s' (len=%d)", current_token_index_, word, (int)word_len);
      current_token_index_++;
    } else {
      ret = OBP_ITER_END;
    }
  } else if (next_ < end_) {
    // 使用原始字符扫描逻辑（fallback）
    const char *start = start_;
    const char *next = next_;
    const char *end = end_;
    const ObPluginCharsetInfoPtr cs = cs_;
    
    do {
      while (next < end) {
        int ctype;
        int mbl = obp_charset_ctype(cs, &ctype, (unsigned char *)next, (unsigned char *)end);
        if (ctype & (OBP_CHAR_TYPE_UPPER | OBP_CHAR_TYPE_LOWER | OBP_CHAR_TYPE_NUMBER) || *next == '_') {
          break;
        }
        next += mbl > 0 ? mbl : (mbl < 0 ? -mbl : 1);
      }
      if (next >= end) {
        ret = OBP_ITER_END;
      } else {
        int64_t c_nums = 0;
        start = next;
        while (next < end) {
          int ctype;
          int mbl = obp_charset_ctype(cs, &ctype, (unsigned char *)next, (unsigned char *)end);
          if (!(ctype & (OBP_CHAR_TYPE_UPPER | OBP_CHAR_TYPE_LOWER | OBP_CHAR_TYPE_NUMBER) || *next == '_')) {
            break;
          }
          ++c_nums;
          next += mbl > 0 ? mbl : (mbl < 0 ? -mbl : 1);
        }
        if (0 < c_nums) {
          word = start;
          word_len = next - start;
          char_len = c_nums;
          word_freq = 1;
          start = next;
          break;
        } else {
          start = next;
        }
      }
    } while (ret == OBP_SUCCESS && next < end);
    if (OBP_ITER_END == ret || OBP_SUCCESS == ret) {
      start_ = start;
      next_ = next;
      end_ = end;
    }
  } else {
    ret = OBP_ITER_END;
  }
  
  OBP_LOG_TRACE("next word. start=%p, next=%p, end=%p", start_, next_, end_);
  return ret;
}

} // namespace thai
} // namespace oceanbase

using namespace oceanbase::thai;

int ftparser_scan_begin(ObPluginFTParserParamPtr param)
{
  int ret = OBP_SUCCESS;
  ObThaiFTParser *parser = new (std::nothrow) ObThaiFTParser;
  if (!parser) {
    return OBP_PLUGIN_ERROR;
  }
  
  ret = parser->init(param);
  if (OBP_SUCCESS != ret) {
    delete parser;
    return ret;
  }
  obp_ftparser_set_user_data(param, (parser));
  return OBP_SUCCESS;
}

int ftparser_scan_end(ObPluginFTParserParamPtr param)
{
  ObThaiFTParser *parser = (ObThaiFTParser *)(obp_ftparser_user_data(param));
  if (parser) {
    delete parser;
    obp_ftparser_set_user_data(param, 0);
  }
  return OBP_SUCCESS;
}

int ftparser_next_token(ObPluginFTParserParamPtr param,
                        char **word,
                        int64_t *word_len,
                        int64_t *char_cnt,
                        int64_t *word_freq)
{
  int ret = OBP_SUCCESS;
  if (word == nullptr || word_len == nullptr || char_cnt == nullptr || word_freq == nullptr) {
    ret = OBP_INVALID_ARGUMENT;
  } else {
    ObThaiFTParser *parser = (ObThaiFTParser *)(obp_ftparser_user_data(param));
    if (parser) {
      ret = parser->get_next_token((const char *&)(*word), *word_len, *char_cnt, *word_freq);
    } else {
      ret = OBP_PLUGIN_ERROR;
    }
  }
  return ret;
}

int ftparser_get_add_word_flag(uint64_t *flag)
{
  int ret = OBP_SUCCESS;
  if (flag == nullptr) {
    ret = OBP_INVALID_ARGUMENT;
  } else {
    *flag = OBP_FTPARSER_AWF_MIN_MAX_WORD
            | OBP_FTPARSER_AWF_STOPWORD
            | OBP_FTPARSER_AWF_CASEDOWN
            | OBP_FTPARSER_AWF_GROUPBY_WORD;
  }
  return ret;
}

/**
 * plugin init function
 * @param plugin The plugin param
 * @details This function will be called when OceanBase load the library.
 * We register the plugin(s) in this function and we can initialize other
 * variables here.
 */
int plugin_init(ObPluginParamPtr plugin)
{
  int ret = OBP_SUCCESS;
  /// A ftparser plugin descriptor
  ObPluginFTParser parser = {
    .init              = nullptr,
    .deinit            = nullptr,
    .scan_begin        = ftparser_scan_begin,
    .scan_end          = ftparser_scan_end,
    .next_token        = ftparser_next_token,
    .get_add_word_flag = ftparser_get_add_word_flag
  };

  /// register the ftparser plugin
  ret = OBP_REGISTER_FTPARSER(plugin,
                              "thai_ftparser",
                              parser,
                              "This is a Thai language ftparser using thai-tokenizer.");
  return ret;
}

OBP_DECLARE_PLUGIN(thai_ftparser)
{
  OBP_AUTHOR_OCEANBASE,       // 作者
  OBP_MAKE_VERSION(1, 0, 0),  // 当前插件库的版本
  OBP_LICENSE_MULAN_PSL_V2,   // 该插件的license
  plugin_init, // init        // 插件的初始化函数，在plugin_init中注册各个插件功能
  nullptr, // deinit          // 插件的析构函数
} OBP_DECLARE_PLUGIN_END;

/** @} */ 
