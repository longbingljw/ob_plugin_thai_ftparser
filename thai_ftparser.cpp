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
  int initialize_python();
  int tokenize_text();
  int tokenize_with_spaces();
  int is_thai_text(const char* text, int64_t len);
  void cleanup_python();
  
  ObPluginDatum  cs_   = 0;
  const char *   start_     = nullptr;
  const char *   next_      = nullptr;
  const char *   end_       = nullptr;
  bool           is_inited_ = false;
  
  // Python相关
  void* pModule_ = nullptr;
  void* pTokenizer_ = nullptr;
  void* pSplitFunc_ = nullptr;
  bool python_initialized_ = false;
  pthread_mutex_t python_mutex_;
  
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
  
  cleanup_python();
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
    
    // 初始化Python互斥锁
    pthread_mutex_init(&python_mutex_, nullptr);
    
    // 检查是否为泰语文本
    if (is_thai_text(fulltext, ft_length)) {
      ret = initialize_python();
      if (ret == OBP_SUCCESS) {
        ret = tokenize_text();
      }
    } else {
      ret = tokenize_with_spaces();
    }
  }
  if (ret != OBP_SUCCESS && !is_inited_) {
    reset();
  }
  OBP_LOG_INFO("thai ftparser init done. ret=%d", ret);
  return ret;
}

int ObThaiFTParser::initialize_python()
{
  pthread_mutex_lock(&python_mutex_);
  
  if (python_initialized_) {
    pthread_mutex_unlock(&python_mutex_);
    return OBP_SUCCESS;
  }
  
  try {
    // 初始化Python解释器
    if (!Py_IsInitialized()) {
      Py_Initialize();
    }
    
    // 导入thai_tokenizer模块
    PyObject* pModule = PyImport_ImportModule("thai_tokenizer");
    if (!pModule) {
      OBP_LOG_WARN("Failed to import thai_tokenizer module");
      pthread_mutex_unlock(&python_mutex_);
      return OBP_PLUGIN_ERROR;
    }
    
    // 获取Tokenizer类
    PyObject* pTokenizerClass = PyObject_GetAttrString(pModule, "Tokenizer");
    if (!pTokenizerClass) {
      OBP_LOG_WARN("Failed to get Tokenizer class");
      Py_DECREF(pModule);
      pthread_mutex_unlock(&python_mutex_);
      return OBP_PLUGIN_ERROR;
    }
    
    // 创建Tokenizer实例
    PyObject* pTokenizer = PyObject_CallObject(pTokenizerClass, nullptr);
    if (!pTokenizer) {
      OBP_LOG_WARN("Failed to create Tokenizer instance");
      Py_DECREF(pTokenizerClass);
      Py_DECREF(pModule);
      pthread_mutex_unlock(&python_mutex_);
      return OBP_PLUGIN_ERROR;
    }
    
    // 获取split方法
    PyObject* pSplitFunc = PyObject_GetAttrString(pTokenizer, "split");
    if (!pSplitFunc) {
      OBP_LOG_WARN("Failed to get split method");
      Py_DECREF(pTokenizer);
      Py_DECREF(pTokenizerClass);
      Py_DECREF(pModule);
      pthread_mutex_unlock(&python_mutex_);
      return OBP_PLUGIN_ERROR;
    }
    
    pModule_ = pModule;
    pTokenizer_ = pTokenizer;
    pSplitFunc_ = pSplitFunc;
    python_initialized_ = true;
    
    pthread_mutex_unlock(&python_mutex_);
    return OBP_SUCCESS;
  } catch (const std::exception& e) {
    OBP_LOG_WARN("Python initialization failed: %s", e.what());
    pthread_mutex_unlock(&python_mutex_);
    return OBP_PLUGIN_ERROR;
  }
}

int ObThaiFTParser::tokenize_text()
{
  if (!python_initialized_ || !is_inited_) {
    return OBP_PLUGIN_ERROR;
  }
  
  pthread_mutex_lock(&python_mutex_);
  
  PyGILState_STATE gstate = PyGILState_Ensure();
  
  // 创建Python字符串
  PyObject* pText = PyUnicode_FromStringAndSize(start_, (Py_ssize_t)(end_ - start_));
  if (!pText) {
    PyGILState_Release(gstate);
    pthread_mutex_unlock(&python_mutex_);
    return OBP_PLUGIN_ERROR;
  }
  
  // 调用split函数
  PyObject* pArgs = PyTuple_Pack(1, pText);
  PyObject* pResult = PyObject_CallObject((PyObject*)pSplitFunc_, pArgs);
  
  Py_DECREF(pText);
  Py_DECREF(pArgs);
  
  if (!pResult) {
    PyGILState_Release(gstate);
    pthread_mutex_unlock(&python_mutex_);
    return OBP_PLUGIN_ERROR;
  }
  
  // 解析结果
  if (!PyList_Check(pResult)) {
    Py_DECREF(pResult);
    PyGILState_Release(gstate);
    pthread_mutex_unlock(&python_mutex_);
    return OBP_PLUGIN_ERROR;
  }
  
  Py_ssize_t size = PyList_Size(pResult);
  token_count_ = (int)size;
  
  // 分配内存
  tokens_ = (char**)malloc(size * sizeof(char*));
  if (!tokens_) {
    Py_DECREF(pResult);
    PyGILState_Release(gstate);
    pthread_mutex_unlock(&python_mutex_);
    return OBP_PLUGIN_ERROR;
  }
  
  // 初始化tokens数组
  for (Py_ssize_t i = 0; i < size; i++) {
    tokens_[i] = nullptr;
  }
  
  for (Py_ssize_t i = 0; i < size; i++) {
    PyObject* pItem = PyList_GetItem(pResult, i);
    if (PyUnicode_Check(pItem)) {
      const char* str = PyUnicode_AsUTF8(pItem);
      if (str) {
        int len = strlen(str);
        tokens_[i] = (char*)malloc(len + 1);
        if (tokens_[i]) {
          strcpy(tokens_[i], str);
        }
      }
    }
  }
  
  Py_DECREF(pResult);
  PyGILState_Release(gstate);
  pthread_mutex_unlock(&python_mutex_);
  return OBP_SUCCESS;
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
  tokens_ = (char**)malloc(count * sizeof(char*));
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
      int len = current - word_start;
      tokens_[index] = (char*)malloc(len + 1);
      if (tokens_[index]) {
        memcpy(tokens_[index], word_start, len);
        tokens_[index][len] = '\0';
      }
      index++;
    }
  }
  
  token_count_ = count;
  return OBP_SUCCESS;
}

int ObThaiFTParser::is_thai_text(const char* text, int64_t len)
{
  // 更准确的泰语字符检测
  for (int64_t i = 0; i < len; i++) {
    unsigned char c = (unsigned char)text[i];
    // 检查UTF-8泰语字符范围
    if (c >= 0xE0 && c <= 0xEF) {
      // 多字节UTF-8字符
      if (i + 2 < len) {
        unsigned char c2 = (unsigned char)text[i + 1];
        unsigned char c3 = (unsigned char)text[i + 2];
        // 泰语字符的UTF-8范围：0xE0 0xB8 0x80 - 0xE0 0xBB 0xBF
        if (c == 0xE0 && c2 >= 0xB8 && c2 <= 0xBB) {
          if ((c2 == 0xB8 && c3 >= 0x80) || 
              (c2 == 0xB9 && c3 >= 0x80) || 
              (c2 == 0xBA && c3 >= 0x80) || 
              (c2 == 0xBB && c3 <= 0xBF)) {
            return 1;
          }
        }
        i += 2; // 跳过后续字节
      }
    }
  }
  return 0;
}

void ObThaiFTParser::cleanup_python()
{
  pthread_mutex_lock(&python_mutex_);
  
  if (pSplitFunc_) {
    Py_DECREF((PyObject*)pSplitFunc_);
    pSplitFunc_ = nullptr;
  }
  
  if (pTokenizer_) {
    Py_DECREF((PyObject*)pTokenizer_);
    pTokenizer_ = nullptr;
  }
  
  if (pModule_) {
    Py_DECREF((PyObject*)pModule_);
    pModule_ = nullptr;
  }
  
  python_initialized_ = false;
  pthread_mutex_unlock(&python_mutex_);
  
  pthread_mutex_destroy(&python_mutex_);
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
  delete parser;
  obp_ftparser_set_user_data(param, 0);
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
    ret = parser->get_next_token((const char *&)(*word), *word_len, *char_cnt, *word_freq);
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

