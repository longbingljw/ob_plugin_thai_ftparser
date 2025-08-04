/*
 * Copyright (c) 2025 OceanBase.
 * Emergency Fix Version
 * SIGABRT FIX
 */
#include <new>
#include <Python.h>
#include <pthread.h>
#include <signal.h>
#include <setjmp.h>

#include "oceanbase/ob_plugin_ftparser.h"

/**
 * @defgroup ThaiFtParser Thai Fulltext Parser Plugin - Emergency Fix
 * @brief Emergency fix for SIGABRT crashes in multi-threaded environment
 * @{
 */

namespace oceanbase {
namespace thai {

// 全局静态变量 - 增强版
static pthread_mutex_t g_python_mutex;
static pthread_once_t g_mutex_once = PTHREAD_ONCE_INIT;
static bool g_python_initialized = false;
static PyObject* g_pModule = nullptr;
static PyObject* g_pTokenizerClass = nullptr;
static int g_ref_count = 0;
static bool g_emergency_shutdown = false;

// 初始化互斥锁
static void init_mutex() {
    pthread_mutex_init(&g_python_mutex, nullptr);
}

// 信号处理器
static void signal_handler(int sig) {
    g_emergency_shutdown = true;
    OBP_LOG_WARN("Emergency shutdown triggered by signal %d", sig);
}

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
  int initialize_python_safe();
  int tokenize_text_safe();
  int tokenize_with_spaces();
  int is_thai_text(const char* text, int64_t len);
  void cleanup_python_safe();
  bool check_python_health();
  
  ObPluginDatum  cs_   = 0;
  const char *   start_     = nullptr;
  const char *   next_      = nullptr;
  const char *   end_       = nullptr;
  bool           is_inited_ = false;
  
  // Python相关
  PyObject* pTokenizer_ = nullptr;
  PyObject* pSplitFunc_ = nullptr;
  bool instance_has_python_ = false;
  
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
  
  cleanup_python_safe();
}

bool ObThaiFTParser::check_python_health() {
  // 检查 Python 解释器健康状态
  if (!Py_IsInitialized()) {
    OBP_LOG_WARN("Python interpreter not initialized");
    return false;
  }
  
  // Python 3.12 中 PyGILState_Check() 返回 int，不是 PyGILState_STATE
  // 简化健康检查，避免使用已弃用的 API
  try {
    // 尝试获取 GIL 状态来验证 Python 环境
    PyGILState_STATE gstate = PyGILState_Ensure();
    PyGILState_Release(gstate);
    return true;
  } catch (...) {
    OBP_LOG_WARN("Python GIL state check failed");
    return false;
  }
}

int ObThaiFTParser::init(ObPluginFTParserParamPtr param)
{
  int ret = OBP_SUCCESS;
  const char *fulltext = obp_ftparser_fulltext(param);
  int64_t ft_length = obp_ftparser_fulltext_length(param);
  ObPluginCharsetInfoPtr cs = obp_ftparser_charset_info(param);

  // 安装信号处理器
  signal(SIGABRT, signal_handler);
  signal(SIGSEGV, signal_handler);

  if (g_emergency_shutdown) {
    OBP_LOG_WARN("Emergency shutdown mode, using fallback tokenizer");
    ret = tokenize_with_spaces();
    return ret;
  }

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
      OBP_LOG_INFO("Detected Thai text, attempting safe Python initialization");
      ret = initialize_python_safe();
      if (ret == OBP_SUCCESS) {
        OBP_LOG_INFO("Python initialized successfully, attempting safe tokenization");
        ret = tokenize_text_safe();
        if (ret != OBP_SUCCESS) {
          OBP_LOG_WARN("Safe tokenization failed, falling back to space tokenization");
          ret = tokenize_with_spaces();
        }
      } else {
        OBP_LOG_WARN("Safe Python initialization failed, using space tokenization");
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

int ObThaiFTParser::initialize_python_safe()
{
  // 确保互斥锁初始化
  pthread_once(&g_mutex_once, init_mutex);
  
  if (pthread_mutex_lock(&g_python_mutex) != 0) {
    OBP_LOG_WARN("Failed to acquire Python mutex");
    return OBP_PLUGIN_ERROR;
  }
  
  try {
    // 检查紧急关闭状态
    if (g_emergency_shutdown) {
      pthread_mutex_unlock(&g_python_mutex);
      return OBP_PLUGIN_ERROR;
    }
    
    // 初始化全局Python环境（只做一次）
    if (!g_python_initialized) {
      if (!Py_IsInitialized()) {
        // Python 3.12 中不再需要 Py_SetProgramName 和 PyEval_InitThreads
        // 这些在 Py_Initialize() 中自动处理
        
        Py_Initialize();
        if (!Py_IsInitialized()) {
          OBP_LOG_WARN("Failed to initialize Python interpreter");
          pthread_mutex_unlock(&g_python_mutex);
          return OBP_PLUGIN_ERROR;
        }
        
        // Python 3.12 中线程支持默认启用，不需要显式初始化
        // 释放 GIL，让其他线程可以获取
        PyEval_SaveThread();
      }
      
      // 获取 GIL 进行模块导入
      PyGILState_STATE gstate = PyGILState_Ensure();
      
      // 设置 Python 路径
      PyRun_SimpleString("import sys");
      PyRun_SimpleString("sys.path.append('/usr/local/lib/python3.8/site-packages')");
      PyRun_SimpleString("sys.path.append('/home/longbing.ljw/.local/lib/python3.8/site-packages')");
      
      // 导入thai_tokenizer模块
      g_pModule = PyImport_ImportModule("thai_tokenizer");
      if (!g_pModule) {
        OBP_LOG_WARN("Failed to import thai_tokenizer module");
        PyErr_Clear(); // 清除 Python 错误状态
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&g_python_mutex);
        return OBP_PLUGIN_ERROR;
      }
      
      // 获取Tokenizer类
      g_pTokenizerClass = PyObject_GetAttrString(g_pModule, "Tokenizer");
      if (!g_pTokenizerClass) {
        OBP_LOG_WARN("Failed to get Tokenizer class");
        PyErr_Clear();
        Py_DECREF(g_pModule);
        g_pModule = nullptr;
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&g_python_mutex);
        return OBP_PLUGIN_ERROR;
      }
      
      g_python_initialized = true;
      PyGILState_Release(gstate);
    }
    
    // 为当前实例创建Tokenizer实例
    PyGILState_STATE gstate = PyGILState_Ensure();
    
    // 检查 Python 健康状态
    if (!check_python_health()) {
      PyGILState_Release(gstate);
      pthread_mutex_unlock(&g_python_mutex);
      return OBP_PLUGIN_ERROR;
    }
    
    pTokenizer_ = PyObject_CallObject(g_pTokenizerClass, nullptr);
    if (!pTokenizer_) {
      OBP_LOG_WARN("Failed to create Tokenizer instance");
      PyErr_Clear();
      PyGILState_Release(gstate);
      pthread_mutex_unlock(&g_python_mutex);
      return OBP_PLUGIN_ERROR;
    }
    
    // 获取split方法
    pSplitFunc_ = PyObject_GetAttrString(pTokenizer_, "split");
    if (!pSplitFunc_) {
      OBP_LOG_WARN("Failed to get split method");
      PyErr_Clear();
      Py_DECREF(pTokenizer_);
      pTokenizer_ = nullptr;
      PyGILState_Release(gstate);
      pthread_mutex_unlock(&g_python_mutex);
      return OBP_PLUGIN_ERROR;
    }
    
    g_ref_count++;
    instance_has_python_ = true;
    PyGILState_Release(gstate);
    pthread_mutex_unlock(&g_python_mutex);
    return OBP_SUCCESS;
    
  } catch (const std::exception& e) {
    OBP_LOG_WARN("Exception in Python initialization: %s", e.what());
    pthread_mutex_unlock(&g_python_mutex);
    return OBP_PLUGIN_ERROR;
  } catch (...) {
    OBP_LOG_WARN("Unknown exception in Python initialization");
    pthread_mutex_unlock(&g_python_mutex);
    return OBP_PLUGIN_ERROR;
  }
}

int ObThaiFTParser::tokenize_text_safe()
{
  if (!g_python_initialized || !is_inited_ || !pTokenizer_ || !pSplitFunc_) {
    return OBP_PLUGIN_ERROR;
  }
  
  if (g_emergency_shutdown) {
    return OBP_PLUGIN_ERROR;
  }
  
  PyGILState_STATE gstate = PyGILState_Ensure();
  
  try {
    // 再次检查 Python 健康状态
    if (!check_python_health()) {
      PyGILState_Release(gstate);
      return OBP_PLUGIN_ERROR;
    }
    
    // 创建Python字符串 - 限制长度以避免内存问题
    int64_t text_len = end_ - start_;
    if (text_len > 10000) { // 限制最大长度
      text_len = 10000;
      OBP_LOG_WARN("Text too long, truncating to 10000 characters");
    }
    
    PyObject* pText = PyUnicode_FromStringAndSize(start_, (Py_ssize_t)text_len);
    if (!pText) {
      OBP_LOG_WARN("Failed to create Python string");
      PyErr_Clear();
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
      PyErr_Clear();
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
    if (size > 1000) { // 限制 token 数量
      size = 1000;
      OBP_LOG_WARN("Too many tokens, limiting to 1000");
    }
    
    token_count_ = (int)size;
    
    // 分配内存
    tokens_ = (char**)calloc(size, sizeof(char*));
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
        if (str && str_len > 0 && str_len < 1000) { // 限制单个 token 长度
          tokens_[i] = (char*)malloc(str_len + 1);
          if (tokens_[i]) {
            memcpy(tokens_[i], str, str_len);
            tokens_[i][str_len] = '\0';
          }
        }
      }
    }
    
    Py_DECREF(pResult);
    PyGILState_Release(gstate);
    return OBP_SUCCESS;
    
  } catch (const std::exception& e) {
    OBP_LOG_WARN("Exception in tokenize_text_safe: %s", e.what());
    PyGILState_Release(gstate);
    return OBP_PLUGIN_ERROR;
  } catch (...) {
    OBP_LOG_WARN("Unknown exception in tokenize_text_safe");
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

void ObThaiFTParser::cleanup_python_safe()
{
  if (!instance_has_python_) {
    return;
  }
  
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
  
  if (pthread_mutex_lock(&g_python_mutex) == 0) {
    g_ref_count--;
    instance_has_python_ = false;
    
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
  
  if (g_emergency_shutdown) {
    return OBP_ITER_END;
  }
  
  if (!is_inited_) {
    ret = OBP_PLUGIN_ERROR;
    OBP_LOG_WARN("thai ft parser isn't initialized. ret=%d, is_inited=%d", ret, is_inited_);
  } else if (tokens_ && current_token_index_ < token_count_) {
    // 使用分词结果
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
                              "Emergency fix version for Thai language ftparser.");
  return ret;
}

OBP_DECLARE_PLUGIN(thai_ftparser)
{
  OBP_AUTHOR_OCEANBASE,       
  OBP_MAKE_VERSION(1, 0, 1),  // 版本号升级
  OBP_LICENSE_MULAN_PSL_V2,   
  plugin_init,
  nullptr,
} OBP_DECLARE_PLUGIN_END;

/** @} */