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
 * @brief This is a Thai language fulltext parser plugin using PyThaiNLP
 * @{
 */

// 前向声明C++类（仅在C++部分使用）
#ifdef __cplusplus
namespace oceanbase {
namespace thai {

class PythonCaller {
public:
    PythonCaller();
    ~PythonCaller();
    
    PythonCaller(const PythonCaller&) = delete;
    PythonCaller& operator=(const PythonCaller&) = delete;
    
    int initialize();
    int tokenize(const char* text, int text_len, char*** tokens, int* token_count);
    void cleanup();

private:
    int parse_python_result(void* pResult, char*** tokens, int* token_count);
    int handle_python_error();
    
    void* pModule_;
    void* pTokenizeFunc_;
    int initialized_;
    pthread_mutex_t mutex_;
};

class ObThaiFTParser {
public:
    ObThaiFTParser();
    ~ObThaiFTParser();

    int init(ObPluginFTParserParamPtr param);
    void reset();
    int get_next_token(
        const char **word,
        int64_t *word_len,
        int64_t *char_len,
        int64_t *word_freq);

private:
    int tokenize_text();
    int tokenize_with_spaces();
    int is_thai_text(const char* text, int64_t len);
    void cleanup_python();
    
    ObPluginDatum cs_;
    const char* start_;
    const char* next_;
    const char* end_;
    int is_inited_;
    
    PythonCaller* python_caller_;
    char** tokens_;
    int token_count_;
    int current_token_index_;
};

} // namespace thai
} // namespace oceanbase
#endif

// C接口实现
extern "C" {

// 泰语分词器句柄类型
typedef struct _ObThaiFTParserHandle {
    void* parser;  // ObThaiFTParser*
} ObThaiFTParserHandle;

// C接口函数
int ftparser_scan_begin(ObPluginFTParserParamPtr param)
{
    int ret = OBP_SUCCESS;
    ObThaiFTParserHandle* handle = (ObThaiFTParserHandle*)malloc(sizeof(ObThaiFTParserHandle));
    if (!handle) {
        return OBP_PLUGIN_ERROR;
    }
    
    handle->parser = new (std::nothrow) oceanbase::thai::ObThaiFTParser();
    if (!handle->parser) {
        free(handle);
        return OBP_PLUGIN_ERROR;
    }
    
    ret = ((oceanbase::thai::ObThaiFTParser*)handle->parser)->init(param);
    if (OBP_SUCCESS != ret) {
        delete (oceanbase::thai::ObThaiFTParser*)handle->parser;
        free(handle);
        return ret;
    }
    
    obp_ftparser_set_user_data(param, handle);
    return OBP_SUCCESS;
}

int ftparser_scan_end(ObPluginFTParserParamPtr param)
{
    ObThaiFTParserHandle* handle = (ObThaiFTParserHandle*)(obp_ftparser_user_data(param));
    if (handle) {
        if (handle->parser) {
            delete (oceanbase::thai::ObThaiFTParser*)handle->parser;
        }
        free(handle);
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
    if (word == NULL || word_len == NULL || char_cnt == NULL || word_freq == NULL) {
        ret = OBP_INVALID_ARGUMENT;
    } else {
        ObThaiFTParserHandle* handle = (ObThaiFTParserHandle*)(obp_ftparser_user_data(param));
        if (handle && handle->parser) {
            ret = ((oceanbase::thai::ObThaiFTParser*)handle->parser)->get_next_token(
                (const char**)word, word_len, char_cnt, word_freq);
        } else {
            ret = OBP_PLUGIN_ERROR;
        }
    }
    return ret;
}

int ftparser_get_add_word_flag(uint64_t *flag)
{
    int ret = OBP_SUCCESS;
    if (flag == NULL) {
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
        .init              = NULL,
        .deinit            = NULL,
        .scan_begin        = ftparser_scan_begin,
        .scan_end          = ftparser_scan_end,
        .next_token        = ftparser_next_token,
        .get_add_word_flag = ftparser_get_add_word_flag
    };

    /// register the ftparser plugin
    ret = OBP_REGISTER_FTPARSER(plugin,
                                "thai_ftparser",
                                parser,
                                "This is a Thai language ftparser using PyThaiNLP.");
    return ret;
}

} // extern "C"

// C++实现部分
#ifdef __cplusplus

namespace oceanbase {
namespace thai {

// PythonCaller实现
PythonCaller::PythonCaller() : pModule_(NULL), pTokenizeFunc_(NULL), initialized_(0) {
    // 初始化互斥锁
    pthread_mutex_init(&mutex_, NULL);
}

PythonCaller::~PythonCaller() {
    cleanup();
    pthread_mutex_destroy(&mutex_);
}

int PythonCaller::initialize() {
    pthread_mutex_lock(&mutex_);
    
    if (initialized_) {
        pthread_mutex_unlock(&mutex_);
        return OBP_SUCCESS;
    }
    
    try {
        // 初始化Python解释器
        if (!Py_IsInitialized()) {
            Py_Initialize();
        }
        
        // 导入PyThaiNLP模块
        PyObject* pModule = PyImport_ImportModule("pythainlp");
        if (!pModule) {
            OBP_LOG_WARN("Failed to import pythainlp module");
            pthread_mutex_unlock(&mutex_);
            return OBP_PLUGIN_ERROR;
        }
        
        // 获取tokenize函数
        PyObject* pTokenizeFunc = PyObject_GetAttrString(pModule, "word_tokenize");
        if (!pTokenizeFunc) {
            OBP_LOG_WARN("Failed to get word_tokenize function");
            Py_DECREF(pModule);
            pthread_mutex_unlock(&mutex_);
            return OBP_PLUGIN_ERROR;
        }
        
        pModule_ = pModule;
        pTokenizeFunc_ = pTokenizeFunc;
        initialized_ = 1;
        
        pthread_mutex_unlock(&mutex_);
        return OBP_SUCCESS;
    } catch (const std::exception& e) {
        OBP_LOG_WARN("Python initialization failed: %s", e.what());
        pthread_mutex_unlock(&mutex_);
        return OBP_PLUGIN_ERROR;
    }
}

int PythonCaller::tokenize(const char* text, int text_len, char*** tokens, int* token_count) {
    if (!initialized_) {
        OBP_LOG_WARN("Python caller not initialized");
        return OBP_PLUGIN_ERROR;
    }
    
    pthread_mutex_lock(&mutex_);
    
    try {
        PyGILState_STATE gstate = PyGILState_Ensure();
        
        // 创建Python字符串
        PyObject* pText = PyUnicode_FromStringAndSize(text, text_len);
        if (!pText) {
            PyGILState_Release(gstate);
            pthread_mutex_unlock(&mutex_);
            return OBP_PLUGIN_ERROR;
        }
        
        // 调用tokenize函数
        PyObject* pArgs = PyTuple_Pack(1, pText);
        PyObject* pResult = PyObject_CallObject((PyObject*)pTokenizeFunc_, pArgs);
        
        Py_DECREF(pText);
        Py_DECREF(pArgs);
        
        if (!pResult) {
            PyGILState_Release(gstate);
            pthread_mutex_unlock(&mutex_);
            return handle_python_error();
        }
        
        // 解析结果
        int ret = parse_python_result(pResult, tokens, token_count);
        Py_DECREF(pResult);
        
        PyGILState_Release(gstate);
        pthread_mutex_unlock(&mutex_);
        return ret;
        
    } catch (const std::exception& e) {
        OBP_LOG_WARN("Tokenization failed: %s", e.what());
        pthread_mutex_unlock(&mutex_);
        return OBP_PLUGIN_ERROR;
    }
}

int PythonCaller::parse_python_result(void* pResult, char*** tokens, int* token_count) {
    if (!PyList_Check((PyObject*)pResult)) {
        OBP_LOG_WARN("Expected list result from Python");
        return OBP_PLUGIN_ERROR;
    }
    
    Py_ssize_t size = PyList_Size((PyObject*)pResult);
    *token_count = (int)size;
    
    // 分配内存
    *tokens = (char**)malloc(size * sizeof(char*));
    if (!*tokens) {
        return OBP_PLUGIN_ERROR;
    }
    
    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject* pItem = PyList_GetItem((PyObject*)pResult, i);
        if (PyUnicode_Check(pItem)) {
            const char* str = PyUnicode_AsUTF8(pItem);
            if (str) {
                int len = strlen(str);
                (*tokens)[i] = (char*)malloc(len + 1);
                if ((*tokens)[i]) {
                    strcpy((*tokens)[i], str);
                }
            } else {
                (*tokens)[i] = NULL;
            }
        } else {
            (*tokens)[i] = NULL;
        }
    }
    
    return OBP_SUCCESS;
}

int PythonCaller::handle_python_error() {
    if (PyErr_Occurred()) {
        PyObject *type, *value, *traceback;
        PyErr_Fetch(&type, &value, &traceback);
        
        if (value) {
            PyObject* str = PyObject_Str(value);
            if (str) {
                const char* error_msg = PyUnicode_AsUTF8(str);
                if (error_msg) {
                    OBP_LOG_WARN("Python error: %s", error_msg);
                }
                Py_DECREF(str);
            }
            Py_DECREF(value);
        }
        
        if (type) Py_DECREF(type);
        if (traceback) Py_DECREF(traceback);
        
        PyErr_Clear();
    }
    
    return OBP_PLUGIN_ERROR;
}

void PythonCaller::cleanup() {
    pthread_mutex_lock(&mutex_);
    
    if (pTokenizeFunc_) {
        Py_DECREF((PyObject*)pTokenizeFunc_);
        pTokenizeFunc_ = NULL;
    }
    
    if (pModule_) {
        Py_DECREF((PyObject*)pModule_);
        pModule_ = NULL;
    }
    
    initialized_ = 0;
    pthread_mutex_unlock(&mutex_);
}

// ObThaiFTParser实现
ObThaiFTParser::ObThaiFTParser() : cs_(0), start_(NULL), next_(NULL), end_(NULL), 
    is_inited_(0), python_caller_(NULL), tokens_(NULL), token_count_(0), current_token_index_(0) {
    python_caller_ = new PythonCaller();
}

ObThaiFTParser::~ObThaiFTParser() {
    reset();
    if (python_caller_) {
        delete python_caller_;
        python_caller_ = NULL;
    }
}

void ObThaiFTParser::reset() {
    cs_ = 0;
    start_ = NULL;
    next_ = NULL;
    end_ = NULL;
    is_inited_ = 0;
    current_token_index_ = 0;
    
    // 清理tokens
    if (tokens_) {
        for (int i = 0; i < token_count_; i++) {
            if (tokens_[i]) {
                free(tokens_[i]);
            }
        }
        free(tokens_);
        tokens_ = NULL;
    }
    token_count_ = 0;
    
    cleanup_python();
}

int ObThaiFTParser::init(ObPluginFTParserParamPtr param) {
    int ret = OBP_SUCCESS;
    const char *fulltext = obp_ftparser_fulltext(param);
    int64_t ft_length = obp_ftparser_fulltext_length(param);
    ObPluginCharsetInfoPtr cs = obp_ftparser_charset_info(param);

    if (is_inited_) {
        ret = OBP_INIT_TWICE;
        OBP_LOG_WARN("init twice. ret=%d, param=%p, this=%p", ret, param, this);
    } else if (0 == param
        || 0 == cs
        || NULL == fulltext
        || 0 >= ft_length) {
        ret = OBP_INVALID_ARGUMENT;
        OBP_LOG_WARN("invalid arguments, ret=%d, param=%p", ret, param);
    } else {
        cs_ = cs;
        start_ = fulltext;
        next_ = start_;
        end_ = start_ + ft_length;
        is_inited_ = 1;
        current_token_index_ = 0;
        
        // 检查是否为泰语文本
        if (is_thai_text(fulltext, ft_length)) {
            ret = python_caller_->initialize();
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
    
    return ret;
}

int ObThaiFTParser::tokenize_text() {
    if (!python_caller_ || !is_inited_) {
        return OBP_PLUGIN_ERROR;
    }
    
    return python_caller_->tokenize(start_, (int)(end_ - start_), &tokens_, &token_count_);
}

int ObThaiFTParser::tokenize_with_spaces() {
    // 简单的空格分词，作为fallback
    tokens_ = NULL;
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

int ObThaiFTParser::is_thai_text(const char* text, int64_t len) {
    for (int64_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)text[i];
        
        // 检查UTF-8泰语字符范围
        if (c >= 0xE0 && c <= 0xEF) {
            // 这是UTF-8多字节字符的第一个字节
            // 泰语字符在UTF-8中的范围是 0xE0 0xB8 0x80 到 0xE0 0xBB 0xBF
            if (i + 2 < len) {
                unsigned char c2 = (unsigned char)text[i + 1];
                unsigned char c3 = (unsigned char)text[i + 2];
                
                // 检查是否是泰语字符范围
                if (c == 0xE0 && c2 >= 0xB8 && c2 <= 0xBB) {
                    if ((c2 == 0xB8 && c3 >= 0x80) || 
                        (c2 == 0xB9 && c3 <= 0xFF) ||
                        (c2 == 0xBA && c3 <= 0xFF) ||
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

void ObThaiFTParser::cleanup_python() {
    if (python_caller_) {
        python_caller_->cleanup();
    }
}

int ObThaiFTParser::get_next_token(
    const char **word,
    int64_t *word_len,
    int64_t *char_len,
    int64_t *word_freq)
{
    int ret = OBP_SUCCESS;
    
    if (!is_inited_) {
        ret = OBP_NOT_INIT;
        OBP_LOG_WARN("not inited. ret=%d, this=%p", ret, this);
    } else if (tokens_ && current_token_index_ < token_count_) {
        // 使用Python分词结果
        if (tokens_[current_token_index_]) {
            *word = tokens_[current_token_index_];
            *word_len = strlen(tokens_[current_token_index_]);
            *char_len = *word_len;
            *word_freq = 1;
            current_token_index_++;
        } else {
            ret = OBP_ITER_END;
        }
    } else if (next_ < end_) {
        // 使用原始字符扫描逻辑（fallback）
        const char *start = next_;
        const char *end = end_;
        
        // 跳过空白字符
        while (start < end && (*start == ' ' || *start == '\t' || *start == '\n')) {
            start++;
        }
        
        if (start >= end) {
            ret = OBP_ITER_END;
        } else {
            // 找到单词开始
            const char *word_start = start;
            
            // 找到单词结束
            while (start < end && *start != ' ' && *start != '\t' && *start != '\n') {
                start++;
            }
            
            if (start > word_start) {
                *word = word_start;
                *word_len = start - word_start;
                *char_len = *word_len;
                *word_freq = 1;
                next_ = start;
            } else {
                ret = OBP_ITER_END;
            }
        }
    } else {
        ret = OBP_ITER_END;
    }
    
    return ret;
}

} // namespace thai
} // namespace oceanbase

#endif // __cplusplus

OBP_DECLARE_PLUGIN(thai_ftparser)
{
    OBP_AUTHOR_OCEANBASE,       // 作者
    OBP_MAKE_VERSION(1, 0, 0),  // 当前插件库的版本
    OBP_LICENSE_MULAN_PSL_V2,   // 该插件的license
    plugin_init, // init        // 插件的初始化函数，在plugin_init中注册各个插件功能
    NULL, // deinit          // 插件的析构函数
} OBP_DECLARE_PLUGIN_END;

/** @} */ 

