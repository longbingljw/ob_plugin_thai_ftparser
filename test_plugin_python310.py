#!/usr/bin/env python3.10
# -*- coding: utf-8 -*-

import sys
import thai_tokenizer

def test_tokenizer_python310():
    print("Testing thai_tokenizer with Python", sys.version)
    
    # 创建分词器（Python 3.10版本不需要参数）
    tokenizer = thai_tokenizer.Tokenizer()
    
    # 测试文本
    test_texts = [
        "อาหารไทยเป็นที่นิยมมากในโลก",
        "ไทย",
        "ตลาดน้ำ", 
        "สงกรานต์",
        "เพลงลูกทุ่ง",
        "สวัสดีค่ะ ยินดีต้อนรับสู่เว็บไซต์ของเรา"
    ]
    
    print("=" * 60)
    for i, text in enumerate(test_texts, 1):
        tokens = tokenizer.split(text)
        print(f"{i}. Text: '{text}'")
        print(f"   Tokens: {tokens}")
        print(f"   Token count: {len(tokens)}")
        print("-" * 50)

if __name__ == "__main__":
    test_tokenizer_python310() 