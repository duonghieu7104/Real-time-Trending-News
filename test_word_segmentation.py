#!/usr/bin/env python3
"""
Test script để kiểm tra word segmentation và embedding với câu tiếng Việt
"""

import os
import sys
import numpy as np
import onnxruntime as ort
from transformers import AutoTokenizer
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test câu tiếng Việt
test_sentence = "Hôm nay thời tiết rất đẹp, tôi đi dạo trong công viên và gặp nhiều người bạn."

def test_word_segmentation():
    """Test Vietnamese word segmentation"""
    try:
        from pyvi.ViTokenizer import tokenize as vi_tokenize
        logger.info("✅ PyVi available for word segmentation")
        
        # Test segmentation
        segmented = vi_tokenize(test_sentence)
        logger.info(f"📝 Original: {test_sentence}")
        logger.info(f"🔤 Segmented: {segmented}")
        
        return segmented
    except ImportError:
        logger.warning("⚠️ PyVi not available, skipping word segmentation")
        return test_sentence
    except Exception as e:
        logger.error(f"❌ Word segmentation error: {e}")
        return test_sentence

def test_onnx_embedding():
    """Test ONNX embedding generation"""
    try:
        # Check if model files exist
        model_path = "/opt/spark/work-dir/model/embedding"
        if not os.path.exists(model_path):
            logger.error(f"❌ Model path not found: {model_path}")
            return None
        
        # List model files
        files = os.listdir(model_path)
        logger.info(f"📁 Model files: {files}")
        
        # Load ONNX model
        onnx_path = os.path.join(model_path, "model.onnx")
        if not os.path.exists(onnx_path):
            logger.error(f"❌ ONNX model not found: {onnx_path}")
            return None
        
        logger.info("🔄 Loading ONNX model...")
        session = ort.InferenceSession(onnx_path, providers=['CPUExecutionProvider'])
        logger.info("✅ ONNX model loaded successfully")
        
        # Load tokenizer from original model
        logger.info("🔄 Loading tokenizer...")
        try:
            tokenizer = AutoTokenizer.from_pretrained("vinai/phobert-base")
            logger.info("✅ Tokenizer loaded successfully from vinai/phobert-base")
        except Exception as e:
            logger.warning(f"⚠️ Failed to load from vinai/phobert-base: {e}")
            # Try loading from local path
            tokenizer = AutoTokenizer.from_pretrained(model_path)
            logger.info("✅ Tokenizer loaded successfully from local path")
        
        # Test with Vietnamese sentence
        logger.info(f"📝 Testing with: {test_sentence}")
        
        # Tokenize
        inputs = tokenizer(
            test_sentence,
            padding=True,
            truncation=True,
            max_length=512,
            return_tensors="np"
        )
        
        logger.info(f"🔤 Tokenized input shape: {inputs['input_ids'].shape}")
        
        # Run inference
        input_ids = inputs["input_ids"].astype(np.int64)
        attention_mask = inputs["attention_mask"].astype(np.int64)
        
        logger.info("🔄 Running ONNX inference...")
        outputs = session.run(
            None,
            {
                "input_ids": input_ids,
                "attention_mask": attention_mask
            }
        )
        
        # Extract embeddings
        embeddings = outputs[0]  # Shape: (batch_size, seq_len, hidden_size)
        logger.info(f"📊 Embeddings shape: {embeddings.shape}")
        
        # Pool embeddings (mean pooling)
        attention_mask_expanded = attention_mask[:, :, None]
        pooled_embeddings = np.sum(embeddings * attention_mask_expanded, axis=1) / np.sum(attention_mask_expanded, axis=1)
        
        logger.info(f"📊 Pooled embeddings shape: {pooled_embeddings.shape}")
        logger.info(f"📊 Embedding dimension: {pooled_embeddings.shape[1]}")
        logger.info(f"📊 First 5 values: {pooled_embeddings[0][:5]}")
        
        return pooled_embeddings
        
    except Exception as e:
        logger.error(f"❌ ONNX embedding error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main test function"""
    logger.info("🚀 Starting Vietnamese word segmentation and embedding test")
    
    # Test word segmentation
    logger.info("\n" + "="*50)
    logger.info("1. Testing Vietnamese Word Segmentation")
    logger.info("="*50)
    segmented_text = test_word_segmentation()
    
    # Test ONNX embedding
    logger.info("\n" + "="*50)
    logger.info("2. Testing ONNX Embedding Generation")
    logger.info("="*50)
    embeddings = test_onnx_embedding()
    
    if embeddings is not None:
        logger.info("✅ Test completed successfully!")
        logger.info(f"📊 Final embedding shape: {embeddings.shape}")
    else:
        logger.error("❌ Test failed!")

if __name__ == "__main__":
    main()
