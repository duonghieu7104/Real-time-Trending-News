#!/usr/bin/env python3
"""
Test script đơn giản để kiểm tra embedding với câu tiếng Việt
"""

import os
import sys
import numpy as np
import onnxruntime as ort
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

def test_onnx_model():
    """Test ONNX model loading"""
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
        
        # Get model input/output info
        input_details = session.get_inputs()
        output_details = session.get_outputs()
        
        logger.info(f"📊 Model inputs: {[inp.name for inp in input_details]}")
        logger.info(f"📊 Model outputs: {[out.name for out in output_details]}")
        
        # Test with dummy input
        logger.info("🔄 Testing with dummy input...")
        
        # Create dummy input (batch_size=1, seq_len=10)
        dummy_input_ids = np.array([[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]], dtype=np.int64)
        dummy_attention_mask = np.array([[1, 1, 1, 1, 1, 1, 1, 1, 1, 1]], dtype=np.int64)
        
        logger.info(f"📊 Input shape: {dummy_input_ids.shape}")
        
        # Run inference
        outputs = session.run(
            None,
            {
                "input_ids": dummy_input_ids,
                "attention_mask": dummy_attention_mask
            }
        )
        
        # Extract embeddings
        embeddings = outputs[0]  # Shape: (batch_size, seq_len, hidden_size)
        logger.info(f"📊 Embeddings shape: {embeddings.shape}")
        
        # Pool embeddings (mean pooling)
        attention_mask_expanded = dummy_attention_mask[:, :, None]
        pooled_embeddings = np.sum(embeddings * attention_mask_expanded, axis=1) / np.sum(attention_mask_expanded, axis=1)
        
        logger.info(f"📊 Pooled embeddings shape: {pooled_embeddings.shape}")
        logger.info(f"📊 Embedding dimension: {pooled_embeddings.shape[1]}")
        logger.info(f"📊 First 5 values: {pooled_embeddings[0][:5]}")
        
        return pooled_embeddings
        
    except Exception as e:
        logger.error(f"❌ ONNX model error: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main test function"""
    logger.info("🚀 Starting Vietnamese word segmentation and ONNX model test")
    
    # Test word segmentation
    logger.info("\n" + "="*50)
    logger.info("1. Testing Vietnamese Word Segmentation")
    logger.info("="*50)
    segmented_text = test_word_segmentation()
    
    # Test ONNX model
    logger.info("\n" + "="*50)
    logger.info("2. Testing ONNX Model")
    logger.info("="*50)
    embeddings = test_onnx_model()
    
    if embeddings is not None:
        logger.info("✅ Test completed successfully!")
        logger.info(f"📊 Final embedding shape: {embeddings.shape}")
    else:
        logger.error("❌ Test failed!")

if __name__ == "__main__":
    main()
