import numpy as np

def parse_fall_detection(tensor_data):
    """
    Parses classification output for fall detection.
    Assume 0 = Fall, 1 = No-Fall.
    Returns (fall_detected, confidence)
    """
    # Softmax on logits
    exp_data = np.exp(tensor_data - np.max(tensor_data))
    probs = exp_data / np.sum(exp_data)
    
    fall_idx = 0
    fall_detected = np.argmax(probs) == fall_idx
    confidence = float(probs[fall_idx])
    
    return fall_detected, confidence

def parse_crowd_density(tensor_data):
    """
    Parses density map output for crowd counting.
    Returns (count, density_score, zone)
    """
    # Density map sum gives the count
    count = float(np.sum(tensor_data))
    
    # Normalize count to a 0-1 density score
    # (assuming 50 persons as max for a high-density zone)
    density_score = min(count / 50.0, 1.0)
    
    if density_score < 0.3:
        zone = "low"
    elif density_score < 0.7:
        zone = "medium"
    else:
        zone = "high"
        
    return count, density_score, zone
