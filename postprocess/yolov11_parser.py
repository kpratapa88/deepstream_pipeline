import numpy as np

def nms(boxes, scores, iou_threshold=0.45):
    """
    Simple NMS implementation using NumPy.
    boxes: (N, 4) in (x1, y1, x2, y2) format
    scores: (N,)
    """
    if len(boxes) == 0:
        return []

    x1 = boxes[:, 0]
    y1 = boxes[:, 1]
    x2 = boxes[:, 2]
    y2 = boxes[:, 3]
    areas = (x2 - x1) * (y2 - y1)
    
    order = scores.argsort()[::-1]
    keep = []
    
    while order.size > 0:
        i = order[0]
        keep.append(i)
        
        xx1 = np.maximum(x1[i], x1[order[1:]])
        yy1 = np.maximum(y1[i], y1[order[1:]])
        xx2 = np.minimum(x2[i], x2[order[1:]])
        yy2 = np.minimum(y2[i], y2[order[1:]])
        
        w = np.maximum(0.0, xx2 - xx1)
        h = np.maximum(0.0, yy2 - yy1)
        inter = w * h
        
        ovr = inter / (areas[i] + areas[order[1:]] - inter)
        inds = np.where(ovr <= iou_threshold)[0]
        order = order[inds + 1]
        
    return keep

def parse_yolov11_output(tensor_data, confidence_threshold=0.4, num_classes=80):
    """
    Parses YOLOv11 output tensor [1, 84, 8400].
    Returns list of (class_id, confidence, bbox[x1, y1, x2, y2])
    """
    # Transpose to (8400, 84)
    data = tensor_data.T
    
    # Check if scores need sigmoid (raw logits vs probabilities)
    # YOLOv8/v11 usually have sigmoid applied in the ONNX, but if they don't, 
    # the values can be large.
    scores = data[:, 4:]
    if np.max(scores) > 1.0 or np.min(scores) < 0.0:
        # Probable logits, apply sigmoid
        scores = 1 / (1 + np.exp(-np.clip(scores, -20, 20)))
    
    max_scores = np.max(scores, axis=1)
    class_ids = np.argmax(scores, axis=1)
    
    # Filter by threshold
    mask = max_scores > confidence_threshold
    filtered_scores = max_scores[mask]
    filtered_class_ids = class_ids[mask]
    filtered_boxes = data[mask, :4]
    
    if len(filtered_scores) == 0:
        return []
    
    # Convert boxes from (cx, cy, w, h) to (x1, y1, x2, y2)
    # YOLOv11 usually outputs boxes scaled to model input size (640)
    x1 = filtered_boxes[:, 0] - filtered_boxes[:, 2] / 2
    y1 = filtered_boxes[:, 1] - filtered_boxes[:, 3] / 2
    x2 = filtered_boxes[:, 0] + filtered_boxes[:, 2] / 2
    y2 = filtered_boxes[:, 1] + filtered_boxes[:, 3] / 2
    
    boxes = np.column_stack((x1, y1, x2, y2))
    
    # Apply NMS
    keep_indices = nms(boxes, filtered_scores)
    
    results = []
    for i in keep_indices:
        results.append({
            "class_id": int(filtered_class_ids[i]),
            "confidence": float(filtered_scores[i]),
            "bbox": [float(x1[i]), float(y1[i]), float(x2[i]), float(y2[i])]
        })
    return results
