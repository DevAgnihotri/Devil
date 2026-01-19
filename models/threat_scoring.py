def calculate_threat_score(threat_confidence, campaign_detected):
    score = threat_confidence * 5

    if campaign_detected:
        score += 3

    if score >= 7:
        level = "HIGH"
    elif score >= 4:
        level = "MEDIUM"
    else:
        level = "LOW"

    return round(score, 1), level
