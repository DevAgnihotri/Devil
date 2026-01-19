def infer_location(language):
    """
    VERY SAFE heuristic-based location inference
    """
    mapping = {
        "en": ("Global", 20.5937, 78.9629),
        "hi": ("India", 20.5937, 78.9629),
        "ur": ("Pakistan Region", 30.3753, 69.3451),
        "bn": ("India/Bangladesh", 23.6850, 90.3563),
        "ta": ("South India", 10.8505, 76.2711)
    }
    return mapping.get(language, ("Unknown", 0, 0))
