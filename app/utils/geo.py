import math


def distance_km(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    # Equirectangular approximation for short distances
    rad = math.pi / 180.0
    x = (lon2 - lon1) * rad * math.cos(0.5 * (lat1 + lat2) * rad)
    y = (lat2 - lat1) * rad
    return 6371.0 * math.sqrt(x * x + y * y)
