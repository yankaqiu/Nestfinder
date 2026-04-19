from app.enrichment.global_score import compute_global_score, explain_score


def test_compute_global_score_returns_weighted_breakdown() -> None:
    scores = compute_global_score(
        {
            "price_vs_city_median": 0.82,
            "features": ["balcony", "elevator", "parking"],
            "text_features_json": {"modern": True, "bright": True},
            "is_urban": 1,
            "lake_distance_m": 1200,
            "distance_public_transport": 180,
            "year_built": 2018,
            "renovation_year": None,
            "floor_level": 4,
            "price": 2600,
            "area": 82,
            "rooms": 3.5,
            "description": "Bright, modern flat",
            "latitude": 47.37,
            "available_from": "2026-05-01",
            "images_json": {"images": [{"url": "https://example.com/1.jpg"}, {"url": "https://example.com/2.jpg"}]},
        }
    )

    assert 0.0 <= scores["global_score"] <= 1.0
    assert scores["score_value"] > 0
    assert scores["score_location"] > 0
    assert scores["score_completeness"] > 0


def test_explain_score_includes_human_readable_dimensions() -> None:
    scores = {
        "global_score": 0.71,
        "score_value": 0.82,
        "score_amenity": 0.6,
        "score_location": 0.78,
        "score_building": 0.75,
        "score_completeness": 0.7,
        "score_freshness": 0.8,
    }
    text = explain_score(
        scores,
        {
            "price_vs_city_median": 0.82,
            "is_urban": 1,
            "lake_distance_m": 800,
            "distance_public_transport": 200,
            "renovation_year": 2021,
            "floor_level": 4,
        },
    )

    assert "value" in text.lower() or "price" in text.lower()
    assert "location" in text.lower() or "urban" in text.lower()
    assert "building" in text.lower() or "renovated" in text.lower()
