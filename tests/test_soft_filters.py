from app.participant.ranking import rank_listings
from app.participant.soft_fact_extraction import extract_soft_facts
from app.participant.soft_filtering import filter_soft_facts


def test_extract_soft_facts_returns_stub_structure() -> None:
    result = extract_soft_facts("bright flat near transport")

    assert isinstance(result, dict)


def test_filter_soft_facts_returns_candidate_subset() -> None:
    candidates = [{"listing_id": "1"}, {"listing_id": "2"}]

    filtered = filter_soft_facts(candidates, {"raw_query": "quiet"})

    assert isinstance(filtered, list)
    assert {item["listing_id"] for item in filtered} <= {"1", "2"}


def test_rank_listings_returns_ranked_shape() -> None:
    ranked = rank_listings(
        candidates=[
            {
                "listing_id": "abc",
                "title": "Example",
                "city": "Zurich",
                "price": 2500,
                "rooms": 3.0,
                "latitude": 47.37,
                "longitude": 8.54,
                "street": "Main 1",
                "postal_code": "8000",
                "canton": "ZH",
                "area": 75.0,
                "available_from": "2026-06-01",
                "image_urls": ["https://example.com/1.jpg"],
                "hero_image_url": "https://example.com/1.jpg",
                "original_url": "https://example.com/listing",
                "features": ["balcony", "elevator"],
                "offer_type": "RENT",
                "object_category": "Wohnung",
                "object_type": "Apartment",
            }
        ],
        soft_facts={"raw_query": "bright"},
    )

    assert len(ranked) == 1
    assert ranked[0].listing_id == "abc"
    assert isinstance(ranked[0].score, float)
    assert isinstance(ranked[0].reason, str)
    assert isinstance(ranked[0].explanation, str)
    assert ranked[0].global_scores is not None
    assert ranked[0].listing.id == "abc"
    assert ranked[0].listing.title == "Example"
    assert ranked[0].listing.city == "Zurich"
    assert ranked[0].listing.image_urls


def test_rank_listings_uses_image_rag_when_available(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.participant.ranking.search_image_rag",
        lambda **kwargs: {
            "results": [
                {
                    "listing_id": "second",
                    "score": 0.9,
                    "best_image_url": "https://example.com/2.jpg",
                },
                {
                    "listing_id": "first",
                    "score": 0.2,
                    "best_image_url": "https://example.com/1.jpg",
                },
            ]
        },
    )

    ranked = rank_listings(
        candidates=[
            {
                "listing_id": "first",
                "title": "Bright apartment",
                "description": "Sunny home",
                "features": [],
            },
            {
                "listing_id": "second",
                "title": "Apartment with balcony",
                "description": "Outdoor space",
                "features": ["balcony"],
            },
        ],
        soft_facts={
            "raw_query": "bright apartment with balcony",
            "signals": {"bright": 1.0, "balcony": 0.8},
        },
    )

    assert [item.listing_id for item in ranked] == ["second", "first"]
    assert ranked[0].reason.startswith("soft match + image bonus")
    assert ranked[0].listing.hero_image_url == "https://example.com/2.jpg"


def test_rank_listings_limits_image_bonus_for_non_visual_queries(monkeypatch) -> None:
    monkeypatch.setattr(
        "app.participant.ranking.search_image_rag",
        lambda **kwargs: {
            "results": [
                {
                    "listing_id": "image-heavy",
                    "score": 0.95,
                    "best_image_url": "https://example.com/1.jpg",
                },
                {
                    "listing_id": "commute-fit",
                    "score": 0.2,
                    "best_image_url": "https://example.com/2.jpg",
                },
            ]
        },
    )

    ranked = rank_listings(
        candidates=[
            {
                "listing_id": "image-heavy",
                "title": "Stylish loft",
                "description": "Far from transport.",
                "distance_public_transport": 1600,
                "features": [],
            },
            {
                "listing_id": "commute-fit",
                "title": "Apartment next to the station",
                "description": "Quick commute.",
                "distance_public_transport": 120,
                "features": [],
            },
        ],
        soft_facts={
            "raw_query": "apartment with short commute",
            "signals": {"public_transport": 1.0, "short_commute": 1.2},
            "max_commute_minutes": 20,
        },
    )

    assert [item.listing_id for item in ranked] == ["commute-fit", "image-heavy"]
    assert ranked[0].score > ranked[1].score


def test_rank_listings_applies_subtle_user_preference_bonus(monkeypatch) -> None:
    monkeypatch.setattr("app.participant.ranking.search_image_rag", lambda **kwargs: None)

    ranked = rank_listings(
        candidates=[
            {
                "listing_id": "preferred",
                "title": "Apartment with balcony",
                "city": "Zurich",
                "price": 2600,
                "features": ["balcony"],
            },
            {
                "listing_id": "neutral",
                "title": "Apartment",
                "city": "Bern",
                "price": 2600,
                "features": [],
            },
        ],
        soft_facts={
            "raw_query": "apartment",
            "signals": {},
        },
        user_profile={
            "preferred_cities": ["Zurich"],
            "preferred_features": ["balcony"],
            "price_range": {"min": 2400, "max": 2800},
            "clicked_listing_ids": [],
            "favorite_listing_ids": [],
            "dismissed_listing_ids": [],
        },
    )

    assert [item.listing_id for item in ranked] == ["preferred", "neutral"]
    assert ranked[0].score > ranked[1].score
    assert "user preference" in ranked[0].reason


def test_rank_listings_uses_global_score_as_quality_boost(monkeypatch) -> None:
    monkeypatch.setattr("app.participant.ranking.search_image_rag", lambda **kwargs: None)

    ranked = rank_listings(
        candidates=[
            {
                "listing_id": "quality",
                "title": "Renovated apartment near the lake",
                "city": "Zurich",
                "price": 2500,
                "price_vs_city_median": 0.8,
                "features": ["balcony", "elevator", "parking"],
                "is_urban": 1,
                "lake_distance_m": 800,
                "distance_public_transport": 150,
                "renovation_year": 2022,
                "floor_level": 4,
                "description": "Bright renovated apartment.",
                "latitude": 47.37,
                "area": 82,
                "rooms": 3.5,
                "available_from": "2026-05-01",
                "images_json": {"images": [{"url": "https://example.com/1.jpg"}, {"url": "https://example.com/2.jpg"}]},
            },
            {
                "listing_id": "plain",
                "title": "Apartment",
                "city": "Zurich",
                "price": 2500,
                "description": "Basic apartment.",
                "features": [],
            },
        ],
        soft_facts={"raw_query": "apartment", "signals": {}},
    )

    assert [item.listing_id for item in ranked] == ["quality", "plain"]
    assert ranked[0].global_scores is not None
    assert ranked[0].score > ranked[1].score
    assert ranked[0].reason in {"overall quality boost", "hard filters only"}
    assert ranked[0].explanation
