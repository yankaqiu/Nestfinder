import sqlite3
from pathlib import Path


def _create_listings_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    try:
        connection.execute(
            """
            CREATE TABLE listings (
                listing_id TEXT PRIMARY KEY,
                city TEXT,
                price INTEGER,
                feature_balcony INTEGER DEFAULT 0,
                feature_elevator INTEGER DEFAULT 0,
                feature_parking INTEGER DEFAULT 0,
                feature_garage INTEGER DEFAULT 0,
                feature_fireplace INTEGER DEFAULT 0,
                feature_child_friendly INTEGER DEFAULT 0,
                feature_pets_allowed INTEGER DEFAULT 0,
                feature_new_build INTEGER DEFAULT 0,
                feature_wheelchair_accessible INTEGER DEFAULT 0,
                feature_minergie_certified INTEGER DEFAULT 0,
                feature_private_laundry INTEGER DEFAULT 0
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO listings (
                listing_id,
                city,
                price,
                feature_balcony,
                feature_elevator,
                feature_parking
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                ("fav-1", "Zurich", 2500, 1, 1, 0),
                ("fav-2", "Zurich", 2700, 1, 0, 1),
                ("dismissed", "Basel", 1900, 0, 0, 0),
            ],
        )
        connection.commit()
    finally:
        connection.close()


def test_build_user_profile_derives_cities_features_and_price_range(
    tmp_path: Path,
    monkeypatch,
) -> None:
    preferences_db_path = tmp_path / "preferences.db"
    listings_db_path = tmp_path / "listings.db"
    monkeypatch.setenv("PREFERENCES_DB_PATH", str(preferences_db_path))
    monkeypatch.setenv("LISTINGS_DB_PATH", str(listings_db_path))
    _create_listings_db(listings_db_path)
    from app import preferences

    preferences.ensure_schema()
    preferences.record_event(listing_id="fav-1", action="click", query="bright flat", session_id="sess-1")
    preferences.record_event(listing_id="fav-2", action="favorite", query="balcony in zurich", session_id="sess-1")
    preferences.record_event(listing_id="dismissed", action="dismiss", query="cheap flat", session_id="sess-1")
    preferences.log_search(query="bright flat", session_id="sess-1", result_count=2)

    profile = preferences.build_user_profile(session_id="sess-1")

    assert profile["session_id"] == "sess-1"
    assert profile["clicked_listing_ids"] == ["fav-1"]
    assert profile["favorite_listing_ids"] == ["fav-2"]
    assert "dismissed" in profile["dismissed_listing_ids"]
    assert profile["preferred_cities"] == ["Zurich"]
    assert "balcony" in profile["preferred_features"]
    assert profile["price_range"] == {"min": 2500, "max": 2700}
    assert profile["recent_searches"]
