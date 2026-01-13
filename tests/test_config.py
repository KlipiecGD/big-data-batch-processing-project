from src.config.config import config

def test_config_tables_loading():
    """
    Test to ensure that the required tables are loaded from the config.
    """
    required_tables = config.database.get("required_tables", [])
    assert isinstance(required_tables, list), "Required tables should be a list."
    assert len(required_tables) > 0, "There should be at least one required table."
    assert "users" in required_tables, "'users' table should be in the required tables."
    assert "transactions" in required_tables, (
        "'transactions' table should be in the required tables."
    )
    assert "products" in required_tables, (
        "'products' table should be in the required tables."
    )

