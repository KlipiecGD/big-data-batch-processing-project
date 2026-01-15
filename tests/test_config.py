from src.config.config import config


def test_config_tables_loading() -> None:
    """
    Test to ensure that the required tables are loaded from the config.
    """
    tables = config.required_tables.get("creation_order", [])
    assert isinstance(tables, list), "Required tables should be a list."
    assert len(tables) > 0, "There should be at least one required table."
    assert "users" in tables, "'users' table should be in the required tables."
    assert "transactions" in tables, (
        "'transactions' table should be in the required tables."
    )
    assert "products" in tables, "'products' table should be in the required tables."
    assert tables.index("users") < tables.index("transactions"), (
        "'users' table should be processed before 'transactions' table."
    )
    assert tables.index("products") < tables.index("transactions"), (
        "'products' table should be processed before 'transactions' table."
    )
