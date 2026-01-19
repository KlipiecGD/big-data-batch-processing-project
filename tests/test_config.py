from src.config.config import config


def test_config_creation_order_tables_loading() -> None:
    """
    Test to ensure that the required tables are loaded from the config in order of creation.
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

def test_config_drop_order_tables_loading() -> None:
    """
    Test to ensure that the required tables are loaded from the config in order of deletion.
    """
    tables = config.required_tables.get("drop_order", [])
    assert isinstance(tables, list), "Required tables should be a list."
    assert len(tables) > 0, "There should be at least one required table."
    assert "transactions" in tables, "'transactions' table should be in the required tables."
    assert "users" in tables, "'users' table should be in the required tables."
    assert "products" in tables, "'products' table should be in the required tables."
    assert tables.index("transactions") < tables.index("users"), (
        "'transactions' table should be deleted before 'users' table."
    )
    assert tables.index("transactions") < tables.index("products"), (
        "'transactions' table should be deleted before 'products' table."
    )
