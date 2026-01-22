import pytest
from typing import Generator
from pyspark.sql import SparkSession
from tests.data.test_data_gold import (
    USERS_DATA,
    PRODUCTS_DATA,
    TRANSACTIONS_DATA,
    USERS_SCHEMA,
    PRODUCTS_SCHEMA,
    TRANSACTIONS_SCHEMA,
)


@pytest.fixture(scope="session")
def spark_session() -> Generator[SparkSession, None, None]:
    """
    Fixture to create a Spark session for testing.
    Returns:
        SparkSession: A Spark session object.
    """
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("TransformationTests")
        .getOrCreate()
    )
    yield spark
    spark.stop()


@pytest.fixture
def setup_mock_views(spark_session: SparkSession) -> None:
    """
    Fixture to set up mock views for testing transformations.
    """
    # Mock users data
    users_data = USERS_DATA
    users_schema = USERS_SCHEMA

    spark_session.createDataFrame(users_data, users_schema).createOrReplaceTempView(
        "users"
    )

    # Mock products data
    products_data = PRODUCTS_DATA
    products_schema = PRODUCTS_SCHEMA

    spark_session.createDataFrame(
        products_data, products_schema
    ).createOrReplaceTempView("products")

    # Mock transactions data
    transactions_data = TRANSACTIONS_DATA
    transactions_schema = TRANSACTIONS_SCHEMA

    spark_session.createDataFrame(
        transactions_data, transactions_schema
    ).createOrReplaceTempView("transactions")


def test_top_spenders(spark_session: SparkSession, setup_mock_views) -> None:
    """
    Test to verify the top spenders transformation.
    Expected top 3:
    - John: 30 + 150 + 80 + 75 = 335.0
    - Maria: 60 + 50 + 100 = 210.0
    - Sarah: 70 + 100 = 170.0
    """
    with open("sql_queries/top_spenders.sql", "r") as f:
        query = f.read()
    result_df = spark_session.sql(query)
    results = result_df.collect()

    # Verify we have results
    assert len(results) > 0, "No results returned from top_spenders query"

    # Verify the top spender is John
    assert results[0]["name"] == "John", (
        f"Expected top spender to be John, got {results[0]['name']}"
    )
    assert results[0]["total_spent"] == 335.0, (
        f"Expected John's total to be 335.0, got {results[0]['total_spent']}"
    )

    # Verify second place is Maria
    assert results[1]["name"] == "Maria", (
        f"Expected second spender to be Maria, got {results[1]['name']}"
    )
    assert results[1]["total_spent"] == 210.0, (
        f"Expected Maria's total to be 210.0, got {results[1]['total_spent']}"
    )

    # Verify third place is Sarah
    assert results[2]["name"] == "Sarah", (
        f"Expected third spender to be Sarah, got {results[2]['name']}"
    )
    assert results[2]["total_spent"] == 170.0, (
        f"Expected Sarah's total to be 170.0, got {results[2]['total_spent']}"
    )

    # Verify results are ordered by total_spent descending
    for i in range(len(results) - 1):
        assert results[i]["total_spent"] >= results[i + 1]["total_spent"], (
            f"Results not properly ordered: {results[i]['total_spent']} < {results[i + 1]['total_spent']}"
        )


def test_top_products_by_category(spark_session: SparkSession, setup_mock_views) -> None:
    """
    Test to verify the top products by category transformation.
    Expected:
    - Category 1: Product A (20 units), Product C (7 units), Product B (10 units), Product I (4 units)
    - Category 2: Product E (5 units), Product D (5 units), Product F (2 units), Product J (0 units)
    - Category 3: Product G (3 units), Product H (1 unit)
    """
    with open("sql_queries/top_products_by_category.sql", "r") as f:
        query = f.read()
    result_df = spark_session.sql(query)
    results = result_df.collect()

    # Verify we have results
    assert len(results) > 0, "No results returned from top_products_by_category query"

    # Group results by category
    results_by_category = {}
    for row in results:
        category = row["category"]
        if category not in results_by_category:
            results_by_category[category] = []
        results_by_category[category].append(row)

    # Verify Category 1 top product
    category_1_products = results_by_category.get("Category 1", [])
    assert len(category_1_products) > 0, "No products found for Category 1"
    assert category_1_products[0]["name"] == "Product A", (
        f"Expected top product in Category 1 to be Product A, got {category_1_products[0]['name']}"
    )
    assert category_1_products[0]["total_quantity_sold"] == 20, (
        f"Expected Product A to have 20 units sold, got {category_1_products[0]['total_quantity_sold']}"
    )

    # Verify Category 2 has products
    category_2_products = results_by_category.get("Category 2", [])
    assert len(category_2_products) > 0, "No products found for Category 2"

    # Verify Category 3 has products
    category_3_products = results_by_category.get("Category 3", [])
    assert len(category_3_products) > 0, "No products found for Category 3"

    # Verify each category has at most 5 products
    for category, products in results_by_category.items():
        assert len(products) <= 5, (
            f"Category {category} has more than 5 products: {len(products)}"
        )


def test_sales_moving_average(spark_session: SparkSession, setup_mock_views) -> None:
    """
    Test to verify the sales moving average transformation.
    Verifies that moving averages are calculated correctly over 7-day windows.
    The MA divides the sum by the actual number of days in the window.
    """
    with open("sql_queries/sales_moving_average.sql", "r") as f:
        query = f.read()
    result_df = spark_session.sql(query)
    results = result_df.collect()

    # Verify we have results
    assert len(results) > 0, "No results returned from sales_moving_average query"

    # Verify we have 8 days of data (2025-01-01 to 2025-01-08)
    assert len(results) == 8, f"Expected 8 days of data, got {len(results)}"

    # Verify first day (2025-01-01) - only 1 day in window
    first_day = results[0]
    assert first_day["total_sales"] == 80.0, (
        f"Expected first day sales to be 80.0, got {first_day['total_sales']}"
    )
    # Moving average for first day: 80.0 / 1 = 80.0
    assert abs(first_day["moving_average_sales"] - 80.0) < 0.01, (
        f"Expected first day MA to be 80.0, got {first_day['moving_average_sales']}"
    )

    # Verify second day (2025-01-02) - 2 days in window
    second_day = results[1]
    assert second_day["total_sales"] == 100.0, (
        f"Expected second day sales to be 100.0, got {second_day['total_sales']}"
    )
    # Moving average: (80 + 100) / 2 = 90.0
    expected_second_ma = (80.0 + 100.0) / 2
    assert abs(second_day["moving_average_sales"] - expected_second_ma) < 0.01, (
        f"Expected second day MA to be {expected_second_ma}, got {second_day['moving_average_sales']}"
    )

    # Verify seventh day (2025-01-07) - full 7-day window
    # Days 1-7: 80, 100, 268, 125, 190, 85, 175 = 1023 total
    seventh_day = results[6]
    expected_seventh_ma = (80.0 + 100.0 + 268.0 + 125.0 + 190.0 + 85.0 + 175.0) / 7
    assert abs(seventh_day["moving_average_sales"] - expected_seventh_ma) < 0.01, (
        f"Expected seventh day MA to be {expected_seventh_ma}, got {seventh_day['moving_average_sales']}"
    )

    # Verify eighth day (2025-01-08) - full 7-day window (days 2-8)
    # Days 2-8: 100, 268, 125, 190, 85, 175, 130 = 1073 total
    eighth_day = results[7]
    expected_eighth_ma = (100.0 + 268.0 + 125.0 + 190.0 + 85.0 + 175.0 + 130.0) / 7
    assert abs(eighth_day["moving_average_sales"] - expected_eighth_ma) < 0.01, (
        f"Expected eighth day MA to be {expected_eighth_ma}, got {eighth_day['moving_average_sales']}"
    )

    # Verify that moving_average_sales is never null
    for row in results:
        assert row["moving_average_sales"] is not None, (
            f"Moving average should not be null for date {row['transaction_date']}"
        )

    # Verify results are ordered by date
    for i in range(len(results) - 1):
        assert results[i]["transaction_date"] < results[i + 1]["transaction_date"], (
            "Results not properly ordered by date"
        )


def test_performance_analysis_by_country(spark_session: SparkSession, setup_mock_views) -> None:
    """
    Test to verify the performance analysis by country transformation.
    Expected:
    - USA: John (335.0) + Sarah (170.0) = 505.0, 6 transactions, 2 users
    - Poland: Kacper (113.0) + Maria (210.0) + Emma (50.0) = 373.0, 7 transactions, 3 users
    - Germany: Anna (110.0) + Peter (90.0) = 200.0, 5 transactions, 2 users
    - France: Lucas (75.0), 2 transactions, 1 user
    """
    with open("sql_queries/performance_analysis_by_country.sql", "r") as f:
        query = f.read()
    result_df = spark_session.sql(query)
    results = result_df.collect()

    # Verify we have results
    assert len(results) > 0, (
        "No results returned from performance_analysis_by_country query"
    )

    # Verify we have all 4 countries
    countries = [row["country"] for row in results]
    expected_countries = {"USA", "Poland", "Germany", "France"}
    assert set(countries) == expected_countries, (
        f"Expected countries {expected_countries}, got {set(countries)}"
    )

    # Verify USA is the top country by sales
    assert results[0]["country"] == "USA", (
        f"Expected USA to be top country, got {results[0]['country']}"
    )
    assert results[0]["total_sales"] == 505.0, (
        f"Expected USA total sales to be 505.0, got {results[0]['total_sales']}"
    )
    assert results[0]["total_transactions"] == 6, (
        f"Expected USA to have 6 transactions, got {results[0]['total_transactions']}"
    )
    assert results[0]["total_users"] == 2, (
        f"Expected USA to have 2 users, got {results[0]['total_users']}"
    )

    # Verify Poland is second
    assert results[1]["country"] == "Poland", (
        f"Expected Poland to be second, got {results[1]['country']}"
    )
    assert results[1]["total_sales"] == 373.0, (
        f"Expected Poland total sales to be 373.0, got {results[1]['total_sales']}"
    )
    assert results[1]["total_transactions"] == 7, (
        f"Expected Poland to have 7 transactions, got {results[1]['total_transactions']}"
    )
    assert results[1]["total_users"] == 3, (
        f"Expected Poland to have 3 users, got {results[1]['total_users']}"
    )

    # Verify Germany is third
    assert results[2]["country"] == "Germany", (
        f"Expected Germany to be third, got {results[2]['country']}"
    )
    assert results[2]["total_sales"] == 200.0, (
        f"Expected Germany total sales to be 200.0, got {results[2]['total_sales']}"
    )
    assert results[2]["total_transactions"] == 5, (
        f"Expected Germany to have 5 transactions, got {results[2]['total_transactions']}"
    )
    assert results[2]["total_users"] == 2, (
        f"Expected Germany to have 2 users, got {results[2]['total_users']}"
    )

    # Verify France is last
    assert results[3]["country"] == "France", (
        f"Expected France to be last, got {results[3]['country']}"
    )
    assert results[3]["total_sales"] == 75.0, (
        f"Expected France total sales to be 75.0, got {results[3]['total_sales']}"
    )
    assert results[3]["total_transactions"] == 2, (
        f"Expected France to have 2 transactions, got {results[3]['total_transactions']}"
    )
    assert results[3]["total_users"] == 1, (
        f"Expected France to have 1 user, got {results[3]['total_users']}"
    )

    # Verify results are ordered by total_sales descending
    for i in range(len(results) - 1):
        assert results[i]["total_sales"] >= results[i + 1]["total_sales"], (
            f"Results not properly ordered by sales: {results[i]['total_sales']} < {results[i + 1]['total_sales']}"
        )


def test_day_to_day_sales(spark_session: SparkSession, setup_mock_views) -> None:
    """
    Test to verify the day-to-day sales transformation.
    Verifies daily sales, previous day sales, and percent change calculations.
    """
    with open("sql_queries/day_to_day_sales.sql", "r") as f:
        query = f.read()
    result_df = spark_session.sql(query)
    results = result_df.collect()

    # Verify we have results
    assert len(results) > 0, "No results returned from day_to_day_sales query"

    # Verify we have 8 days of data
    assert len(results) == 8, f"Expected 8 days of data, got {len(results)}"

    # Verify first day has no previous day sales and no percent change
    first_day = results[0]
    assert first_day["previous_day_sales"] is None, (
        "First day should have no previous day sales"
    )
    assert first_day["percent_change"] is None, (
        "First day should have no percent change"
    )

    # Verify second day has previous day sales
    second_day = results[1]
    assert second_day["previous_day_sales"] == first_day["total_sales"], (
        "Second day previous_day_sales should equal first day total_sales"
    )

    # Verify percent change calculation for second day
    if second_day["previous_day_sales"] > 0:
        expected_pct = (
            (second_day["total_sales"] - second_day["previous_day_sales"])
            / second_day["previous_day_sales"]
        ) * 100
        assert abs(second_day["percent_change"] - expected_pct) < 0.01, (
            f"Expected percent change {expected_pct}, got {second_day['percent_change']}"
        )

    # Verify results are ordered by date
    for i in range(len(results) - 1):
        assert results[i]["transaction_date"] < results[i + 1]["transaction_date"], (
            "Results not properly ordered by date"
        )

    # Verify all sales values are non-negative
    for row in results:
        assert row["total_sales"] >= 0, (
            f"Sales should be non-negative, got {row['total_sales']} for {row['transaction_date']}"
        )

    # Verify specific day calculations (2025-01-03)
    day_3 = results[2]
    assert day_3["total_sales"] == 268.0, (
        f"Expected day 3 sales to be 268.0, got {day_3['total_sales']}"
    )
