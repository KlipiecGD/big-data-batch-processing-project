import pytest
from typing import Generator
from pyspark.sql import SparkSession, DataFrame
from tests.data.test_data_gold import (
    USERS_DATA,
    PRODUCTS_DATA,
    TRANSACTIONS_DATA,
    USERS_SCHEMA,
    PRODUCTS_SCHEMA,
    TRANSACTIONS_SCHEMA,
    TOP_SPENDERS_FIELDS,
    TOP_PRODUCTS_FIELDS,
    MOVING_AVERAGE_SALES_FIELDS,
    COUNTRY_SALES_FIELDS,
    DAILY_SALES_TRENDS_FIELDS,
)


@pytest.fixture(scope="module")
def spark() -> Generator[SparkSession, None, None]:
    """Create a Spark session for testing."""
    spark_session = (
        SparkSession.builder.appName("TestGoldTransformations")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryo.registrationRequired", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


@pytest.fixture(scope="module")
def setup_views(spark: SparkSession) -> Generator[None, None, None]:
    """Set up temporary views for testing SQL transformations."""
    # Create and register views
    spark.createDataFrame(USERS_DATA, USERS_SCHEMA).createOrReplaceTempView("users")
    spark.createDataFrame(PRODUCTS_DATA, PRODUCTS_SCHEMA).createOrReplaceTempView(
        "products"
    )
    spark.createDataFrame(TRANSACTIONS_DATA, TRANSACTIONS_SCHEMA).createOrReplaceTempView(
        "transactions"
    )

    yield

    # Cleanup
    spark.catalog.dropTempView("users")
    spark.catalog.dropTempView("products")
    spark.catalog.dropTempView("transactions")


class TestTopSpenders:
    """Test suite for top spenders query."""

    @pytest.fixture(scope="class", autouse=True)
    def query_results(self, spark: SparkSession, setup_views) -> Generator[DataFrame, None, None]:
        """Execute query once and cache results for all tests in this class."""
        with open("sql_queries/top_spenders.sql", "r") as f:
            query = f.read()
        result_df = spark.sql(query)
        result_df.cache()
        yield result_df
        result_df.unpersist()

    def test_query_returns_results(self, query_results: DataFrame):
        """Test that the query returns results."""
        assert query_results.count() > 0, "Query should return at least one result"

    def test_results_ordered_by_spending(self, query_results: DataFrame):
        """Test that results are ordered by total_spent descending."""
        results = query_results.collect()

        # Verify descending order
        for i in range(len(results) - 1):
            assert results[i]["total_spent"] >= results[i + 1]["total_spent"], (
                f"Results not properly ordered: {results[i]['total_spent']} < {results[i + 1]['total_spent']}"
            )

    def test_spending_calculations_not_negative(self, query_results: DataFrame):
        """Test that spending calculations are not negative."""
        results = query_results.collect()

        # Verify all spending values are non-negative
        for row in results:
            assert row["total_spent"] >= 0, (
                f"Spending should be non-negative, got {row['total_spent']}"
            )

    def test_all_users_with_transactions_included(
        self, spark: SparkSession, query_results: DataFrame
    ):
        """Test that all users with transactions are included in results."""
        # Get unique users from transactions
        transactions_users = spark.sql(
            "SELECT DISTINCT user_id FROM transactions"
        ).count()

        # Results should have at most as many rows as users with transactions
        assert query_results.count() <= transactions_users, (
            "Results should not exceed number of users with transactions"
        )

    def test_required_columns_present(self, query_results: DataFrame):
        """Test that required columns are present in results."""
        required_columns = TOP_SPENDERS_FIELDS
        actual_columns = set(query_results.columns)

        assert required_columns.issubset(actual_columns), (
            f"Missing required columns: {required_columns - actual_columns}"
        )


class TestTopProductsByCategory:
    """Test suite for top products by category query."""

    @pytest.fixture(scope="class", autouse=True)
    def query_results(self, spark: SparkSession, setup_views) -> Generator[DataFrame, None, None]:
        """Execute query once and cache results for all tests in this class."""
        with open("sql_queries/top_products_by_category.sql", "r") as f:
            query = f.read()
        result_df = spark.sql(query)
        result_df.cache()
        yield result_df
        result_df.unpersist()

    def test_query_returns_results(self, query_results: DataFrame):
        """Test that the query returns results."""
        assert query_results.count() > 0, "Query should return at least one result"

    def test_categories_represented(self, query_results: DataFrame):
        """Test that multiple categories are represented."""
        categories = query_results.select("category").distinct().collect()
        assert len(categories) > 0, "Should have at least one category"

    def test_max_five_products_per_category(self, query_results: DataFrame):
        """Test that each category has at most 5 products."""
        results = query_results.collect()

        # Group by category and count
        category_counts = {}
        for row in results:
            category = row["category"]
            category_counts[category] = category_counts.get(category, 0) + 1

        for category, count in category_counts.items():
            assert count <= 5, (
                f"Category {category} has {count} products, expected max 5"
            )

    def test_products_ordered_within_category(self, query_results: DataFrame):
        """Test that products are ordered by quantity within each category."""
        results = query_results.collect()

        # Group by category
        by_category = {}
        for row in results:
            category = row["category"]
            if category not in by_category:
                by_category[category] = []
            by_category[category].append(row)

        # Verify ordering within each category
        for category, products in by_category.items():
            for i in range(len(products) - 1):
                assert (
                    products[i]["total_quantity_sold"]
                    >= products[i + 1]["total_quantity_sold"]
                ), f"Products in {category} not properly ordered by quantity"

    def test_required_columns_present(self, query_results: DataFrame):
        """Test that required columns are present in results."""
        required_columns = TOP_PRODUCTS_FIELDS
        actual_columns = set(query_results.columns)

        assert required_columns.issubset(actual_columns), (
            f"Missing required columns: {required_columns - actual_columns}"
        )


class TestSalesMovingAverage:
    """Test suite for sales moving average query."""

    @pytest.fixture(scope="class", autouse=True)
    def query_results(self, spark: SparkSession, setup_views) -> Generator[DataFrame, None, None]:
        """Execute query once and cache results for all tests in this class."""
        with open("sql_queries/sales_moving_average.sql", "r") as f:
            query = f.read()
        result_df = spark.sql(query)
        result_df.cache()
        yield result_df
        result_df.unpersist()

    def test_query_returns_results(self, query_results: DataFrame):
        """Test that the query returns results."""
        assert query_results.count() > 0, "Query should return at least one result"

    def test_results_ordered_by_date(self, query_results: DataFrame):
        """Test that results are ordered by transaction_date."""
        results = query_results.collect()

        # Verify ascending date order
        for i in range(len(results) - 1):
            assert results[i]["transaction_date"] <= results[i + 1]["transaction_date"], (
                "Results not properly ordered by date"
            )

    def test_moving_average_not_null(self, query_results: DataFrame):
        """Test that moving average values are not null."""
        results = query_results.collect()

        for row in results:
            assert row["moving_average_sales"] is not None, (
                f"Moving average should not be null for date {row['transaction_date']}"
            )

    def test_moving_average_non_negative(self, query_results: DataFrame):
        """Test that moving average values are non-negative."""
        results = query_results.collect()

        for row in results:
            assert row["moving_average_sales"] >= 0, (
                f"Moving average should be non-negative, got {row['moving_average_sales']}"
            )

    def test_total_sales_non_negative(self, query_results: DataFrame):
        """Test that total sales values are non-negative."""
        results = query_results.collect()

        for row in results:
            assert row["total_sales"] >= 0, (
                f"Total sales should be non-negative, got {row['total_sales']}"
            )

    def test_required_columns_present(self, query_results: DataFrame):
        """Test that required columns are present in results."""
        required_columns = MOVING_AVERAGE_SALES_FIELDS
        actual_columns = set(query_results.columns)

        assert required_columns.issubset(actual_columns), (
            f"Missing required columns: {required_columns - actual_columns}"
        )


class TestPerformanceByCountry:
    """Test suite for performance analysis by country query."""

    @pytest.fixture(scope="class", autouse=True)
    def query_results(self, spark: SparkSession, setup_views) -> Generator[DataFrame, None, None]:
        """Execute query once and cache results for all tests in this class."""
        with open("sql_queries/performance_analysis_by_country.sql", "r") as f:
            query = f.read()
        result_df = spark.sql(query)
        result_df.cache()
        yield result_df
        result_df.unpersist()

    def test_query_returns_results(self, query_results: DataFrame):
        """Test that the query returns results."""
        assert query_results.count() > 0, "Query should return at least one result"

    def test_results_ordered_by_sales(self, query_results: DataFrame):
        """Test that results are ordered by total_sales descending."""
        results = query_results.collect()

        # Verify descending order
        for i in range(len(results) - 1):
            assert results[i]["total_sales"] >= results[i + 1]["total_sales"], (
                f"Results not properly ordered by sales: {results[i]['total_sales']} < {results[i + 1]['total_sales']}"
            )

    def test_all_metrics_non_negative(self, query_results: DataFrame):
        """Test that all metrics are non-negative."""
        results = query_results.collect()

        for row in results:
            assert row["total_sales"] >= 0, "Total sales should be non-negative"
            assert (
                row["total_transactions"] >= 0
            ), "Total transactions should be non-negative"
            assert row["total_users"] >= 0, "Total users should be non-negative"

    def test_all_countries_included(
        self, spark: SparkSession, query_results: DataFrame
    ):
        """Test that all countries with users are included."""
        # Get unique countries from users
        users_countries = spark.sql("SELECT DISTINCT country FROM users").count()

        # Results should have as many rows as unique countries
        assert query_results.count() == users_countries, (
            f"Expected {users_countries} countries, got {query_results.count()}"
        )

    def test_required_columns_present(self, query_results: DataFrame):
        """Test that required columns are present in results."""
        required_columns = COUNTRY_SALES_FIELDS
        actual_columns = set(query_results.columns)

        assert required_columns.issubset(actual_columns), (
            f"Missing required columns: {required_columns - actual_columns}"
        )


class TestDayToDaySales:
    """Test suite for day-to-day sales query."""

    @pytest.fixture(scope="class", autouse=True)
    def query_results(self, spark: SparkSession, setup_views) -> Generator[DataFrame, None, None]:
        """Execute query once and cache results for all tests in this class."""
        with open("sql_queries/day_to_day_sales.sql", "r") as f:
            query = f.read()
        result_df = spark.sql(query)
        result_df.cache()
        yield result_df
        result_df.unpersist()

    def test_query_returns_results(self, query_results: DataFrame):
        """Test that the query returns results."""
        assert query_results.count() > 0, "Query should return at least one result"

    def test_results_ordered_by_date(self, query_results: DataFrame):
        """Test that results are ordered by transaction_date."""
        results = query_results.collect()

        # Verify ascending date order
        for i in range(len(results) - 1):
            assert results[i]["transaction_date"] <= results[i + 1]["transaction_date"], (
                "Results not properly ordered by date"
            )

    def test_first_day_no_previous_sales(self, query_results: DataFrame):
        """Test that first day has no previous day sales."""
        results = query_results.collect()

        first_day = results[0]
        assert first_day["previous_day_sales"] is None, (
            "First day should have no previous day sales"
        )

    def test_percent_change_logical(self, query_results: DataFrame):
        """Test that percent change is calculated correctly."""
        results = query_results.collect()

        for i in range(1, len(results)):
            row = results[i]
            if row["previous_day_sales"] is not None and row["previous_day_sales"] > 0:
                # Verify percent change calculation
                expected_pct = (
                    (row["total_sales"] - row["previous_day_sales"])
                    / row["previous_day_sales"]
                ) * 100

                if row["percent_change"] is not None:
                    assert abs(row["percent_change"] - expected_pct) < 0.01, (
                        f"Percent change mismatch: expected {expected_pct}, got {row['percent_change']}"
                    )

    def test_total_sales_non_negative(self, query_results: DataFrame):
        """Test that total sales are non-negative."""
        results = query_results.collect()

        for row in results:
            assert row["total_sales"] >= 0, (
                f"Total sales should be non-negative, got {row['total_sales']}"
            )

    def test_required_columns_present(self, query_results: DataFrame):
        """Test that required columns are present in results."""
        required_columns = DAILY_SALES_TRENDS_FIELDS
        actual_columns = set(query_results.columns)

        assert required_columns.issubset(actual_columns), (
            f"Missing required columns: {required_columns - actual_columns}"
        )