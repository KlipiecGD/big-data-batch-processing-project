from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def clean_data(df: DataFrame, table_name: str) -> DataFrame:
    """
    Clean the DataFrame by removing rows with null/invalid primary keys 
    and table-specific noise.
    Args:
        df (DataFrame): The input DataFrame.
        table_name (str): The name of the table being processed.
    Returns:
        DataFrame: The cleaned DataFrame.
    """
    # 1. PRIMARY KEY CHECK: Remove any rows where the PK or FKs are NULL
    id_columns = [c for c in df.columns if "_id" in c]
    df = df.dropna(subset=id_columns)

    # 2. INVALID ID CHECK: Remove non-positive IDs (0, -1, -99, etc.)
    for col_name in id_columns:
        df = df.filter(F.col(col_name) > 0)

    # 3. TABLE-SPECIFIC SCENARIOS
    if table_name == "transactions":
        # Ensure quantity is positive (remove -1, -99, 0, and nulls)
        df = df.filter(
            (F.col("quantity").isNotNull()) & 
            (F.col("quantity") > 0)
        )
        
        # Ensure transaction_date is valid - use try_cast to handle invalid dates gracefully
        df = df.withColumn(
            "transaction_date_parsed", 
            F.expr("try_cast(transaction_date as date)")
        )
        df = df.filter(
            (F.col("transaction_date_parsed").isNotNull()) &
            (F.col("transaction_date_parsed") > F.lit("2000-01-01"))
        )
        df = df.drop("transaction_date_parsed")  # Remove helper column
        
    elif table_name == "products":
        # Ensure price is positive (remove -1, -99, 0, and nulls)
        df = df.filter(
            (F.col("price").isNotNull()) & 
            (F.col("price") > 0)
        )
        
        # Ensure name is not null or empty
        df = df.filter(
            (F.col("name").isNotNull()) & 
            (F.col("name") != "")
        )
        
        # Ensure category is not null or empty
        df = df.filter(
            (F.col("category").isNotNull()) & 
            (F.col("category") != "")
        )
        
        # Ensure description is not null or empty
        df = df.filter(
            (F.col("description").isNotNull()) & 
            (F.col("description") != "")
        )
        
    elif table_name == "users":
        # Ensure name is not null or empty
        df = df.filter(
            (F.col("name").isNotNull()) & 
            (F.col("name") != "")
        )
        
        # Ensure email is valid (contains @, not null, not empty, not corrupted)
        df = df.filter(
            (F.col("email").isNotNull()) & 
            (F.col("email") != "") &
            (F.col("email").contains("@")) &
            (~F.col("email").contains("_at_"))  # Remove corrupted emails
        )
        
        # Ensure country is not null or empty
        df = df.filter(
            (F.col("country").isNotNull()) & 
            (F.col("country") != "")
        )
        
        # Ensure address is not null or empty
        df = df.filter(
            (F.col("address").isNotNull()) & 
            (F.col("address") != "")
        )
        
        # Ensure signup_date is valid - use try_cast to handle invalid dates gracefully
        df = df.withColumn(
            "signup_date_parsed", 
            F.expr("try_cast(signup_date as date)")
        )
        df = df.filter(
            (F.col("signup_date_parsed").isNotNull()) &
            (F.col("signup_date_parsed") > F.lit("2000-01-01"))
        )
        df = df.drop("signup_date_parsed")  # Remove helper column
        
    return df