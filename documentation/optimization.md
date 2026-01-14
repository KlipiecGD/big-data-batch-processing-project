# Spark optimization applied

## Dataset Context
- **Users**: 10,000 records
- **Products**: 10,000 records  
- **Transactions**: 18,000 records

All tables are considered **small to medium datasets** for Spark. 
Depending on the data size, different optimizations are appropriate.

---

## 1. Adaptive Query Execution (AQE)

- All AQE features are **enabled by default** in modern Spark:
    - `spark.sql.adaptive.enabled` = **true** (default)
    - `spark.sql.adaptive.coalescePartitions.enabled` = **true** (default)
    - `spark.sql.adaptive.skewJoin.enabled` = **true** (default)

- Benefits of AQE:
    - **Dynamic Optimization**: Spark adjusts execution plans at runtime based on actual data statistics
    - **Auto-coalesce**: Automatically reduces shuffle partitions if data is smaller than expected
    - **Skew Handling**: Automatically detects and handles data skew in joins

### Impact on Our Data
- With 18K transactions, AQE will automatically reduce excessive shuffle partitions
- Optimizes join strategies based on actual data distribution
- Handles any data skew that appears in joins

---

## 2. Shuffle Partitions Tuning

### Configuration
```python
.config("spark.sql.shuffle.partitions", "8")
```

### Default vs Optimized
- **Default**: 200 partitions
- **Optimized**: 8 partitions

### Why Applied
- For small datasets (10K-18K records), 200 partitions means:
    - Each partition has ~50-90 records (too small)
    - Excessive task overhead (200 tasks for tiny data)
    - More time scheduling tasks than processing data

- With 8 partitions:
    - Each partition has ~1,250-2,250 records (optimal size)
    - Reduced scheduler overhead
    - Better CPU utilization

---

## 3. Automatic Broadcast Joins

- Spark auto-broadcasts tables smaller than **10MB** by default:
    - `spark.sql.autoBroadcastJoinThreshold` = **10MB** (default)

- Our tables are well bellow threshold so we don't need explicit broadcast hints.

### How Auto-Broadcast Works

```python
# We write:
transactions.join(users, on="user_id")

# Spark automatically does:
transactions.join(broadcast(users), on="user_id")  
```

### Benefits
- **No manual optimization needed**: Spark decides automatically
- **Cleaner code**: No broadcast imports or hints
- **Dynamic**: Adapts if table sizes change

### When Explicit Broadcast Is Needed
Only in cases when:
- Table is slightly > 10MB but you know it fits in memory
- We have one very small table that exceeds 10MB a bit and one very large table

**Our case**: All tables well under 10MB -> auto-broadcast works perfectly 

---

## 4. Caching Strategy

### Implementation
```python
# Cache dimension tables used in multiple queries
users.cache()
products.cache()
transactions.cache()

# Clean up after use
users.unpersist()
products.unpersist()
transactions.unpersist()
```

### Why Applied
**Gold layer runs 5 different queries**, each joining transactions with users and products.

Without caching:
```
Query 1: Read transactions from disk → process
Query 2: Read transactions from disk → process  (duplicate work)
Query 3: Read transactions from disk → process  (duplicate work)
...
```

With caching:
```
Query 1: Read transactions from disk → cache in memory
Query 2: Read from memory (fast)
Query 3: Read from memory (fast)
...
```
### Benefits
- Eliminates repeated I/O operations
- Speeds up query execution significantly

---

## 5. Repartitioning and Coalescing

### Implementation

```python
# Silver Layer: Initial read and cleaning
# Gold Layer: Repartitioning the large fact table
transactions = transactions.repartition(4)

# Gold Layer: Coalescing result before writing to PostgreSQL
result_df = result_df.coalesce(1)
```

### Why Applied

* **Balanced Parallelism**: For the transaction fact table (~18,000 records), repartitioning to 4 ensures that each Spark task handles roughly 4,500 records. This provides enough parallelism for modern CPUs without creating excessive task overhead.
* **Write Optimization**: The final analytical reports in the Gold layer are typically smaller aggregations. Using `coalesce(1)` ensures that Spark only opens a single connection to the PostgreSQL database to write the final result, which is much more efficient than multiple simultaneous writes for small data.

---

## 6. Avoiding the `count()` Bottleneck

### Implementation

```python
# Log record counts - action triggers computation - only for debugging
# initial_count = df.count()
# logger.info(f"Transactions loaded: {transactions.count()} records")
```

### Why Applied

* **Lazy Evaluation**: Spark uses lazy evaluation, meaning it only processes data when an "action" (like `write` or `count`) is called.
* **Performance Impact**: Calling `.count()` forces Spark to perform a full scan of the dataset across the entire cluster. By commenting out these calls for production, the data is only read and processed once during the final write to the destination.
* **Debugging vs. Production**: While useful for initial development, removing these calls prevents unnecessary I/O and CPU cycles during automated runs.

---

## 7. Parquet Compression

### Implementation

```python
# Write cleaned DataFrame to Parquet - optimize by compression
df.write.option("compression", "snappy").parquet(parquet_path, mode="overwrite")

```

### Why Applied

* **Storage Efficiency**: Snappy compression provides a high compression ratio while remaining very fast to decompress, making it the industry standard for Parquet files.
* **Lower Disk I/O**: Compressing the data in the Silver layer reduces the amount of data Spark needs to read from disk during the Gold layer transformation phase.

---

## 8. Referential Integrity with Auto-Broadcast

### Implementation

```python
# Filter transactions to keep only those with valid foreign keys
# Small tables are automatically broadcasted by Spark
df = df.join(valid_user_ids, on="user_id", how="inner")
df = df.join(valid_product_ids, on="product_id", how="inner")

```

### Why Applied

* **Data Quality**: This ensures that every transaction in the Silver layer corresponds to a real user and a valid product.
* **Performance**: Because the `valid_user_ids` and `valid_product_ids` dataframes are very small, Spark automatically uses a **Broadcast Hash Join**. This avoids a "Shuffle" (sending data across the network), which is the most expensive operation in distributed computing.

---

## Scalability - What If Data Grows?

As the dataset grows beyond the current ~18,000 records, these optimizations will scale as follows:

| Component | At 100k+ Records | At 10M+ Records |
| --- | --- | --- |
| **Shuffle Partitions** | Increase `spark.sql.shuffle.partitions` to 16 or 32. | Set to 2x the total number of CPU cores in the cluster. |
| **Caching** | Monitor memory; use `StorageLevel.MEMORY_AND_DISK`. | Avoid caching entire tables; use selective filtering. |
| **Repartitioning** | Increase partition count (aim for 128MB per partition). | Use `partitionBy()` when writing to disk to enable Partition Pruning. |
| **Write Strategy** | `coalesce(1)` may become a bottleneck. | Remove `coalesce(1)` to allow parallel writes to the database. |

