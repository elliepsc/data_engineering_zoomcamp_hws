# Question 3 Answer

**Question:** Why are the estimated number of Bytes different?

**Answer:** BigQuery is a columnar database, and it only scans the specific columns
requested in the query. Querying two columns (PULocationID, DOLocationID) requires
reading more data than querying one column (PULocationID), leading to a higher
estimated number of bytes processed.

**Evidence:**
- Query 1 (1 column): ~X MB
- Query 2 (2 columns): ~Y MB (approximately 2× Query 1)
