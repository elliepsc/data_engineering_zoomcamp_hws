# Question 9 Answer (Bonus)

**Question:** How many bytes will be read for COUNT(*)? Why?

**Estimated bytes:** ~0 MB (or very small)

**Explanation:** BigQuery optimizes COUNT(*) queries by reading only table metadata
instead of scanning actual data. The row count is stored in table metadata, so
BigQuery doesn't need to read the 20M rows - it just looks up the pre-computed count.

This is a major optimization that makes COUNT(*) extremely fast and cheap on
BigQuery, even for massive tables.
