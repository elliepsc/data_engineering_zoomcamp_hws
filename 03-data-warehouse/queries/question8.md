# Question 8 Answer

**Question:** It is best practice in Big Query to always cluster your data?

**Answer:** False

**Explanation:** Clustering is NOT always a best practice because:
- **Cost:** Clustering adds overhead to maintain sorted data
- **Small tables:** For tables < 1 GB, clustering overhead exceeds benefits
- **No clear query pattern:** If queries don't filter/sort on specific columns, clustering is wasteful
- **High cardinality needed:** Clustering works best on high-cardinality columns (many distinct values)

**When to cluster:**
- Large tables (> 1 GB)
- Clear query patterns with WHERE/ORDER BY on specific columns
- Columns with high cardinality
