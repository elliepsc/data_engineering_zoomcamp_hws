# Question 5 Answer

**Question:** Best strategy for optimization?

**Answer:** Partition by tpep_dropoff_datetime and Cluster on VendorID

**Reasoning:**
- **Partition by date:** When queries filter on tpep_dropoff_datetime, BigQuery only
  scans relevant partitions (days), reducing data scanned significantly
- **Cluster by VendorID:** Within each partition, data is sorted by VendorID, making
  ORDER BY VendorID operations faster
