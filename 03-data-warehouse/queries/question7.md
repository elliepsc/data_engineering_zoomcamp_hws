# Question 7 Answer

**Question:** Where is the data stored in the External Table you created?

**Answer:** GCP Bucket

**Explanation:** External tables in BigQuery don't store data in BigQuery itself.
They reference data stored in Google Cloud Storage (GCS bucket). In our case, the
data remains in `gs://dezoomcamp-hw3-2026-ellie/` and BigQuery reads it on-demand.
