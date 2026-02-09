from google.cloud import storage

# Test version SDK
PROJECT_ID = "de-zoomcamp-module3-486909"
BUCKET_NAME = "dezoomcamp-hw3-2026-ellie"

try:
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET_NAME)
    
    # Liste les fichiers
    blobs = list(bucket.list_blobs(max_results=3))
    
    print("✅ SDK Authentication WORKS!")
    print(f"✅ Found bucket: {BUCKET_NAME}")
    print(f"✅ Files in bucket: {len(list(bucket.list_blobs()))}")
    print(f"\nFirst 3 files:")
    for blob in blobs:
        print(f"  - {blob.name}")
        
except Exception as e:
    print(f"❌ SDK Authentication FAILED: {e}")
