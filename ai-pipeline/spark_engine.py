import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc
import redis
from pymongo import MongoClient

print("🚀 Aether Nightly AI Pipeline Initiated...")

# 1. CONNECT TO CLOUD DATABASES
REDIS_URL = os.environ.get("REDIS_URL")
MONGO_URI = os.environ.get("MONGODB_URI")

if not REDIS_URL or not MONGO_URI:
    print("❌ ERROR: Database credentials missing. Pipeline aborted.")
    exit(1)

redis_client = redis.from_url(REDIS_URL)
mongo_client = MongoClient(MONGO_URI)
db = mongo_client.get_default_database()

# 2. INITIALIZE APACHE SPARK
spark = SparkSession.builder \
    .appName("Aether_Recommendation_Engine") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("⚡ Spark Session Active. Extracting Telemetry from Redis Stream...")

# 3. EXTRACT: Read the raw telemetry queue from Upstash Redis
stream_key = "aether:telemetry:stream"
try:
    messages = redis_client.xrange(stream_key, min="-", max="+")
except Exception as e:
    print(f"❌ Redis Connection Failed: {e}")
    spark.stop()
    exit(1)

if not messages:
    print("💤 No new telemetry data in the stream. Going back to sleep.")
    spark.stop()
    exit(0)

# Parse the JSON payloads
raw_data = []
message_ids = []
for msg_id, msg_data in messages:
    message_ids.append(msg_id)
    try:
        payload = json.loads(msg_data[b'payload'].decode('utf-8'))
        raw_data.append(payload)
    except Exception as e:
        print(f"⚠️ Skipping malformed message: {e}")

if not raw_data:
    print("⚠️ No valid JSON data extracted. Terminating.")
    spark.stop()
    exit(0)

# 4. TRANSFORM: Load into Spark DataFrame for heavy processing
print(f"📊 Processing {len(raw_data)} telemetry events...")
df = spark.createDataFrame(raw_data)

# Group by GhostID and Genre to find true deep affinity scores
user_profiles = df.groupBy("ghostId", "genre") \
    .agg(_sum("watchTimeSeconds").alias("total_watch_time")) \
    .orderBy(desc("total_watch_time"))

# Collect back to Python dictionary
profiles = user_profiles.collect()

# 5. LOAD: Inject the Deep AI Profiles into MongoDB
print("🧠 Spark ML Processing Complete. Updating MongoDB Deep Profiles...")

final_profiles = {}
for row in profiles:
    ghost_id = row['ghostId']
    genre = row['genre']
    score = row['total_watch_time']
    
    if ghost_id not in final_profiles:
        final_profiles[ghost_id] = []
    final_profiles[ghost_id].append({"genre": genre, "score": score})

# Upsert into a MongoDB collection (this updates existing users or creates new ones)
try:
    for ghost_id, genres in final_profiles.items():
        db.userprofiles.update_one(
            {"ghostId": ghost_id},
            {"$set": {"topGenres": genres, "lastUpdated": "Spark-Nightly-Batch"}},
            upsert=True
        )
    print("✅ MongoDB Atlas Successfully Updated.")
except Exception as e:
    print(f"❌ MongoDB Update Failed: {e}")

# 6. CLEANUP: Delete the processed messages from Redis so we don't re-process them tomorrow
if message_ids:
    redis_client.xdel(stream_key, *message_ids)
    print(f"🧹 Cleaned {len(message_ids)} processed events from the Redis Data Lake stream.")

spark.stop()
print("🎉 Aether AI Pipeline successfully completed. Server shutting down.")