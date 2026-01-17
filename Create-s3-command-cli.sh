# Note replace the bucket name with your own S3 bucket before running these commands
# Bucket used here: de-training-v2-dev

# Copy all JSON reference files (category metadata) into a single S3 location
# This keeps reference data separate from the main statistics files
aws s3 cp . s3://de-training-v2-dev/youtube/raw_statistics_reference_data/ \
  --recursive --exclude "*" --include "*.json"

# Upload each country’s CSV file to S3 using Hive-style partitioning
# The region=<code> folders allow Glue and Athena to automatically detect partitions

aws s3 cp CAvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=ca/
aws s3 cp DEvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=de/
aws s3 cp FRvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=fr/
aws s3 cp GBvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=gb/
aws s3 cp INvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=in/
aws s3 cp JPvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=jp/
aws s3 cp KRvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=kr/
aws s3 cp MXvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=mx/
aws s3 cp RUvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=ru/
aws s3 cp USvideos.csv s3://de-training-v2-dev/youtube/raw_statistics/region=us/
