'''
Original code contributor: mentzera
Article link: https://aws.amazon.com/blogs/big-data/building-a-near-real-time-discovery-platform-with-aws/

'''
import boto3
import re
import requests
import json
from tweet_utils import get_tweet, id_field, get_tweet_mapping
import twitter_to_es


headers = { "Content-Type": "application/json" }

s3 = boto3.client('s3')

# Lambda execution starts here
def handler(event, context):
    for record in event['Records']:

        # Get the bucket name and key for the new file
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']

        # Get s3 object, read, and split the file into lines
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
                
        except Exception as e:
            print(e)
            print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
            raise e

        
            # Parse s3 object content (JSON)
        try:
            # https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
            s3_file_content = obj['Body'].read().decode('utf-8')

            # clean trailing comma
            if s3_file_content.endswith(',\n'):
                s3_file_content = s3_file_content[:-2]
            tweets_str = '['+s3_file_content+']'
            # print(tweets_str)
            tweets = json.loads(tweets_str)

        except Exception as e:
            print(e)
            print('Error loading json from object {} in bucket {}'.format(key, bucket))
            raise e

        # Load data into ES
        try:
            twitter_to_es.load(tweets)

        except Exception as e:
            print(e)
            print('Error loading data into ElasticSearch')
            raise e  