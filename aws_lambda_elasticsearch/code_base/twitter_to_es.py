'''
Original code contributor: mentzera
Article link: https://aws.amazon.com/blogs/big-data/building-a-near-real-time-discovery-platform-with-aws/

'''
from elasticsearch.helpers import bulk
import boto3
import config
from elasticsearch.exceptions import ElasticsearchException
from tweet_utils import get_tweet, id_field, get_tweet_mapping
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

region = 'us-east-1' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

index_name = 'twitter'
bulk_chunk_size = config.es_bulk_chunk_size


def create_index(es,index_name,mapping):
    print('creating index {}...'.format(index_name))
    es.indices.create(index_name, body = mapping)


def load(tweets):    
    # es = Elasticsearch(host = config.es_host, port = config.es_port)

    es = Elasticsearch(
        hosts = [{'host': config.es_host, 'port': config.es_port}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )
    es_version_number = es.info()['version']['number']
    mapping_to_put = get_tweet_mapping(es_version_number)

    print(mapping_to_put)
    # mapping = {doc_type: tweet_mapping
    #            }
    mapping = {'mappings':mapping_to_put}


    if es.indices.exists(index_name):
        print ('index {} already exists'.format(index_name))
        try:
            es.indices.put_mapping(body=mapping_to_put, index=index_name)
        except ElasticsearchException as e:
            print('error putting mapping:\n'+str(e))
            print('deleting index {}...'.format(index_name))
            es.indices.delete(index_name)
            create_index(es, index_name, mapping)
    else:
        print('index {} does not exist'.format(index_name))
        create_index(es, index_name, mapping)
    
    counter = 0
    bulk_data = []
    list_size = len(tweets)
    for doc in tweets:
        tweet = get_tweet(doc)
        bulk_doc = {
            "_index": index_name,
            # "_type": doc_type,
            "_id": tweet[id_field],
            "_source": tweet
            }
        bulk_data.append(bulk_doc)
        counter+=1
        
        if counter % bulk_chunk_size == 0 or counter == list_size:
            print ("ElasticSearch bulk index (index: {INDEX})...".format(INDEX=index_name))
            success, _ = bulk(es, bulk_data)

            print ('ElasticSearch indexed %d documents' % success)
            bulk_data = []
  
