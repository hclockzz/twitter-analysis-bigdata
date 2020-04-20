'''
Original code contributor: mentzera
Article link: https://aws.amazon.com/blogs/big-data/building-a-near-real-time-discovery-platform-with-aws/

'''

es_host = '' #without the https - for example: search-es-twitter-demo-xxxxxxxxxxxxxxxxxxx.us-east-1-.es.amazonaws.com
es_port = 443 # not 80
es_bulk_chunk_size = 1000  #number of documents to index in a single bulk operation

