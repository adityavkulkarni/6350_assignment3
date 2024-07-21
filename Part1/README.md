Assignment 3 Part 1
# Spark Streaming with Real Time Data and Kafka

### Data source used:
Reddit - using the PRAW Python library: https://praw.readthedocs.io/en/latest/code_overview/other/subredditstream.html
PRAW is used to stream the posts(submissions) from the "r/all" and a combination of news and politics related subreddits.

### Tech stack used:
- Pyspark streaming
- Kafka-python
- PRAW
- ELK Stack

### Output:
The report contains the output and snapshots of dashboards
https://github.com/adityavkulkarni/6350_assignment3/blob/master/CS6350_Assignment3.pdf

### Code:
The code is present in the repository:
https://github.com/adityavkulkarni/6350_assignment3/tree/master/Part1

### File structure:
- config.ini: Config file containing kafka producer details and reddit PRAW credentials
- kafka_producer.py: Class for handling Kafka producer
- reddit_scraper.py: Class for streaming Reddit submissions and publishing them to Kafka
- ner_analyser.py: Pyspark streaming script for NER counting and publishing them to Kafka-ELK stack
- logstash-ner.conf: Config file for Logstash for parsing the NER data
- requirements.txt: libraries used

### Execution instructions:
1. Start Zookeeper: ```bin/zookeeper-server-start.sh config/zookeeper.properties```
2. Start Kafka service: ```bin/kafka-server-start.sh config/server.properties```
3. Create topics:
   - reddit: 
   
   ```bin/kafka-topics.sh --create --topic reddit --bootstrap-server localhost:9092```
   - ner: 
   
   ```bin/kafka-topics.sh --create --topic ner --bootstrap-server localhost:9092```
4. Start Elasticsearch: ```cd $ELASTICSEARCH_DIR; bin/elasticsearch```
5. Start Kibana: ```cd $KIBANA_DIR; bin/kibana```
6. Copy ```logstash-ner.conf``` from repository to ```$LOGSTASH_DIR/config```, and replace your credentials
7. Start Logstash: ```cd $LOGSTASH_DIR; bin/logstash -f config/logstash-ner.conf```
8. Create index "ner": ```curl -X PUT "localhost:9200/ner -u user:password```
9. Run ```ner_analyser.py```: ```spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 ner_analyser.py```
10. Run ```reddit_scraper.py```: ```python3 reddit_scrapper.py```
11. Open Kibana: ```http://localhost:5601/app/dashboards#```
12. Go to ```Analytics->Discover->Select Dataview="ner"``` and now you can visualise the data
13. Sample dashboard output is present in the report