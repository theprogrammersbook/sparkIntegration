# sparkIntegration

# Run application locally on 8 cores
./bin/spark-submit \ --class org.apache.spark.examples.SparkPi \ --master local[8] \ /path/to/examples.jar \ 100

# Run on a Spark standalone cluster in client deploy mode
./bin/spark-submit \ --class org.apache.spark.examples.SparkPi \ --master spark://207.184.161.138:7077 \ --executor-memory 20G \ --total-executor-cores 100 \ /path/to/examples.jar \ 1000

# Run on a Spark standalone cluster in cluster deploy mode with supervise
./bin/spark-submit \ --class org.apache.spark.examples.SparkPi \ --master spark://207.184.161.138:7077 \ --deploy-mode cluster \ --supervise \ --executor-memory 20G \ --total-executor-cores 100 \ /path/to/examples.jar \ 1000

# Run on a YARN cluster
export HADOOP_CONF_DIR=XXX ./bin/spark-submit \ --class org.apache.spark.examples.SparkPi \ --master yarn \ --deploy-mode cluster \ # can be client for client mode --executor-memory 20G \ --num-executors 50 \ /path/to/examples.jar \ 1000

# Run a Python application on a Spark standalone cluster
./bin/spark-submit \ --master spark://207.184.161.138:7077 \ examples/src/main/python/pi.py \ 1000

# Run on a Mesos cluster in cluster deploy mode with supervise
./bin/spark-submit \ --class org.apache.spark.examples.SparkPi \ --master mesos://207.184.161.138:7077 \ --deploy-mode cluster \ --supervise \ --executor-memory 20G \ --total-executor-cores 100 \ http://path/to/examples.jar \ 1000


#Different  Spark Integrations 

# spark-hbase integration

