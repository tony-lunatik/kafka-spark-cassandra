import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;

public class SparkKafka {

    public static final int DURATION = 5000;

    public static void main(String[] args) throws IOException {
        Properties kafkaProperties = new Properties();
        kafkaProperties.load(new FileInputStream("kafka.properties"));

        Properties sparkProperties = new Properties();
        sparkProperties.load(new FileInputStream("spark.properties"));

        final String zookeeperServer = kafkaProperties.getProperty("zookeeper.server");
        final String topic = kafkaProperties.getProperty("topic");
        final int partitionNumber = Integer.parseInt(kafkaProperties.getProperty("partitions"));

        final SparkConf sc = new SparkConf().
                setAppName(sparkProperties.getProperty("app.name")).
                setMaster(sparkProperties.getProperty("master"));

        final JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(DURATION));

        final JavaPairReceiverInputDStream<String, String> requests = KafkaUtils.createStream(
                ssc, zookeeperServer, kafkaProperties.getProperty("consumer.group"), Collections.singletonMap(topic, partitionNumber));

        requests.groupByKey().print();

        ssc.start();
        ssc.awaitTermination();
    }

}
