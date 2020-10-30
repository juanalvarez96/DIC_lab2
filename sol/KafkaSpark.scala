package sparkstreaming

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._


object KafkaSpark {
  def main(args: Array[String]) {

    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor':1};")
    session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float)")

    // make a connection to Kafka and read (key, value) pairs from it
    // use kafka with receiver-less  mode where Spark queries Kafka for topic + partition
    val sparkConf = new SparkConf().setAppName("StreamingAverageValue").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(1))
    val kafkaConf = Map(
      "metadata.broker.list" -> "localhost:9092",
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "kafka-spark-streaming",
      "zookeeper.connection.timeout.ms" -> "1000",
      "zookeeper.session.timeout.ms" -> "1000"
    )
    // connect to the topic
    val topicsSet = Set("avg")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaConf, topicsSet)

    // the original format (null, "a, 10"), so we need to extract "a, 10"
    val pairs = kafkaStream.map(x => (x._2.split(",")(0) , x._2.split(",")(1).toDouble))
    //for each value, (null, "a, 10") I take the second one using x._2 then split(",") the take the first element or second element 
    // for the second element I need to convert to double x._2.split(",")(1).toDouble


    // measure the average value for each key in a stateful manner
    // State[(Int, Double) will be the states to keep the values across different mini batch
	def mappingFunc(key: String, value: Option[Double], state: State[(Int, Double)]): (String, Double) = {
		if(!state.exists()){
			state.update(1, value.get)
			(key, value.get)
		}
		else {
			val (prev_count, prev_average) = state.get()
      val mean = (prev_average * prev_count + value.get)/ ( prev_count + 1 )
			state.update(prev_count+1, mean)
      (key,mean)
		}
	}


    val stateDstream = pairs.mapWithState(StateSpec.function(mappingFunc _))

    // print to screen
    stateDstream.print()

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.checkpoint(".")
    ssc.start()
    ssc.awaitTermination()
  }
}