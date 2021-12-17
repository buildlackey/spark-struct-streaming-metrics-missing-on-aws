package com.example

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import java.io.File
import java.util
import java.util.Properties
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object AwsSupportCaseFailsToYieldLogs extends StrictLogging {
  logger.error("THIS SHOULD APPEAR IN LOG")

  case class KafkaEvent(mgpMsgKey: Array[Byte],
                        mgpMsg: Array[Byte],
                        topic: String,
                        partition: String,
                        offset: String) extends Serializable

  case class SparkSessionConfig(appName: String, master: String) {
    def sessionBuilder(): SparkSession.Builder = {
      val builder = SparkSession.builder
      builder.master(master)
      builder
    }
  }

  case class KafkaConfig(kafkaBootstrapServers: String, kafkaTopic: String, kafkaStartingOffsets: String)

  def sessionFactory: (SparkSessionConfig) => SparkSession = {
    (sparkSessionConfig) => {
      sparkSessionConfig.sessionBuilder().getOrCreate()
    }
  }


  def sendEventsToLocalTopic(topic: String, bootstrapServers: String): Unit = {
    import scala.concurrent.ExecutionContext.Implicits.global

    case class KafkaUtils(topic: String, bootstrapServers: String) extends StrictLogging {
      val producer: KafkaProducer[Array[Byte], Array[Byte]] = {
        val kafkaProducerProps: Properties = {
          val props = new Properties()
          props.put("bootstrap.servers", bootstrapServers)
          props.put("key.serializer", classOf[ByteArraySerializer].getName)
          props.put("value.serializer", classOf[ByteArraySerializer].getName)
          props
        }
        new KafkaProducer[Array[Byte], Array[Byte]](kafkaProducerProps)
      }

      def sendEvent(key: Array[Byte], value: Array[Byte]): Unit = {
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, key, value))
      }
    }

    val kafkaUtils = KafkaUtils("test-topic", "kafka:9092")
    Future {
      for (i <- 0 until 100000) {
        kafkaUtils.sendEvent("dummy".getBytes(), i.toString.getBytes())
        Thread.sleep(100) // Pause to make sure event gets there
      }
    }
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new IllegalArgumentException("You need to specify at least 2 args [localTest|cluster] [earliest|latest]")
    }

    val (sparkSessionConfig, kafkaConfig) = {
      val offsetPointer = args(1)
      if (args.length >= 1 && args(0) == "localTest") {
        sendEventsToLocalTopic("test-topic", "localhost:9092")
        getLocalTestConfiguration(offsetPointer)
      } else {
        if (args.length < 4) {
          throw new IllegalArgumentException("In cluster run mode you need to specify " +
            " an additional 2 args, the full list being [localTest|cluster] [earliest|latest]  [brokerAddr] [topic]")
        }
        val brokerAddr = args(2)
        val topic = args(3)
        getRunOnClusterConfiguration(offsetPointer, brokerAddr, topic)
      }
    }
    System.out.println("sparkSessionConfig:" + sparkSessionConfig)
    System.out.println("kafkaConfig:" + kafkaConfig)

    val spark: SparkSession = sessionFactory(sparkSessionConfig)

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val dataSetOfKafkaEvent: Dataset[KafkaEvent] = spark.readStream.
      format("kafka").
      option("subscribe", kafkaConfig.kafkaTopic).
      option("kafka.bootstrap.servers", kafkaConfig.kafkaBootstrapServers).
      option("startingOffsets", kafkaConfig.kafkaStartingOffsets).
      load.
      select(
        $"key" cast "binary",
        $"value" cast "binary",
        $"topic",
        $"partition" cast "string",
        $"offset" cast "string").map { row =>

      KafkaEvent(
        row.getAs[Array[Byte]](0),
        row.getAs[Array[Byte]](1),
        row.getAs[String](2),
        row.getAs[String](3),
        row.getAs[String](4))
    }

    val initDF = dataSetOfKafkaEvent.map { item: KafkaEvent => item.toString }
    val function: (Dataset[String], Long) => Unit =
      (dataSetOfString, batchId) => {
        val iter: util.Iterator[String] = dataSetOfString.toLocalIterator()

        val lines = iter.asScala.toList.mkString("\n")
        val outfile = writeStringToTmpFile(lines)
        println(s"writing to file: ${outfile.getAbsolutePath}")
        logger.error(s"writing to file: ${outfile.getAbsolutePath} /  $lines")
      }

    val trigger = Trigger.ProcessingTime(Duration("1 second"))

    println("Streaming DataFrame : " + initDF.isStreaming)
    println("Schema : " + initDF.printSchema())
    initDF.writeStream
      .foreachBatch(function)
      .trigger(trigger)
      .outputMode("append")
      .start
      .awaitTermination()
  }

  private def getLocalTestConfiguration(offsetPointer: String): (SparkSessionConfig, KafkaConfig) = {
    val sparkSessionConfig: SparkSessionConfig =
      SparkSessionConfig(master = "local[*]", appName = "dummy2")
    val kafkaConfig: KafkaConfig =
      KafkaConfig(
        kafkaBootstrapServers = "localhost:9092",
        kafkaTopic = "test-topic",
        kafkaStartingOffsets = offsetPointer)
    (sparkSessionConfig, kafkaConfig)
  }

  private def getRunOnClusterConfiguration(offsetPointer: String, brokerAddress: String, topic: String) = {
    val sparkSessionConfig: SparkSessionConfig = SparkSessionConfig(master = "yarn", appName = "AwsSupportCase")
    val kafkaConfig: KafkaConfig =
      KafkaConfig(
        kafkaBootstrapServers = brokerAddress,
        kafkaTopic = topic,
        kafkaStartingOffsets = offsetPointer)
    (sparkSessionConfig, kafkaConfig)
  }

  def writeStringFile(string: String, file: File): File = {
    java.nio.file.Files.write(java.nio.file.Paths.get(file.getAbsolutePath), string.getBytes).toFile
  }

  def writeStringToTmpFile(string: String, deleteOnExit: Boolean = false): File = {
    val file: File = File.createTempFile("streamingConsoleMissing", "sad")
    if (deleteOnExit) {
      file.delete()
    }
    writeStringFile(string, file)
  }
}
