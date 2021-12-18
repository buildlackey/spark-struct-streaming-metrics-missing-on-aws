package com.example

import com.typesafe.scalalogging.StrictLogging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{Dataset, SparkSession}

import java.io.File
import java.util
import java.util.{Date, Properties, Random}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.Future
import scala.concurrent.duration.Duration

object AwsSupportCaseFailsToYieldLogs extends StrictLogging {
  logger.warn("THIS warn SHOULD APPEAR IN LOG")
  logger.info("THIS info SHOULD APPEAR IN LOG")
  logger.debug("THIS debug SHOULD APPEAR IN LOG")
  logger.trace("THIS trace SHOULD APPEAR IN LOG")

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
        logger.info(s"sending record to kafka: ${value.toString}")
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
    println("args are " + args.toList)
    if (args.length < 2) {
      throw new IllegalArgumentException("You need to specify at least 2 args [localTest|cluster] [earliest|latest]")
    }

    val (sparkSessionConfig, kafkaConfig) = {
      if (args.length >= 1) {
        println("> 1")
      }
      if (args(0) == "localTest") {
        println("is localtest")
      }
      if (args.length >= 1 && args(0) == "localTest") {
        println("is both")
      }

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


    val writeEachRecordToDistinctFile: (Dataset[String], Long) => Unit =
      (dataSetOfString: Dataset[String], batchId: Long) => {
        val folderPathForBatch = s"${System.getProperty("java.io.tmpdir")}/streaming_bug_batch_${batchId}_${new Date().toInstant.toString}"
        val folderForBatch = new File(folderPathForBatch)
        assert(folderForBatch.exists() || folderForBatch.mkdirs())
        dataSetOfString.foreachPartition(
          (iterOfString: Iterator[String]) => {
            val randomGenerator = new Random()
            val folderPathForPartition = s"$folderPathForBatch/${randomGenerator.nextInt().toString}"
            val folderForPartition = new File(folderPathForPartition)
            assert(folderForPartition.exists() || folderForPartition.mkdirs())
            while (iterOfString.hasNext) {
              val string = iterOfString.next()
              val fileForRecord = new File(s"${folderForPartition}/${randomGenerator.nextInt().toString}")
              val fileForRecordPath = fileForRecord.getAbsolutePath
              logger.error(s"writing kafka record to ${fileForRecordPath}")     // lower levels dont show up in output. strange !
              java.nio.file.Files.write(java.nio.file.Paths.get(fileForRecordPath), string.getBytes)
            }
          }
        )
      }


    val trigger = Trigger.ProcessingTime(Duration("1 second"))

    println("Streaming DataFrame : " + initDF.isStreaming)
    println("Schema : " + initDF.printSchema())
    initDF.writeStream
      .foreachBatch(writeEachRecordToDistinctFile)
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
