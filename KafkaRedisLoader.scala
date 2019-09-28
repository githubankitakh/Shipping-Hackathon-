import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.functions._
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

class ClickForeachWriter(p_host: String, p_port: String) extends
  ForeachWriter[Row]{

  val host: String = p_host
  val port: String = p_port

  var jedis: Jedis = _

  def connect() = {
    jedis = new Jedis(host, port.toInt)
  }

  override def open(partitionId: Long, version: Long):
  Boolean = {
    return true
  }

  override def process(record: Row) = {
    var packageId = Option(record.getString(0)).getOrElse("");
    var SourceEventDate = Option(record.getString(1)).getOrElse("");
    var EventFacilityCity = Option(record.getString(2)).getOrElse("");
    var ShipToCity = Option(record.getString(3)).getOrElse("");
    var MerchantID = Option(record.getString(4)).getOrElse("");
    if(jedis == null){
      connect()
    }


    println("***************************" + record.getString(0))
    println("***************************" + record.getString(1))
    println("***************************" + record.getString(2))
    println("***************************" + record.getString(3))
    println("***************************" + record.getString(4))
    jedis.hset("click1", "SourceEventDate", SourceEventDate)
    jedis.hset("click1", "EventFacilityCity", EventFacilityCity)
    jedis.hset("click1", "ShipToCity", ShipToCity)
    jedis.hset("click1", "MerchantID", MerchantID)
    jedis.hset("click1", "packageId", packageId)
    jedis.pfadd("packagecount",packageId)
  }

  override def close(errorOrNull: Throwable) = {
  }
}

object KafkaRedisLoader {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("kafkarediswriter")
      .master("local")
      .config("spark.redis.host", "localhost")
      .config("spark.redis.port", "6379")
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.OFF)

    import spark.implicits._
    val streamingInputDF = spark.readStream.format("kafka")
      .option("kafka server","kafka port")
      .option("subscribe","PackageEvent")
      .option("startingOffsets", "latest")
      .load()

    var streamingSelectDF =
      streamingInputDF
        .select(get_json_object(($"value").cast("string"), "$.PackageId").alias("PackageId"),    get_json_object(($"value").cast("string"), "$.SourceEventDate").alias("SourceEventDate"),get_json_object(($"value").cast("string"), "$.EventFacilityCity").alias("EventFacilityCity"),get_json_object(($"value").cast("string"), "$.ShipToCity").alias("ShipToCity"),get_json_object(($"value").cast("string"), "$.MerchantID").alias("MerchantID"))

    val clickWriter : ClickForeachWriter =
      new ClickForeachWriter("localhost","6379")



    val query = streamingSelectDF
      .writeStream
      .outputMode("update")
      .foreach(clickWriter)
      .start()

    query.awaitTermination()

  }

}

