package ProcessServerLogs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructType}
import java.sql.Timestamp;
import org.apache.spark.sql.functions.window;

object NumberOfEventsPerUnitTime
{
  case class RawEvent(timestamp: String, CustomerId: String, EventType: String , Source: String)
  case class Event(timestamp: Timestamp, CustomerId: String, EventType: String , Source: String)

  def createEvent(raw_event: RawEvent): Event =
  {
    val ts = raw_event.timestamp.split(" ");
    var datePart= ts(0).split(":")
    val timePart= ts(1)

    val newTimestamp = Timestamp.valueOf(datePart(2)+"-"+datePart(1)+"-"+datePart(0)+" "+timePart)
    Event(newTimestamp,raw_event.CustomerId,raw_event.EventType,raw_event.Source)
  }

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sparkConf = new SparkConf().setAppName("Process Logs").setMaster("local[*]");
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    import sparkSession.implicits._;


    val genericSchema = new StructType().add("timestamp", StringType).add("CustomerId", StringType).add("EventType", StringType).add("Source", StringType);
    val raw_logs = sparkSession.readStream.schema(genericSchema).json(".//ServerLogs").as[RawEvent];
    val events = raw_logs.map(createEvent)

    val windowedCount = events.withWatermark("timestamp","5 seconds").groupBy(window($"timestamp","5 seconds")).count()
    val query = windowedCount.writeStream.format("console").start()
    query.awaitTermination();

  }
}
