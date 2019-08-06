package ProcessServerLogs
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.explode

import scala.util.parsing.json.JSON;

object ProcessLogs
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sparkConf = new SparkConf().setAppName("Process Logs").setMaster("local[*]");
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    import sparkSession.implicits._;


    val genericSchema = new StructType().add("timestamp",StringType).add("CustomerId",StringType).add("EventType",StringType).add("Source",StringType);
    val raw_logs = sparkSession.readStream.schema(genericSchema).json(".//ServerLogs");
    val query= raw_logs.writeStream.format("console").start()
    query.awaitTermination();
  }
}
