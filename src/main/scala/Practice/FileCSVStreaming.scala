package Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types._

object FileCSVStreaming
{
  case class Customers(customerId:String, customerName:String)

  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sparkConf = new SparkConf().setAppName("File CSV Streaming").setMaster("local[*]");
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();


    val transactionType= new StructType().
      add(new StructField("transactionId",IntegerType,true)).
      add(new StructField("customerId",StringType,true)).
      add(new StructField("itemId",IntegerType,true)).
      add(new StructField("amountId",DoubleType,true));


    import sparkSession.implicits._;
    val fileStreamData = sparkSession.readStream.option("header",true).schema(transactionType).csv(".//Input");           //reading streaming data
    val customersBatchData = sparkSession.read.option("header",true).format("csv").load(".//Input//Customers.csv").as[Customers]
    //customersBatchData.show();

    val joinedData = fileStreamData.join(customersBatchData,"customerId");
    val query = joinedData.writeStream.format("console").outputMode(OutputMode.Append()).option("checkpointLocation",".//Checkpoint").start()
    query.awaitTermination();

  }
}
