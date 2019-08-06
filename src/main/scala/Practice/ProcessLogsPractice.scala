package Practice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object ProcessLogsPractice
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sparkConf = new SparkConf().setAppName("Process Logs").setMaster("local[*]");
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    import sparkSession.implicits._;


    val addToCartEventSchema = new StructType().add("timestamp",StringType).add("CustomerId",StringType).add("EventType",new StructType().add("AddToCart",new StructType().add("ProductId",StringType).add("Price",StringType))).add("Source",StringType);
    val addToCartLogs_raw_logs = sparkSession.readStream.schema(addToCartEventSchema).json(".//ServerLogs");
    val raw_logs_addedColumns= addToCartLogs_raw_logs.withColumn("ProductId",$"EventType.AddToCart.ProductId").withColumn("Price",$"EventType.AddToCart.Price").drop($"EventType")
    val addToCartLogs= raw_logs_addedColumns.filter(row=> row.getAs("Price")!=null)
    //val query= addToCartLogs.writeStream.format("console").start()
    //query.awaitTermination();

    val purchaseInfo =  ArrayType(new StructType().add("ProductId",StringType).add("Price",StringType).add("Quantity",StringType));
    val purchaseEventSchema = new StructType().add("timestamp",StringType).add("CustomerId",StringType).add("EventType",new StructType().add("Purchase",purchaseInfo)).add("Source",StringType);
    val purchaseLogs_raw_logs = sparkSession.readStream.schema(purchaseEventSchema).json(".//ServerLogs");
    val raw = purchaseLogs_raw_logs.withColumn("Exploded",explode($"EventType.Purchase")).withColumn("ProductId",$"Exploded.ProductId").withColumn("Price",$"Exploded.Price").withColumn("Quantity",$"Exploded.Quantity").drop($"EventType").drop("Exploded")
    //val query= raw.writeStream.format("console").start()
    //query.awaitTermination();



  }
}
