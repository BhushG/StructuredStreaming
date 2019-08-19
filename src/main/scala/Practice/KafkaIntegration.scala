package Practice
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types._;
import org.apache.spark.sql.functions.col;
import org.apache.spark.sql.functions.from_json;
object KafkaIntegration
{
  def main(args: Array[String]): Unit =
  {
    Logger.getLogger("org").setLevel(Level.ERROR);
    val sparkConf = new SparkConf().setAppName("Kafka App").setMaster("local[*]");
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._;

    val df = sparkSession.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("startingOffsets","earliest").option("subscribe","Topic1").load();
    df.printSchema();  //print schema
    val schema = new StructType(Array(new StructField("Name",StringType),new StructField("Age",IntegerType),new StructField("Salary",DoubleType)))
    val kafkaStream = df.selectExpr("CAST (key as STRING)","CAST (value as STRING)")
    val kafkaStreamData = kafkaStream.select(from_json(col("value"),schema))
      .withColumn("Name",$"jsontostructs(value).Name")
      .withColumn("Salary",$"jsontostructs(value).Salary")
      .withColumn("Age",$"jsontostructs(value).Age")
        .drop(col("jsontostructs(value)"));

    kafkaStreamData.writeStream.outputMode("append").option("checkpointlocation",".//Checkpoint").format("console").start().awaitTermination()

  }
}
