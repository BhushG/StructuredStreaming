package ProcessServerLogs

import java.sql.Timestamp

import ProcessServerLogs.Sessionisation.{Event, RawEvent, SessionInfo, SessionUpdate}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{DoubleType, StringType, StructType}
import scala.collection.mutable.ListBuffer;

object SessionizeCartItems
{

  case class RawEvent(timestamp: String, CustomerId: String, EventType: String , Source: String)
  case class Event(timestamp: Timestamp, CustomerId: String, EventType: String , Source: String)
  case class SessionInfo(totalEvents:Int);
  case class SessionUpdate(CustomerId: String, totalEvents: Int, loggedIn: Boolean);


  case class AddToCartEvent(timestamp: String, CustomerId: String, ProductId: String , Price: Double , Source: String)
  case class Product(ProductId:String,Price:Double)
  case class CartInfo(totalPrice:Double,totalItems:Int,ProductList: ListBuffer[Product]);
  case class CartUpdate(CustomerId: String, totalPrice: Double,totalItems:Int,ProductList: ListBuffer[Product]);

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
    val genericLogs = sparkSession.readStream.schema(genericSchema).json(".//ServerLogs").as[RawEvent];
    val allEvents : Dataset[Event] = genericLogs.map(createEvent)
    val sessionUpdates : Dataset[SessionUpdate]= allEvents.groupByKey(_.CustomerId)
      .mapGroupsWithState[SessionInfo,SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout())
      {
        case (customerId: String, eventsIter: Iterator[Event], state : GroupState[SessionInfo])=>
        {
          val events = eventsIter.toSeq
          val updatedSession = if(state.exists)
          {
            val existingState = state.get
            val updatedTotalEvents = existingState.totalEvents+ events.length
            SessionInfo(updatedTotalEvents)
          }
          else
          {
            SessionInfo(events.length)
          }

          state.update(updatedSession)
          state.setTimeoutDuration("5 minutes")
          val loggedOut = events.filter(event=>event.Source=="LoggedOut").length > 0
          if(state.hasTimedOut)
          {
            state.remove()
            SessionUpdate(customerId,0,false)
          }
          else if(loggedOut)
          {
            state.remove()
            SessionUpdate(customerId,updatedSession.totalEvents,false)
          }
          else
          {
            SessionUpdate(customerId,updatedSession.totalEvents,true)
          }
        }
      }


    //val query = sessionUpdates.writeStream.outputMode(OutputMode.Update()).format("console").start()



    val addToCartEventSchema = new StructType().add("timestamp",StringType).add("CustomerId",StringType).add("EventType",new StructType().add("AddToCart",new StructType().add("ProductId",StringType).add("Price",DoubleType))).add("Source",StringType);
    val rawAddToCartLogs = sparkSession.readStream.schema(addToCartEventSchema).json(".//ServerLogs");
    val addToCartLogsAddedColumns= rawAddToCartLogs.withColumn("ProductId",$"EventType.AddToCart.ProductId").withColumn("Price",$"EventType.AddToCart.Price").drop($"EventType")
    val addToCartLogs: Dataset[AddToCartEvent] = addToCartLogsAddedColumns.filter(row => row.getAs("ProductId")!=null).as[AddToCartEvent]

    val query3 = addToCartLogs.writeStream.outputMode(OutputMode.Update()).format("console").start()
    val cartUpdates : Dataset[CartUpdate] = addToCartLogs.groupByKey(_.CustomerId)
      .mapGroupsWithState[CartInfo,CartUpdate](GroupStateTimeout.NoTimeout())
    {
      case(customerId: String, eventsIter : Iterator[AddToCartEvent], state : GroupState[CartInfo]) =>
        {
          val cartEvents = eventsIter
          var newTot : Double= 0.0;
          var newAddedProduct : ListBuffer[Product] = new ListBuffer[Product]();
          var totItemsAdded =0
          for(event <- eventsIter)
          {
              println("Event for cust "+customerId+" price is "+event.Price)
              newTot= newTot + event.Price;
              newAddedProduct+=new Product(event.ProductId,event.Price)
              totItemsAdded+=1
          }

          println("New Tot for cust "+customerId+" is "+newTot)
          val updatedCart : CartInfo =if(state.exists)
                            {
                              val existingCart : CartInfo = state.get;
                              val newTotVal= newTot + existingCart.totalPrice;
                              val combinedProducts = newAddedProduct++=existingCart.ProductList;
                              val combinedTotalItems= totItemsAdded+existingCart.totalItems;
                              new CartInfo(newTotVal,combinedTotalItems,combinedProducts)
                            }
                            else
                            {
                              new CartInfo(newTot,totItemsAdded,newAddedProduct)
                            }

          state.update(updatedCart)
          CartUpdate(customerId,updatedCart.totalPrice,updatedCart.totalItems,updatedCart.ProductList)
        }
    }


    val query2 = cartUpdates.writeStream.outputMode(OutputMode.Update()).format("console").start()


    //query.awaitTermination();
    query2.awaitTermination();
    query3.awaitTermination();

  }
}



