package ProcessServerLogs

import java.sql.Timestamp

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.window
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{StringType, StructType}

object Sessionisation
{
  case class RawEvent(timestamp: String, CustomerId: String, EventType: String , Source: String)
  case class Event(timestamp: Timestamp, CustomerId: String, EventType: String , Source: String)
  case class SessionInfo(totalEvents:Int);
  case class SessionUpdate(CustomerId: String, totalEvents: Int, LoggedIn: Boolean);


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
    val events : Dataset[Event]= raw_logs.map(createEvent)

    val sessionUpdates : Dataset[SessionUpdate]= events.groupByKey(_.CustomerId).mapGroupsWithState[SessionInfo,SessionUpdate](GroupStateTimeout.ProcessingTimeTimeout())
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
        state.setTimeoutDuration("5 seconds")
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

    val query = sessionUpdates.writeStream.outputMode(OutputMode.Update()).format("console").start()
    query.awaitTermination();

  }
}
