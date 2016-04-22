/**
  * Created by peyton on 4/20/16.
  */
package TelegramIngestor
import org.json4s._
import org.joda.time.DateTime
import org.json4s.native.Serialization.{read, write}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import TelegramReceiver.TelegramReceiver
import TelegramUpdate.{TelegramChat, TelegramMessage, TelegramMessageAuthor, TelegramUpdate}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import org.apache.spark.rdd.RDD

import scala.concurrent.Future

case class Account(chat_id: String, chatUsername: String, quizInterval: BigDecimal, lastQuiz: DateTime)

object TelegramIngestor {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("quizzler-alerts")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    val ssc = new StreamingContext(conf, Seconds(5))

    val existing_users = ssc.cassandraTable[Account]("quizzler", "account").map(x => (x.chat_id, x))

    val parsedMessages = ssc.receiverStream(new TelegramReceiver)
      .foreachRDD(x =>
      {
        implicit val formats = DefaultFormats;
        read[TelegramUpdate](x);
        if(existing_users.lookup())
        x.map(y => {
        })

      })

//    val messages = ssc.receiverStream(new TelegramReceiver)
//                                  .map(x => analyzeMessages(x, existing_users))
    parsedMessages.print()
//    messages.print()
//    test.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    //    val sc = new SparkContext(conf)
    //    val cassandraContext = CassandraConnector(conf)
    //    val quizzler_accounts = sc.cassandraTable("quizzler", "account").cache()
    //    sendMessage("155883415:AAGx6KL9nyEmunhecE8jeowJPdQ2qkCu1R0")

  }
//  def parseMessages(json: String): JValue ={
//
//  }
  def analyzeMessages(message: String, users: RDD[(String, Account)]) ={
    implicit val formats = DefaultFormats
    val parsedMessage = read[TelegramUpdate](message)


    // Determines if the user is already in our hashmap, will be added later if not.
//    val notExists = (users.lookup(parsedMessage.message.chat.id).length == 0)

//    if(notExists)
//    {
      sendMessage("155883415:AAGx6KL9nyEmunhecE8jeowJPdQ2qkCu1R0",parsedMessage.message.chat.id,"Welcome to Quizzler " + parsedMessage.message.chat.first_name + " " + parsedMessage.message.chat.last_name + "!")
//    }

//    return notExists
  }
    def sendMessage(bot_id: String, chat_id: String, message_text: String): Unit = {
      implicit val system = ActorSystem()
      implicit val materializer = ActorMaterializer()
      val params = Map("chat_id" -> chat_id, "text" -> message_text)
      val responseFuture: Future[HttpResponse] = Http().singleRequest(
        HttpRequest(
          uri = Uri("https://api.telegram.org/bot" + bot_id + "/sendMessage").withQuery(Query(params))
        ))
    }
}
