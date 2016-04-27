/**
  * Created by peyton on 4/20/16.
  */
package TelegramIngestor
import org.json4s._
import org.json4s.native.Serialization.{read, write}
import org.json4s.JsonDSL._
import org.joda.time.DateTime
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import TelegramReceiver.TelegramReceiver
import TelegramUpdate.{TelegramChat, TelegramMessage, TelegramMessageAuthor, TelegramUpdate}
import TelegramReplyKeyboardMarkup.{TelegramKeyboardButton, TelegramReplyKeyboardMarkup}
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import com.datastax.driver.core.{BoundStatement, Cluster, Host, Metadata}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable
import scala.concurrent.Future

case class Account(chat_id: String, chatUsername: String, quizInterval: BigDecimal, lastQuiz: DateTime)

object TelegramIngestor {
  def main(args: Array[String]) {
    // Load Configuration File
    val appConfig = ConfigFactory.load();
    var bot_id = ""
    // Error handling for configuration file
    try {
      bot_id = appConfig.getString("appConfig.bot_id")
    }
    catch{
      case configEx: ConfigException => println("You are missing a configuration setting")
    }
    // Establish Spark Conf Object
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("quizzler-alerts")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    // Establish Spark Streaming Context
    val ssc = new StreamingContext(conf, Seconds(5))
    // Broadcast bot_id across the executors so that its available for interacting with Telegram Bot
    val bot_id_broadcast = ssc.sparkContext.broadcast(bot_id)


    val parsedMessages = ssc.receiverStream(new TelegramReceiver)
      .foreachRDD(rdd => rdd.foreachPartition(messagePartition => analyzeMessages(messagePartition, bot_id_broadcast)))
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }

  def analyzeMessages(messages: Iterator[String], bot_id: Broadcast[String]) ={
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    val session = cluster.connect()

    messages.foreach{ message =>
      implicit val formats = DefaultFormats
      val parsedMessage = read[TelegramUpdate](message)

      val results = session.execute("SELECT * FROM quizzler.account WHERE solr_query='chat_id:" + parsedMessage.message.chat.id + "'")
      if(results.all().size() == 0)
        {
          val statement = session.prepare("INSERT INTO quizzler.account (chat_id, chat_username, quiz_interval, last_quiz) VALUES ( ?, ?, ?, ?);")
          val boundStatement = new BoundStatement(statement);
          session.execute(boundStatement.bind(
            parsedMessage.message.chat.id,
            parsedMessage.message.chat.username,
            new java.math.BigDecimal(5.0),
            new java.util.Date()
          ))
//          sendMessage(bot_id.value,parsedMessage.message.chat.id,"Welcome to Quizzler " + parsedMessage.message.chat.first_name + " " + parsedMessage.message.chat.last_name + "!")
        }
      else{
        val telegramReplyKeyboardButtons = mutable.MutableList[mutable.MutableList[TelegramKeyboardButton]]()
        telegramReplyKeyboardButtons += new mutable.MutableList[TelegramKeyboardButton]()
        telegramReplyKeyboardButtons(0) += new TelegramKeyboardButton("Test", false, false)
        val replyKeyboardMarkup = new TelegramReplyKeyboardMarkup(telegramReplyKeyboardButtons, false, true, false)
        sendMessage(bot_id.value,parsedMessage.message.chat.id,"Hey " + parsedMessage.message.chat.first_name + "! What would you like to do?", write(replyKeyboardMarkup), true)

      }

    }
    cluster.close()
  }
  def sendMessage(bot_id: String, chat_id: String, message_text: String, reply_markup_json: String, custom_markup: Boolean): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    var params = Map  (
      "chat_id" -> chat_id,
      "text" -> message_text
    )
    if(custom_markup)
    {
      params = Map (
        "chat_id" -> chat_id,
        "text" -> message_text,
        "reply_markup" -> reply_markup_json
      )
    }
    val responseFuture: Future[HttpResponse] = Http().singleRequest(
      HttpRequest(
        uri = Uri("https://api.telegram.org/bot" + bot_id + "/sendMessage").withQuery(Query(params))
      ))
  }
}
