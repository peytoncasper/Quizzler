/**
  * Created by peyton on 4/20/16.
  */
package TelegramIngestor
import java.io.FileNotFoundException

import org.json4s._
import org.json4s.native.Serialization.{read, write}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import TelegramReceiver.TelegramReceiver
import TelegramUpdate.{TelegramChat, TelegramMessage, TelegramMessageAuthor, TelegramUpdate}
import TelegramReplyKeyboardMarkup.{TelegramKeyboardButton, TelegramReplyKeyboardMarkup}
import SlackRTM.{SlackRTM, SlackNotification}
import SlackReceiver.SlackReceiver
import akka.actor.ActorSystem
import akka.http.scaladsl.model.Uri.Query
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import akka.util.{ByteString, Timeout}
import com.datastax.driver.core.{BoundStatement, Cluster, Host, Metadata}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.jfarcand.wcs.{TextListener, WebSocket}

import scala.io.Source
import scala.concurrent.{Await, Future}


object TelegramIngestor {
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats

    var telegram_bot_api_key = ""
    var telegram_webhook_url = ""
    var telegram_webhook_endpoint = ""

    var slack_bot_api_key = ""

    var keyboardsJson = ""

    var parsedKeyboards = Map[String, TelegramReplyKeyboardMarkup]()
    try {
      // Load Configuration Properties
      val appConfig = ConfigFactory.load();
      // Load Bot API Keys from application.conf
      telegram_bot_api_key = appConfig.getString("appConfig.telegram_bot_api_key")
      slack_bot_api_key = appConfig.getString("appConfig.slack_bot_api_key")
      // Load Telegram Webhook URL
      telegram_webhook_url = appConfig.getString("appConfig.telegram_webhook_url")
      // Load Telegram Webhook Endpoint
      telegram_webhook_endpoint = appConfig.getString("appConfig.telegram_webhook_endpoint")
      // Load keyboards.json
      keyboardsJson = Source.fromFile("src/main/resources/keyboards.json").mkString
      // Parse keyboards.json into object
      parsedKeyboards = read[Map[String, TelegramReplyKeyboardMarkup]](keyboardsJson)

    }
    catch{
      case configEx: ConfigException => println("Application.conf is missing a configuration setting (appConfig.getString)")
      case fileNotFound: FileNotFoundException => println("Keyboards.json is missing (Source.fromFile)")
      case jsonMappingError: MappingException => println("Failed to map keyboards.json (json4s)")
    }

    // Establish Spark Conf Object
    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("quizzler-alerts")
      .set("spark.cassandra.connection.host", "127.0.0.1")
    // Establish Spark Streaming Context
    val ssc = new StreamingContext(conf, Seconds(5))

    // Broadcast bot_id across the executors so that its available for interacting with Telegram Bot
    val telegram_bot_api_key_broadcast = ssc.sparkContext.broadcast(telegram_bot_api_key)
    val slack_bot_api_key_broadcast = ssc.sparkContext.broadcast(slack_bot_api_key)
    val keyboards_broadcast = ssc.sparkContext.broadcast(parsedKeyboards)



    val test = startSlackReiver(ssc, slack_bot_api_key)
      .foreachRDD(rdd => rdd.foreachPartition(messagePartition => analyzeSlackMessages(messagePartition, slack_bot_api_key_broadcast)))

    //    val test = startTelegramReceiver(ssc, telegram_webhook_url, telegram_webhook_endpoint, telegram_bot_api_key)
//      .foreachRDD(rdd => rdd.foreachPartition(messagePartition => analyzeMessages(messagePartition, telegram_bot_api_key_broadcast, keyboards_broadcast)))

    //    val parsedMessages = ssc.receiverStream(new TelegramReceiver)
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
  def test(message: RDD[String]): Unit ={

  }
  def analyzeSlackMessages(messages: Iterator[String], slack_bot_api_key_broadcast: Broadcast[String]) ={
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
    val session = cluster.connect()

    messages.foreach{ message =>
      implicit val formats = DefaultFormats
      val parsedMessage = read[SlackNotification](message)
      if(parsedMessage.`type` == "message" && parsedMessage.user.isDefined) {
        val results = session.execute("SELECT * FROM quizzler.account WHERE solr_query='chat_id:" + parsedMessage.channel.get + "'")
        if (results.all().size() == 0) {
          val statement = session.prepare("INSERT INTO quizzler.account (chat_id, chat_username, quiz_interval, last_quiz) VALUES ( ?, ?, ?, ?);")
          val boundStatement = new BoundStatement(statement);
          session.execute(boundStatement.bind(
            parsedMessage.channel.get,
            parsedMessage.user.get,
            new java.math.BigDecimal(5.0),
            new java.util.Date()
          ))
          val rtmJson = scalaj.http.Http("https://slack.com/api/chat.postMessage")
            .param("token", slack_bot_api_key_broadcast.value)
            .param("channel", parsedMessage.channel.get)
            .param("text", "Welcome to Iainbot! I will keep you up to date on your Rightscale instances and offer suggestions to shut them down at certain times that they may not be is use.")
            .param("username", "Iainbot")
            .asString.body
        }
        else if(parsedMessage.text.contains("List Instances"))
        {
          val rtmJson = scalaj.http.Http("https://slack.com/api/chat.postMessage")
            .param("token", slack_bot_api_key_broadcast.value)
            .param("channel", parsedMessage.channel.get)
            .param("text", "You currently have 0 Rightscale instances launched")
            .param("username", "Iainbot")
            .asString.body
          println(rtmJson)
        }
        else
        {
          val rtmJson = scalaj.http.Http("https://slack.com/api/chat.postMessage")
            .param("token", slack_bot_api_key_broadcast.value)
            .param("channel", parsedMessage.channel.get)
            .param("text", "I noticed you've had PLACEHOLDER up for a while, would you like me to shut it down?")
            .param("username", "Iainbot")
            .asString.body
          println(rtmJson)
        }
      }

    }
    cluster.close()
  }

  def analyzeMessages(messages: Iterator[String], telegram_bot_api_key: Broadcast[String], keyboards: Broadcast[Map[String, TelegramReplyKeyboardMarkup]]) ={
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
          sendMessage(telegram_bot_api_key.value,parsedMessage.message.chat.id,"Welcome to Quizzler " + parsedMessage.message.chat.first_name + " " + parsedMessage.message.chat.last_name + "!", "", false)
        }
      else if(parsedMessage.message.text == "Add a Question"){

      }
      else{
//        val telegramReplyKeyboardButtons = mutable.MutableList[mutable.MutableList[TelegramKeyboardButton]]()
//        telegramReplyKeyboardButtons += new mutable.MutableList[TelegramKeyboardButton]()
//        telegramReplyKeyboardButtons(0) += new TelegramKeyboardButton("Add a Question", false, false)
//        telegramReplyKeyboardButtons(0) += new TelegramKeyboardButton("Take a Quiz", false, false)
//        telegramReplyKeyboardButtons += new mutable.MutableList[TelegramKeyboardButton]()
//        telegramReplyKeyboardButtons(1) += new TelegramKeyboardButton("Set Quiz Interval", false, false)
//        telegramReplyKeyboardButtons(1) += new TelegramKeyboardButton("Delete a Question", false, false)
//        val replyKeyboardMarkup = new TelegramReplyKeyboardMarkup(telegramReplyKeyboardButtons, false, true, false)
        sendMessage(telegram_bot_api_key.value,parsedMessage.message.chat.id,"Hey " + parsedMessage.message.chat.first_name + "! What would you like to do?", write(keyboards), true)

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
    val responseFuture: Future[HttpResponse] = akka.http.scaladsl.Http().singleRequest(
      HttpRequest(
        uri = Uri("https://api.telegram.org/bot" + bot_id + "/sendMessage").withQuery(Query(params))
      ))
  }
  def startTelegramReceiver(ssc: StreamingContext, telegram_webhook_url: String, telegram_webhook_endpoint: String, telegram_bot_api_key: String): ReceiverInputDStream[String] ={
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    val params = Map  (
      "url" -> (telegram_webhook_url + "/" + telegram_webhook_endpoint)
    )
    val responseFuture: Future[HttpResponse] = akka.http.scaladsl.Http().singleRequest(
      HttpRequest(
        uri = Uri("https://api.telegram.org/bot" + telegram_bot_api_key + "/setWebhook").withQuery(Query(params))
      ))
    return ssc.receiverStream(new TelegramReceiver(telegram_webhook_endpoint, "localhost", 8080))
  }
  def startSlackReiver(ssc: StreamingContext, slack_bot_api_key: String): ReceiverInputDStream[String] ={
    implicit val formats = DefaultFormats
    val params = Map  (
      "token" -> slack_bot_api_key
    )
    val rtmJson = scalaj.http.Http("https://slack.com/api/rtm.start").param("token", slack_bot_api_key).asString.body
    val parsedRTM = read[SlackRTM](rtmJson)

    return ssc.receiverStream(new SlackReceiver(parsedRTM.url, 443))
  }
}
