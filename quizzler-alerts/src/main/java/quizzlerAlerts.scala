
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import scalaj.http.{HttpResponse, HttpOptions, Http}

object quizzlerAlerts {
  def main(args: Array[String]) {

    val conf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("quizzler-alerts")
      .set("spark.cassandra.connection.host", "127.0.0.1")



    val sc = new SparkContext(conf)
    val cassandraContext = CassandraConnector(conf)
    val quizzler_accounts = sc.cassandraTable("quizzler", "account").cache()
//    sendMessage("155883415:AAGx6KL9nyEmunhecE8jeowJPdQ2qkCu1R0")

  }
  def getUpdates(): Unit ={
    
  }
  def sendMessage(bot_id: String, chat_id: String, question: String): Unit ={
    val response: HttpResponse[String] = Http("https://api.telegram.org/bot" + bot_id + "/sendMessage")
      .param("chat_id", chat_id)
      .param("text", question).asString
  }
}