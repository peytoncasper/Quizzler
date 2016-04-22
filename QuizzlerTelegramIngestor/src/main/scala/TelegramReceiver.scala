/**
  * Created by peyton on 4/20/16.
  */

package TelegramReceiver
import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer


class TelegramReceiver()
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  def onStart() {
    // Start the thread that receives data over a connection
    new Thread("Telegram Receiver") {
      override def run() { receive() }
    }.start()
  }

  def onStop() {
    // There is nothing much to do as the thread calling receive()
    // is designed to stop by itself if isStopped() returns false
  }

  /** Create a socket connection and receive data until receiver is stopped */
  private def receive() {
    try {
      implicit val system = ActorSystem("TelegramReceiver")
      implicit val materializer = ActorMaterializer()
      // needed for the future flatMap/onComplete in the end
      implicit val executionContext = system.dispatcher
      val route = path("UpdatesWebHook") {
        post {
          entity(as[String]) { request =>
            store(request)
            complete("Request Stored")
          }
        }
      }
      val bindingFuture = Http().bindAndHandle(route, "localhost", 8080)

      while(!isStopped ) {
      }
      bindingFuture
        .flatMap(_.unbind()) // trigger unbinding from the port
        .onComplete(_ ⇒ system.terminate()) // and shutdown when done
      // Restart in an attempt to connect again when server is active again
      restart("Trying to connect again")
    } catch {
      case e: java.net.ConnectException =>
        // restart if could not connect to server
        restart("Error connecting")
      case t: Throwable =>
        // restart if there is any other error
        restart("Error receiving data", t)
    }
  }

}

