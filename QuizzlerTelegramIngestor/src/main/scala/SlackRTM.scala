package SlackRTM
/**
  * Created by peyton on 4/26/16.
  */
case class SlackRTM(ok: Boolean, url: String)
case class SlackNotification(`type`: String, url: Option[String], presence: Option[String], user: Option[String], channel: Option[String], text: Option[String], ts: Option[String], team: Option[String])