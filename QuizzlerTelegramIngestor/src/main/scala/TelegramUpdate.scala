package TelegramUpdate
import java.util.Date

/**
  * Created by peyton on 4/21/16.
  */
case class TelegramUpdate(update_id: Int, message: TelegramMessage)
case class TelegramMessage(message_id: Int, from: TelegramMessageAuthor, chat: TelegramChat, date: String, text: String)
case class TelegramChat(id: String, first_name: String, last_name: String, username: String)
case class TelegramMessageAuthor(id: String, first_name: String, last_name: String, username: String)
