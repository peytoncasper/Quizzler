package TelegramReplyKeyboardMarkup
/**
  * Created by peyton on 4/26/16.
  */
case class TelegramReplyKeyboardMarkup(keyboard: List[List[TelegramKeyboardButton]], resize_keyboard: Boolean, one_time_keyboard: Boolean, selective: Boolean)
case class TelegramKeyboardButton(text: String, request_contact: Boolean, request_location: Boolean)
