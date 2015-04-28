package com.stratio.model

import com.stratio.utils.ParserUtils
import org.joda.time.DateTime

sealed case class Cancelled(id: String) {

  override def toString: String = id
}

object OnTime extends Cancelled(id = "OnTime")

object Cancel extends Cancelled(id = "Cancel")

object Unknown extends Cancelled(id = "Unknown")

case class Delays(
                   carrier: Cancelled,
                   weather: Cancelled,
                   nAS: Cancelled,
                   security: Cancelled,
                   lateAircraft: Cancelled)

case class Flight(date: DateTime, //Tip: Use ParserUtils.getDateTime
                  departureTime: Int,
                  crsDepatureTime: Int,
                  arrTime: Int,
                  cRSArrTime: Int,
                  uniqueCarrier: String,
                  flightNum: Int,
                  actualElapsedTime: Int,
                  cRSElapsedTime: Int,
                  arrDelay: Int,
                  depDelay: Int,
                  origin: String,
                  dest: String,
                  distance: Int,
                  cancelled: Cancelled,
                  cancellationCode: Int,
                  delay: Delays) {

  def isGhost: Boolean = arrTime == -1

  def departureDate: DateTime =
    date.hourOfDay.setCopy(departureTime.toString.substring(0, departureTime.toString.size - 2)).minuteOfHour
      .setCopy(departureTime.toString.substring(departureTime.toString.size - 2)).secondOfMinute.setCopy(0)

  def arriveDate: DateTime =
    date.hourOfDay.setCopy(departureTime.toString.substring(0, departureTime.toString.size - 2)).minuteOfHour
      .setCopy(departureTime.toString.substring(departureTime.toString.size - 2)).secondOfMinute.setCopy(0)
      .plusMinutes(cRSElapsedTime)
}

object Flight {

  /*
  *
  * Create a new Flight Class from a CSV file
  *
  */
  def apply(fields: Array[String]): Flight = {
    new Flight(ParserUtils.getDateTime(fields(0).toInt, fields(1).toInt, fields(2).toInt),
               fields(4).toInt,
               fields(5).toInt,
               fields(6).toInt,
               fields(7).toInt,
               fields(8),
               fields(9).toInt,
               fields(11).toInt,
               fields(12).toInt,
               fields(14).toInt,
               fields(15).toInt,
               fields(16),
               fields(17),
               fields(18).toInt,
               parseCancelled(fields(21)),
               fields(22).toInt,
               Delays(parseCancelled(fields(24)), parseCancelled(fields(25)), parseCancelled(fields(26)),
                      parseCancelled(fields(27)),
                      parseCancelled(fields(28))))
  }

  /*
   *
   * Extract the different types of errors in a string list
   *
   */
  def validateDate(fields: Array[String], l: List[(String, String, String)]): List[String] = {
    if (l.isEmpty) {
      List.empty
    } else {
      try {
        ParserUtils.getDateTime(l.head._1.toInt, l.head._2.toInt, l.head._3.toInt)
        validateDate(fields, l.tail)
      } catch {
        case e: Exception => validateDate(fields, l.tail) :+ "DATE"
      }
    }
  }

  def validateInt(fields: Array[String], l: List[Int]): List[String] = {
    if (l.isEmpty) {
      List.empty
    } else {
      try {
        fields(l.head).toInt
        validateInt(fields, l.tail)
      } catch {
        case e: Exception => validateInt(fields, l.tail) :+ "TOINT"
      }
    }
  }

  def extractErrors(fields: Array[String]): Seq[String] = {
    validateInt(fields, List(0, 1, 2, 4, 5, 6, 7, 9, 11, 12, 14, 15, 18, 22)) ++
    validateDate(fields, List((fields(0), fields(1), fields(2))))
  }

  /*
  *
  * Parse String to Cancelled Enum:
  *   if field == 1 -> Cancel
  *   if field == 0 -> OnTime
  *   if field <> 0 && field<>1 -> Unknown
  */
  def parseCancelled(field: String): Cancelled = {
    field match {
      case "1" => Cancel
      case "0" => OnTime
      case _ => Unknown
    }
  }
}
