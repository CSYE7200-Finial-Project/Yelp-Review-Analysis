package fpinscala.z
import play.api.libs.json._
import scala.io._
import scala.collection.mutable
/*
 * this file can clean the reviews of German business 
 * and print out the text of the review context of the other country except Germany City Karlsruhe.
 * @author:yssdnj
 */
object JsonPaserFile extends App {
  val stream_business  = getClass.getResourceAsStream("/business_test.json")
  val stream_review  = getClass.getResourceAsStream("/review_test.json")
  val source_business = Source.fromInputStream(stream_business).getLines
  val source_review = Source.fromInputStream(stream_review).getLines
  
  //  val json1: JsValue = Json.parse(source)
  var counter = 0;
  val karlsruhe_id = mutable.Set.empty[String]
  for (line <- source_business) {
    val text = Json.parse(line)
    val address = (text \ "full_address").get.toString()

    if (address.contains("Karlsruhe")) {
      println((text \ "business_id").get) // all of Karlsruhe business id
      karlsruhe_id += (text \ "business_id").get.toString()
      //counter = counter + 1
    }
  }
  println("---")
  for (line <- source_review) {
    val text = Json.parse(line)
    val business_id = (text \ "business_id").get.toString()
    if (!karlsruhe_id.exists(x => x.equals(business_id))) {
      println((text \ "text").get)
      counter = counter + 1
    }
  }
  println(counter)
}