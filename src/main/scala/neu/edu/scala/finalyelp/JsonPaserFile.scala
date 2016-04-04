package neu.edu.scala.finalyelp
import play.api.libs.json._
import scala.io._
import scala.collection.mutable
import java.io._

object JsonPaserFile extends App {
  val stream_business  = getClass.getResourceAsStream("/business_test.json")
  val stream_review  = getClass.getResourceAsStream("/review_test.json")
  val source_business = Source.fromInputStream(stream_business).getLines
  val source_review = Source.fromInputStream(stream_review).getLines
  val writer = new PrintWriter(new File("src/main/resources/test200.csv" )) 
  //  val json1: JsValue = Json.parse(source)
  var counter = 0;
  val karlsruhe_id = mutable.Set.empty[String]
  val pronoun = Set("I","you","she","he","it","we","they","me")
  val article = Set("a","an","the")
  val adverb = Set("when","where","why","what","how")
  val preposition = Set("from","to","until","over","with","after")
  val conjunction = Set("and","but","or","nor","too")
  val ignoreSet = mutable.Set.empty[String]
  ignoreSet ++= pronoun ++= article ++= adverb ++=preposition ++= conjunction
  
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
      print((text \ "stars").get.toString + " ")
      println((text \ "text").get.toString.replace("""\n""", " "))
      writer.write((text \ "stars").get.toString + " " + (text \ "text").get.toString.replace("""\n""", " ") + "\n")
      counter = counter + 1
    }
  }
  writer.close()
  println(counter)
  
  
  
}