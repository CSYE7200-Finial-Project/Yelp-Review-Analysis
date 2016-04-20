package neu.edu.scala.finalyelp.test

import org.scalatest._
import Matchers._
import collection.mutable.Stack
import scala.io._
import play.api.libs.json._
import neu.edu.scala.finalyelp.CleanIdentical

class StackSpec extends FlatSpec with Matchers{
  val stream_review = getClass.getResourceAsStream("/JsonParser_Test.json")
  val source_review = Source.fromInputStream(stream_review).getLines
  val jsValue_Line1 = Json.parse(source_review.next())
  
  "a JsonParser" should "extract the review context from a json file" in {
    val reviewContext = (jsValue_Line1 \ "text").get.toString()
    assert(reviewContext.===("\"Excellent food\""))
  }
  it should "extract the star level from a json file" in {
    val star = (jsValue_Line1 \ "stars").get.toString()
    assert(star.===("5"))
  }
  
  "A string" should "match a regular expression" in {
    val cl = new CleanIdentical
//    val string01 = """"""
    val string0 = "( ,10)"
    val string1 = """"""
    val string2 = """()"""
    val string3 = """"""
    val string4 = """"""
    val string50 = """(10$$,20)"""
    val string51 = """($$,54)"""
    val string80 = """(a$$,12)"""
    val string61 = """($in,8)"""
    val s ="""@#$%^&*()_+-=~`"""
    string50 should fullyMatch regex cl.reg7
    string51 should fullyMatch regex cl.reg5 
    string61 should fullyMatch regex cl.reg6
    string80 should fullyMatch regex cl.reg8
    val sreplace = s.replaceAll("""[@#$%^&*()_+-=~`]""","")
    assert (s==="")
  }

    
  
}