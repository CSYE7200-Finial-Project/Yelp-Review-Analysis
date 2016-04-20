package neu.edu.scala.finalyelp
import play.api.libs.json._
import scala.io._
import scala.collection.mutable
import java.io._
/*
 * this file can clean the reviews of Germany City Karlsruhe's business 
 * and print out 5 files with review context based on the different stars
 * @author:wanlima
 */

class JsonPaserFile {

  val writer5 = new PrintWriter(new File("src/main/resources/reviewContext/star5.csv"))
  val writer4 = new PrintWriter(new File("src/main/resources/reviewContext/star4.csv"))
  val writer3 = new PrintWriter(new File("src/main/resources/reviewContext/star3.csv"))
  val writer2 = new PrintWriter(new File("src/main/resources/reviewContext/star2.csv"))
  val writer1 = new PrintWriter(new File("src/main/resources/reviewContext/star1.csv"))

  def getKarlsruheId(businessSource: String): Set[String] = {
    val source = scala.io.Source.fromFile(businessSource).getLines
    val karlsruhe_id = mutable.Set.empty[String]
    for (line <- source) {
      val text = Json.parse(line)
      val address = (text \ "full_address").get.toString()
      if (address.contains("Karlsruhe")) {
        karlsruhe_id += (text \ "business_id").get.toString()
      }
    }
    karlsruhe_id.toSet
  }

  def saveReviews(businessSourcePath: String, reviewSourcePath: String) = {
    val karlsruhe_id = getKarlsruheId(businessSourcePath)
    val source = scala.io.Source.fromFile(reviewSourcePath).getLines
    for (line <- source) {
      val text = Json.parse(line)
      val business_id = (text \ "business_id").get.toString()
      if (!karlsruhe_id.exists(x => x.equals(business_id))) {
        val stars = (text \ "stars").get.toString() match {
          case "5" => writer5.write(clean((text \ "text").get.toString.toLowerCase()) + "\n")
          case "4" => writer4.write(clean((text \ "text").get.toString.toLowerCase()) + "\n")
          case "3" => writer3.write(clean((text \ "text").get.toString.toLowerCase()) + "\n")
          case "2" => writer2.write(clean((text \ "text").get.toString.toLowerCase()) + "\n")
          case "1" => writer1.write(clean((text \ "text").get.toString.toLowerCase()) + "\n")
        }
      }
    }
    writer5.close()
    writer4.close()
    writer3.close()
    writer2.close()
    writer1.close()
  }

  def clean(s: String): String = s.replace("""\n""", "").replace("""(""", "").replace(""")""", "")
    .replace("\"", "").replace("""'""", "").replace("|", "").replace("<", "").replace(">", "").replace("·", "")
    .replace("""\""", "").replace("""/""", "").replace("""--""", "").replace("""-""", "").replace("""#""", "").replace("$", "").replace("%", "")
    .replace("""{""", "").replace("""}""", "").replace("""*""", "").replace("""@""", "").replace("""+""", "").replace("""=""", "")
    .replace("""~""", "").replace("ñ", "n").replace("é", "e").replace("ö", "o").replace("&", "").replace("®", "")
    .replace("ä", "a").replace("à", "a").replace("è", "e").replace("ê", "e").replace("]", "").replace("[", "").replace("ç", "c")
    .replaceAll("[èéêë]", "e").replaceAll("[ûùü]", "u").replaceAll("[ïî]", "i").replaceAll("[àâ]", "a").replaceAll("ô", "o")
    .replaceAll("[ÈÉÊË]", "E").replaceAll("[ÛÙ]", "U").replaceAll("[ÏÎ]", "I").replaceAll("[ÀÂ]", "A").replaceAll("Ô", "O")

}