package neu.edu.scala.finalyelp.test
import org.scalatest._
import Matchers._

class JsonPaserFileSpec extends FlatSpec with Matchers{
  import neu.edu.scala.finalyelp._
  val jpf = new JsonPaserFile
  val signToSpace = """",,---+=~\n""""
  val variantLetter = "È,É,Ê,Ë,Û,Ù,Ï,Î,À,Â,Ô,è,é,ê,ë,û,ù,ï,î,à,â,ô"
  val signToEmpty = """()".!'\/#{}*@&®[]"""
  signToSpace should "only remain 7 spaces" in {
    val stringRemain = jpf.clean(signToSpace)
    assert(stringRemain === "       ")
  }
  
  variantLetter should "replaced by the corresponding letters" in {
    val letters = jpf.clean(variantLetter).replace(" ", ",")
    assert(letters === "E,E,E,E,U,U,I,I,A,A,O,e,e,e,e,u,u,i,i,a,a,o")
  }
  
  signToEmpty should "be empty" in {
    val stringRemain = jpf.clean(signToEmpty)
    assert(stringRemain === "")
  }
}