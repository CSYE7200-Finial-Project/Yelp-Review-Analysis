package neu.edu.scala.finalyelp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.io._
import java.io._

object PreDataML extends App {
  val conf = new SparkConf().setAppName("PreDataML").setMaster("local")
  val sc = new SparkContext(conf)
  val file = sc.textFile("src/main/resources/wordsDelCom/word5.txt")

  val setIdentical = mutable.Set.empty[String]

  val reg = """^\((\w+)-?&?'?\w*?,(\d+)\)$""".r
  val reg1 = """^\((:\)),(\d+)\)$""".r
  val reg2 = """^\((\$?\d+%?),(\d+)\)$""".r // ($10,8)
  val reg3 = """^\('\w*,(\d+)\)$""".r // ('n, 10)

  //get the words from the (word,count) files into a set
  def getSet(l: Stream[String]): mutable.Set[String] = {
    val set = mutable.Set.empty[String]
    for (line <- l) {
      def getWord(line: String) = line match {
        case reg(word, num)  => set += word
        case reg1(word, num) => set += word
        case reg2(word, num) => set += word
        case reg3(word, num) => set += word
        case _               => set += ""
      }
      getWord(line)
    }
    set
  }

  val set5 = getSet(file.take(1000).toStream) 
  val writer5 = new PrintWriter(new File("src/main/resources/wordFreLine/wordFreLine5.txt"))
  val file5 = sc.textFile("src/main/resources/test5.csv")

  for (line <- file5.flatMap(lines => lines.split("/n")).collect()) {
    val res = sc.parallelize(line.split("[\\.,\\s!;?:\"]+"))
      .filter(word => set5.exists(x => x.equalsIgnoreCase(word)))
      .map(word => (word, 1))
      .reduceByKey(_ + _, 1)
      .map(item => item.swap)
      .sortByKey(false, 1)
      .map(item => item.swap).collect()
    writer5.write("5 " + res.mkString(",") + "\n")
  }


}

