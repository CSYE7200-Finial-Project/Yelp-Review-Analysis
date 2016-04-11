package neu.edu.scala.finalyelp
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable
import scala.collection.mutable
import java.io._

object CleanIdentical extends App{
  
    val conf = new SparkConf().setAppName("word count").setMaster("local")
    val sc = new SparkContext(conf)
    val file5 = sc.textFile("src/main/resources/words5/part-00000")
    val file4 = sc.textFile("src/main/resources/words4/part-00000")
    val file3 = sc.textFile("src/main/resources/words3/part-00000")
    val file2 = sc.textFile("src/main/resources/words2/part-00000")
    val file1 = sc.textFile("src/main/resources/words1/part-00000")

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
        //      val reg(word, num) = line
        //      set += word
      }
      set
    }

    val set5 = getSet(file5.collect().toStream)
    val set4 = getSet(file4.collect().toStream)
    val set3 = getSet(file3.collect().toStream)
    val set2 = getSet(file2.collect().toStream)
    val set1 = getSet(file1.collect().toStream)

    for (i <- 1 to 3) {

      set4.take(800).foreach {
        word =>
          if (set5.take(800).exists(x => x.equals(word))) {
            setIdentical += word
          }
      }

      def getIdenticalSet(set1: mutable.Set[String], set2: mutable.Set[String]): Unit = {
        set1.take(800).foreach {
          word =>
            if (!set2.take(800).exists(x => x.equals(word))) {
              setIdentical -= word
            }
        }
      }
      getIdenticalSet(setIdentical, set3)
      getIdenticalSet(setIdentical, set2)
      getIdenticalSet(setIdentical, set1)

      set1 --= setIdentical
      set2 --= setIdentical
      set3 --= setIdentical
      set4 --= setIdentical
      set5 --= setIdentical

      println(i + " " + setIdentical.size)
      println(set5)
    }
   def set55(set: scala.collection.immutable.Set[String]) = set5
   file5.persist
    //  class wf extends java.io.Serializable {
    val writer5 = new PrintWriter(new File("src/main/resources/wordsDelCom/word5.txt"))
    val writer4 = new PrintWriter(new File("src/main/resources/wordsDelCom/wrod4.txt"))
    val writer3 = new PrintWriter(new File("src/main/resources/wordsDelCom/word3.txt"))
    val writer2 = new PrintWriter(new File("src/main/resources/wordsDelCom/word2.txt"))
    val writer1 = new PrintWriter(new File("src/main/resources/wordsDelCom/word1.txt"))

    //    val writeFile = (file: org.apache.spark.rdd.RDD[String], writer: java.io.PrintWriter) => {
    //      file.foreach(line => line match {
    //        case reg(word, num)  => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
    //        case reg1(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
    //        case reg2(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
    //        case reg3(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer.write(line + "\n") }
    //        case _               => null
    //      })
    //    }
    file5.foreach(line => line match {
      case reg(word, num)  => if (!setIdentical.exists(x => x.equals(word))) { writer5.write(line + "\n") }
      case reg1(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer5.write(line + "\n") }
      case reg2(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer5.write(line + "\n") }
      case reg3(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer5.write(line + "\n") }
      case _               => null
    })
    file4.foreach(line => line match {
      case reg(word, num)  => if (!setIdentical.exists(x => x.equals(word))) { writer4.write(line + "\n") }
      case reg1(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer4.write(line + "\n") }
      case reg2(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer4.write(line + "\n") }
      case reg3(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer4.write(line + "\n") }
      case _               => null
    })
    file3.foreach(line => line match {
      case reg(word, num)  => if (!setIdentical.exists(x => x.equals(word))) { writer3.write(line + "\n") }
      case reg1(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer3.write(line + "\n") }
      case reg2(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer3.write(line + "\n") }
      case reg3(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer3.write(line + "\n") }
      case _               => null
    })
    file2.foreach(line => line match {
      case reg(word, num)  => if (!setIdentical.exists(x => x.equals(word))) { writer2.write(line + "\n") }
      case reg1(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer2.write(line + "\n") }
      case reg2(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer2.write(line + "\n") }
      case reg3(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer2.write(line + "\n") }
      case _               => null
    })
    file1.foreach(line => line match {
      case reg(word, num)  => if (!setIdentical.exists(x => x.equals(word))) { writer1.write(line + "\n") }
      case reg1(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer1.write(line + "\n") }
      case reg2(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer1.write(line + "\n") }
      case reg3(word, num) => if (!setIdentical.exists(x => x.equals(word))) { writer1.write(line + "\n") }
      case _               => null
    })

    writer5.close();
    writer4.close();
    writer3.close();
    writer2.close();
    writer1.close();
  

}