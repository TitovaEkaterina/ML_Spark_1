
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.List
import scala.collection.JavaConverters._
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import scala.util.{Failure, Try}

object WordCount {

  def writeToCsv(fileName: String,
                 rows: List[List[String]]): Try[Unit] =
    Try(new CSVWriter(new BufferedWriter(new FileWriter(fileName))))
      .flatMap((csvWriter: CSVWriter) =>
        Try{
          csvWriter.writeAll(
            rows.map(_.toArray).asJava
          )
          csvWriter.close()
        } match {
          case f @ Failure(_) =>
            // Always return the original failure.  In production code we might
            // define a new exception which wraps both exceptions in the case
            // they both fail, but that is omitted here.
            Try(csvWriter.close()).recoverWith{
              case _ => f
            }
          case success =>
            success
        }
      )

  def countWordsInFile(sc: SparkContext,
                       fileName: String): Unit = {
      val steamBook = sc.textFile(fileName + "/" + fileName + ".txt");
      val book=steamBook
        .flatMap(lines=>lines.split("\\p{Punct}|\\p{Blank}"))
        .map(lines=>lines.replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}]", "").toUpperCase)
        .filter(word=>(!word.equals("")))
        .map(word=>(word,1))
        .reduceByKey(_+_);

      writeToCsv(fileName + "/" + fileName + "_res.csv",
        (book.sortBy(_._2, false)
          .collect()
          .map(tuple=>List(tuple._1, tuple._2.toString))
          ).toList)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("LearnScalaSpark")
      .set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)

    countWordsInFile(sc,"Chapaev")
    countWordsInFile(sc,"The_Devils_Dostoevsky")


  }
}

