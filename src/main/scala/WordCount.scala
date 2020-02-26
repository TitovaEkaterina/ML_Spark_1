
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.immutable.List
import scala.collection.JavaConverters._
import java.io.{BufferedWriter, FileWriter}
import au.com.bytecode.opencsv.CSVWriter
import java.nio.charset.Charset
import org.apache.spark.rdd.RDD
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

  def textFile(sc: SparkContext,
               path: String): RDD[String] =
  { sc.textFile(path).map(str => new String(str.getBytes, 0 , str.length, "utf-8")).setName(path) }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().
      setMaster("local").
      setAppName("StartSparkTitova")
    val sc = new SparkContext(conf)

    val steamBook = textFile(sc, "Text_Dorian/The_Picture_of_Dorian_Gray.txt");
    val book=steamBook
      .flatMap(lines=>lines.split(" "))
      .filter(word=>(!word.equals("")))
      .map(word=>(word,1))
      .reduceByKey(_+_);

    writeToCsv("Text_Dorian/The_Picture_of_Dorian_Gray2.csv",
      (book.sortBy(_._2, false)
        .collect()
        .take(100)
        .map(tuple=>List(tuple._1, tuple._2.toString))
        ).toList)
  }
}
