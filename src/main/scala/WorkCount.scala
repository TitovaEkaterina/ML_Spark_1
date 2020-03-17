
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object WordCount {

  def countWordsInFile(sc: SparkContext,
                       fileName: String): Unit = {
    val steamBook = sc.textFile(fileName + "/" + fileName + ".txt");
    val book=steamBook
      .flatMap(lines=>lines.split("\\p{Punct}|\\p{Blank}"))
      .map(lines=>lines.replaceAll("[^\\p{IsAlphabetic}\\p{IsDigit}]", "").toUpperCase)
      .filter(word=>(!word.equals("")))
      .map(word=>(word,1))
      .reduceByKey(_+_);

    val sqlContext= new SQLContext(sc)
    import sqlContext.implicits._

    book
      .sortBy(_._2, false)
      .coalesce(1,false)
      .toDF("word","count")
      .write.csv(fileName + "/" + fileName + "_res.csv")
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("LearnScalaSpark")
      .set("spark.driver.bindAddress", "127.0.0.1")
    val sc = new SparkContext(conf)



    countWordsInFile(sc,"Chapaev")
    countWordsInFile(sc,"Text_Dorian")
    countWordsInFile(sc,"The_Devils_Dostoevsky")


  }
}

