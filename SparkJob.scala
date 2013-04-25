import spark.SparkContext
import SparkContext._

object SimpleJob extends Application {
  val sc = new SparkContext("local", "Simple Job", "/Users/nicolas/Downloads/spark-0.7.0/",
      List("target/scala-2.9.2/botify-spark_2.9.2-1.0.jar"))

  val urlInfos = sc.textFile("MMT--urlinfos.txt.*")
    .filter(!_.startsWith("#"))
    .map(_.split("\t"))
    .cache()

  /*
    Q1. Pages by HTTP code
    Note: I don't know why a web server would return negative http codes, and since it doesn't make
    any (w3c) sense, I'm just grouping it under a "NEGATIVE" tag
   */
  println("Pages by status code")
  println("Code\tCount")
  urlInfos
    .map { row =>
      val code = if(row(3).startsWith("-")) "NEGATIVE" else row(3)
      (code, 1)
    }
    .reduceByKey(_ + _)
    .sortByKey()
    .foreach { case (code, count) => println("%s\t%d".format(code, count)) }

  /*
    Q2. Pages by response time buckets
    Note: we use delay2.ms (last byte) to bucket pages, we could easily use delay1.ms or an average value
    of those two, depending on what make more sense for the user.
   */
  println("Pages by response time buckets (delay2.ms)")
  println("Bucket\tCount")
  urlInfos
    .map { row =>
      val delay2 = row(6).toInt
      val bucket =
        if(delay2 < 500) "< 500 ms"
        else if(delay2 <= 1000) "< 500 ms and <= 1 s"
        else "> 1s"
      (bucket, 1)
    }
    .reduceByKey(_ + _)
    .sortByKey()
    .foreach { case (bucket, count) => println("%s\t%s".format(bucket, count)) }

}
