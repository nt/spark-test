import com.twitter.algebird.AveragedValue
import spark.SparkContext
import SparkContext._
import SparkAlgebird._

object SimpleJob extends App {
  val sc = new SparkContext("local[2]", "Simple Job", "/Users/nicolas/Downloads/spark-0.7.0/",
      List("target/scala-2.9.2/botify-spark_2.9.2-1.0.jar"))

  val urlInfos = sc.textFile("MMT--urlinfos.txt.*")
    .filter(!_.startsWith("#"))
    .map(_.split("\t"))
    .cache()

  /*
    Q1. Pages by HTTP code
    Note: I don't know why a web server would return negative http codes, and since it doesn't make
    any (w3c) sense, I'm just grouping it under a "NEGATIVE_OR_NULL" tag
   */
  println("Pages by status code")
  println("Code\tCount")
  urlInfos
    .map { row =>
      val code = if(row(3).startsWith("-") || row(3) == "0") "NEGATIVE_OR_NULL" else row(3)
      (code, 1)
    }
    .reduceByKeyUsingMonoid()
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
        else if(delay2 <= 1000) ">= 500 ms and <= 1 s"
        else "> 1s"
      (bucket, 1)
    }
    .reduceByKeyUsingMonoid()
    .sortByKey()
    .foreach { case (bucket, count) => println("%s\t%s".format(bucket, count)) }

  /*
    Q3. AVG response time by path directory
    Note, same as before, we will be using delay2.ms
   */
  println("AVG response time by path directory")

  def pathToRoot(path:String) = {
    val s = (path + ".trailing").split("/") //We add .trailing so that /recette/ -> /recette and /recette.html -> /
    if(s.size > 2 && s(1).length > 0) "/"+s(1).toLowerCase+"/" else "/"    //we also have ///crap -> / not sure this is right
  }

  val idToPath = sc.textFile("MMT--urlids.txt.*")
    .filter(!_.startsWith("#"))
    .map { row =>
      val split = row.split("\t")
      split(1) match {
        case "mailto" => (split(0), "EMAIL")
        case _ => (split(0), pathToRoot(split(3)))
      }
    }
    .sortByKey()
    .cache()

  urlInfos
    .map(r => (r(0), r(6).toInt))
    .join(idToPath)
    .map { case (urlId, (delay2, path)) =>
      (path, AveragedValue(delay2))         //This is part of algebird's lib, nice !
    }
    .reduceByKeyUsingMonoid()
    .sortByKey()
    .foreach { case (path, avg) => println("%s\t%f (%d urls)".format(path, avg.value, avg.count)) }

  /*
    Q4. H1 tags presence by first dir
    Q5. H1 uniqueness, by first dir
   */

  println("H1 prensence and uniqueness by path (in percents)")
  println("path\tpresence\tuniqueness")
  val urlContentH1s = sc.textFile("MMT--content.txt.*")
    .filter(r => !r.startsWith("#") && r != "")
    .map(_.split("\t"))
    .filter { r => r(1) == "2" }
    .map(r => (r(0), 1))
    .sortByKey()
    .reduceByKeyUsingMonoid()

  def tupleToPercent(t:(Int, Int)) = {
    val tot = (t._1 + t._2)
    "%.2f%% (%d individuals)".format(100*t._1.toFloat/tot.toFloat, tot)
  }
  idToPath
    .leftOuterJoin(urlContentH1s)
    .map { case (urlid, (path, h1s)) =>
      val q4 = if(h1s.getOrElse(0) >= 1) (1, 0) else (0, 1)
      val q5 = if(h1s.getOrElse(0) == 1) (1, 0) else (0, 1)
      (path, (q4, q5))
    }
    .reduceByKeyUsingMonoid() // Big win here in code readability thanks to algebird
    .foreach { case (path, (q4, q5)) =>
      println("%s\t%s\t%s".format(path, tupleToPercent(q4), tupleToPercent(q5)))
    }

  /*
    Q6 & Q7
   */

  val links = sc.textFile("MMT--links.txt.1").filter(!_.startsWith("#"))
    .map { r =>
      val s = r.split("\t")
      (s(2), s)
    }
    .filter { r => (r._2(2) != "-1") && (r._2(3) != "-1") }  //Filtering out external urls
    .join(idToPath)
    .map { case (_, (link, srcPath)) =>
      (link(3), srcPath)
    }
    .join(idToPath)
    .map { case (_, (srcPath, dstPath)) => ((srcPath, dstPath), 1) }
    .reduceByKeyUsingMonoid()

  println("Outbound links between first directories")
  println("From\tTo\tCount")
  links
    .sortByKey()
    //.foreach { case ((src, dst), count) => println("%s\t%s\t%d".format(src, dst, count)) }

  println("Inbound links between first directories")
  println("To\tFrom\tCount")
  links
    .map { case ((src, dst), count) => ((dst, src), count)}
    .sortByKey()
    //.foreach { case ((dst, src), count) => println("%s\t%s\t%d".format(dst, src, count)) }


}
