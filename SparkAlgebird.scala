import com.twitter.algebird.{Field, Group, Monoid}
import spark.{SparkContext, RDD}
import SparkContext._

object SparkAlgebird {

  class AlgebirdRDD[K : ClassManifest, V : Monoid : ClassManifest]
    (rdd:RDD[(K, V)]) extends Serializable {

    val m = implicitly[Monoid[V]]

    def reduceByKeyUsingMonoid() = {
      rdd.reduceByKey(m.plus(_, _))
    }
  }

  implicit def toAlgebirdRDD[K : ClassManifest, V : Monoid : ClassManifest](rdd:RDD[(K, V)]) =
    new AlgebirdRDD[K, V](rdd)

  class AlgebirdRDD2[K : Monoid](rdd:RDD[K]) {
    val m = implicitly[Monoid[K]]
    def reduceUsingMonoid() = {
      rdd.reduce(m.plus(_, _))
    }
    def foldUsingMonoid() = {
      rdd.fold(m.zero)(m.plus(_, _))
    }
  }

  implicit def toAlgebirdRDD2[K : Monoid](rdd:RDD[K]) = new AlgebirdRDD2[K](rdd)
}

