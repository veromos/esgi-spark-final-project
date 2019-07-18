package io.saagie.esgi.spark

case class Beer(id: String, name: String, brewery_id: String, state: String, country: String, style: String, availability: String, abv: String, notes: String, retired: String)
case class Brewery(id: String, name: String, city: String, state: String, country: String, notes: String , types: String)
case class Review(beer_id: String, username: String, date: String, text: String, look: String, smell: String, taste: String, feel: String, overall: String, score: Double)
case class Stopword(id: String, word: String)

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.DoubleType

object SparkFinalProject {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val beers: Dataset[Beer] = spark.read.option("header", true).csv("hdfs:/data/esgi-spark/final-project/beers.csv").as[Beer]
    val reviews: Dataset[Review] = spark.read.option("header", true).csv("hdfs:/data/esgi-spark/final-project/reviews.csv").withColumn("score", $"score".cast(DoubleType)).as[Review]
    val breweries: Dataset[Brewery] = spark.read.option("header", true).csv("hdfs:/data/esgi-spark/final-project/breweries.csv").as[Brewery]
    val stopwords: Dataset[Stopword] = spark.read.option("header", true).option("delimiter", ";").csv("hdfs:/data/esgi-spark/final-project/stopwords.csv").as[Stopword]

    // Afficher dans les logs la bière qui a le meilleur score
    val beersRanking = beers
      .join(reviews, beers("id") === reviews("beer_id"), "inner")
      .groupBy("beer_id")
      .avg("score")
      .orderBy($"avg(score)".desc)

    beersRanking.show(1)

    // Afficher dans les logs la brasserie qui a le meilleur score en moyenne sur toutes ses bières
    val breweriesRanking = beers
      .join(beersRanking, beers("id") === beersRanking("beer_id"), "left")
      .groupBy("brewery_id")
      .avg("avg(score)")
      .orderBy($"avg(avg(score))".desc)

    breweriesRanking.show(1)

    // Afficher les 10 pays qui ont le plus de brasseries de type Beer-to-go
    val topCountries  = breweries
      .filter(breweries("types").contains("Beer-to-go"))
      .groupBy("country")
      .count()
      .orderBy($"count".desc)

    topCountries.show(10)

    /*
      En vous basant sur le text des reviews, donner le mot qui revient le plus (avec le plus d'occurrences) dans les
      reviews concernant les bières de type IPA (American IPA, American Imperial IPA, New England IPA, etc…).
      Vous devrez vous assurer de supprimer de cette liste les “stop words” dont une liste est disponible ici :
      https://www.textfixer.com/tutorials/common-english-words.txt
    */
    val textDataset = reviews
      .join(beers, beers("id") === reviews("beer_id"), "left")
      .filter(beers("style").contains("IPA"))
      .select(reviews("text"))
      .as[String]
      .flatMap(_.toLowerCase.replaceAll("[^a-z' ]", "").split(" "))
      .filter(value => !value.isEmpty)
      .map(word=>(word,1))
      .groupByKey(_._1)
      .reduceGroups((a, b) => (a._1, a._2 + b._2))
      .map(_._2)
      .orderBy($"_2".desc)
      .join(stopwords, $"_1" === stopwords("word"), "left_anti")
      .show(1)

    println(s"Sleeping for ${args(0)}")

    Thread.sleep(args(0).toLong)
  }
}
