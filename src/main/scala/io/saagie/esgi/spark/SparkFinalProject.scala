package io.saagie.esgi.spark

case class Beer(id: String, name: String, brewery_id: String, state: String, country: String, style: String, availability: String, abv: String, notes: String, retired: String)
case class Brewery(id: String, name: String, city: String, state: String, country: String, notes: String , types: String)
case class Review(beer_id: String, username: String, date: String, text: String, look: String, smell: String, taste: String, feel: String, overall: String, score: Double)

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

object SparkFinalProject {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName(getClass.getSimpleName)
      .getOrCreate()

    import spark.implicits._

    val beers = spark.read.option("header", true).csv("data/beers.csv").as[Beer]
    val reviews = spark.read.option("header", true).csv("data/reviews.csv").withColumn("score", $"score".cast(DoubleType)).as[Review]
    val breweries = spark.read.option("header", true).csv("data/breweries.csv").as[Brewery]

    // Afficher dans les logs la bière qui a le meilleur score
    val beersRanking = beers
      .join(reviews, beers("id") === reviews("beer_id"), "inner")
      .groupBy("beer_id")
      .avg("score").withColumnRenamed("avg(score)", "score_avg")
      .orderBy($"score_avg".desc)

    beersRanking.show(1)

    // Afficher dans les logs la brasserie qui a le meilleur score en moyenne sur toutes ses bières
    val breweriesRanking = beers
      .join(beersRanking, beers("id") === beersRanking("beer_id"), "left")
      .groupBy("brewery_id")
      .avg("score_avg").withColumnRenamed("avg(score_avg)", "brewery_score")
      .orderBy($"brewery_score".desc)

    breweriesRanking.show(1)

    // Afficher les 10 pays qui ont le plus de brasseries de type Beer-to-go
    val top10 = breweries
      .filter(breweries("types").contains("Beer-to-go"))
      .groupBy("country")
      .count()
      .orderBy($"count".desc)
      .show(10)

    /*
      En vous basant sur le text des reviews, donner le mot qui revient le plus (avec le plus d'occurrences) dans les
      reviews concernant les bières de type IPA (American IPA, American Imperial IPA, New England IPA, etc…).
      Vous devrez vous assurer de supprimer de cette liste les “stop words” dont une liste est disponible ici :
      https://www.textfixer.com/tutorials/common-english-words.txt
    */

    println(s"Sleeping for ${args(0)}")

    Thread.sleep(args(0).toLong)
  }

}
