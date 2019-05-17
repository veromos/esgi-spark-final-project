package io.saagie.esgi.spark

import org.apache.spark.sql.SparkSession

object SparkFinalProject {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder
      .appName(getClass.getName)
      .getOrCreate()

    val beers = spark.read.option("header", true).csv("data/beers.csv")

    beers.printSchema()
    beers.count()

    /*val reviews = spark.read.option("header", true).csv("data/reviews.csv")
    reviews.count()
    reviews.printSchema()

    val breweries = spark.read.option("header", true).csv("data/breweries.csv")
    breweries.count()
    breweries.printSchema()*/

    // Afficher dans les logs la bière qui a le meilleur score

    // Afficher dans les logs la brasserie qui a le meilleur score en moyenne sur toutes ses bières

    // Afficher les 10 pays qui ont le plus de brasseries de type Beer-to-go

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
