package com.availity.spark.provider

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, StringType, StructType}
import org.apache.spark.sql.functions.{array, avg, col, collect_list, count,  lit, month, year}
import org.apache.spark.sql.Column


object ProviderRoster  {

  def main(args: Array[String]): Unit = {

    //Okay so things I didn't add
    //option parser so you could provide options for different file paths for input and outputs

    val spark = SparkSession.builder().master("local[*]").appName(ProviderRoster.getClass.getName).getOrCreate()
    //silence all the spark logs probably need to set this as a configurable option to be passed in
    spark.sparkContext.setLogLevel("WARN")
    val providers = dataFrameReader(spark,"data/providers.csv","|")
    //visits don't have column names lets apply some
    val visits = dataFrameReader(
      spark,
      "data/visits.csv",
      ",",
      List("visit_id","provider_id","date_of_service"),
      false)

    val duplicateRecords = providerIdCalculations(providers, visits)
    visitsPerMonthCalc(visits,duplicateRecords)


  }


  def providerIdCalculations(providers:DataFrame, visits:DataFrame, basePath:String ="target/providerCalculations") : Option[org.apache.spark.sql.DataFrame] = {
    //lets run some data exploration to make sure we have a clean dataset
    //since visits has nothing to id rows aside for provider_id no real use running cleaning exercises
    //with provider id and speciality we have a bit more to go on
    val providersIdsDistinct = providers.select("provider_id", "provider_specialty").distinct().count() == providers.select("provider_id").distinct().count()
    /*
      okay so based on the above "provider_id_distinct there are a few way to handle this
      if it's true then no real processing is needed just groupBy provider_id
      if false I see few different options
          option 1. groupBy provider_id and provider_specialty join with visits and be done
                    however doing it this way will get you incorrect results on number of visits per provider in a time period
          option 2  remove duplicate provider_ids from providers.csv df and join to visits
          option 3. kick out the provider_id's that are duplicates from visits and write NA in number of visits


      since I don't know exactly how you want it I'll produce results for all the options
    * */
    val ignore_inaccurate_results = calculateNumberOfTotalVisits(visits, providers, Seq(col("provider_id")))
    writeDf(ignore_inaccurate_results,s"${basePath}/provider_ids.json",Seq("provider_specialty"))

    if (!providersIdsDistinct) {
      //now lets grab the provider_id's that occur twice
      val duplicate_ids = providers
        .groupBy(col("provider_id"))
        .agg(count("provider_id").as("occurrence"),
          collect_list(col("provider_specialty")).as("provider_specialty_list"),
          collect_list(col("first_name")).as("first_name_list"),
          collect_list(col("middle_name")).as("middle_name_list"),
          collect_list(col("last_name")).as("last_name_list"))
        .filter(col("occurrence") > lit(1))

      //get a clean provider_id set without duplicates
      val clean_providers = providers.select("provider_id").except(duplicate_ids.select("provider_id")).join(providers, Seq("provider_id"), "inner")
      val clean_visits_per_provider = calculateNumberOfTotalVisits(visits, clean_providers, Seq(col("provider_id")))

      val clean_visits = visits.select("provider_id").except(duplicate_ids.select("provider_id")).join(visits, Seq("provider_id"), "inner")
      val providers_dup_with_NA_visits = calculateNumberOfTotalVisits(clean_visits, providers, Seq(col("provider_id")), "right")

      val all_providers_incorrect_visits = providers.
        join(visits.select("provider_id", "visit_id"), Seq("provider_id"), "inner").
        groupBy(col("provider_id"),
          col("provider_specialty")).
        agg(count("visit_id").as("total_visits"))
        .join(providers, Seq("provider_id", "provider_specialty"))

      //dataset contains importation associated to duplicate provider ids
      writeDf(duplicate_ids,s"${basePath}/errorDataset/duplicate_id_info.json")
      //removed duplicate provider id's from dataset "contains what I consider the cleanest data"
      writeDf(clean_visits_per_provider,s"${basePath}/cleaned/provider_ids.json",Seq("provider_specialty"))
      //provider id's maybe duplicate but corresponding visits are left null
      writeDf(providers_dup_with_NA_visits,s"${basePath}/cleaned/providers_dup_with_NA_visits.json",Seq("provider_specialty"))
      //grouped based on id and specialty still contains semi invalid results since visits couldn't all be connected to correct provider
      writeDf(all_providers_incorrect_visits,s"${basePath}/cleaned/provider_id_specialty_visits.json",Seq("provider_specialty"))

      Option(duplicate_ids)
    }else {
      None
    }


  }

  def visitsPerMonthCalc(visits:DataFrame, duplicateRecords:Option[org.apache.spark.sql.DataFrame],basePath:String ="target/visitCalculations"): Unit = {
    val multipleYears = visits.withColumn("year",year(col("date_of_service"))).withColumn("month",month(col("date_of_service")))
    duplicateRecords match {
      case None =>{
        //do nothing since I want to records regardless
        //in a real situation where is was one or the other I'd run the lines
        // val totalNumVisitsPerMonthPerYear = multipleYears.groupBy(col("provider_id"),col("year"),col("month")).agg(count(col("visit_id")).as("total_visits_per_month_per_year"))
        // val totalNumVisitsPerMonth = multipleYears.groupBy(col("provider_id"),col("month")).agg(count(col("visit_id")).as("total_visits_per_month"))
        // writeDf(totalNumVisitsPerMonthPerYear,"target/visitCalculations/regularData/totalVisitsPerMonthPerYear.json")
        // writeDf(totalNumVisitsPerMonth,"target/visitCalculations/regularData/totalVisitsPerMonth.json")
      }
      case Some(dupRecords) => {
        //eliminate duplicate provider_ids
        val cleanedVisits = visits.select("provider_id").except(dupRecords.select("provider_id")).join(multipleYears, Seq("provider_id"), "inner")
        //split dataset into month and year data give accurate representation on "monthly" data
        val totalNumVisitsPerMonthPerYearCleaned = cleanedVisits.groupBy(col("provider_id"), col("year"), col("month")).agg(count(col("visit_id")).as("total_visits_per_month_per_year"))
        //ignore year data and collects cleaned provider ids to particular month regardless of year
        val totalNumVisitsPerMonthCleaned = cleanedVisits.groupBy(col("provider_id"), col("month")).agg(count(col("visit_id")).as("total_visits_per_month"))

        writeDf(totalNumVisitsPerMonthPerYearCleaned,s"${basePath}/cleanedData/totalVisitsPerMonthPerYear.json")
        writeDf(totalNumVisitsPerMonthCleaned,s"${basePath}/cleanedData/totalVisitsPerMonth.json")
      }
    }
    // ignores duplicate data groups month by year calculates sum
    val totalNumVisitsPerMonthPerYear = multipleYears.groupBy(col("provider_id"),col("year"),col("month")).agg(count(col("visit_id")).as("total_visits_per_month_per_year"))
    //ignores duplicate data and years groups on monthly data
    val totalNumVisitsPerMonth = multipleYears.groupBy(col("provider_id"),col("month")).agg(count(col("visit_id")).as("total_visits_per_month"))

    writeDf(totalNumVisitsPerMonthPerYear,s"${basePath}/regularData/totalVisitsPerMonthPerYear.json")
    writeDf(totalNumVisitsPerMonth,s"${basePath}/regularData/totalVisitsPerMonth.json")
  }


  def calculateNumberOfTotalVisits(visits:DataFrame,providers:DataFrame, grpByColumns: Seq[Column],joinType:String = "inner"): DataFrame ={
    visits.groupBy(grpByColumns:_*).agg(count("visit_id").as("total_visits")).join(providers,Seq("provider_id"),joinType)
  }


  /*
  * create a method to write a dataframe to a provided location
  *
  * */
  def writeDf(df:DataFrame, path:String, partitionCols:Seq[String] = Seq(),fileFormat:String="json"): Unit = {
    //add a _ to partition column name to retain it within the data on write
    val t = partitionCols.map(c => ("_"+c,col(c)))
    var tempDf = df
    for(e<- t) {
     tempDf = df.withColumn(e._1,e._2)
    }
    val writerStage = tempDf.write.partitionBy(partitionCols.map(columnName => "_"+columnName):_*).mode(SaveMode.Overwrite)
    fileFormat.toLowerCase() match {
      case "json" =>
        writerStage.json(path)
      case  "csv" =>
        writerStage.csv(path)
      case "orc" =>
        writerStage.orc(path)
      case "parquet"=>
        writerStage.parquet(path)
      case _ =>
        writerStage.csv(path)

    }
  }

  def dataFrameReader(spark:SparkSession,
                      path:String,
                      delimiter:String = ",",
                      colNames:List[String] = List(),
                      header:Boolean = true,
                      format:String = "csv",
                      inferSchema:Boolean = true): DataFrame = {

    val optionsMap = Map(
      "header" -> header.toString,
      "inferSchema" -> inferSchema.toString,
      "delimiter" -> delimiter
    )

    if (colNames.isEmpty)
      spark.read.format(format)
        .options(optionsMap)
        .load(path).toDF()
    else
      spark.read.format(format)
        .options(optionsMap)
        .load(path)
        .toDF(colNames:_*)

  }
}
