package com.availity.spark.provider

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import java.io.File;
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.functions.{array, avg, col, collect_list, count,  lit, month, year}


class ProviderRosterSpec extends AnyFunSpec with DataFrameComparer with BeforeAndAfterEach {
  val spark = SparkSession.builder().master("local[*]").getOrCreate()
  import spark.implicits._

  val test_visitsDF = Seq(
    (40838,123,"2022-08-23"),
    (58362,123,"2022-03-25"),
    (65989,124,"2021-10-28"),
    (69139,124,"2022-02-04"),
    (84080,125,"2022-07-04"),
    (75368,125,"2021-10-16"),
    (15660,126,"2022-07-04"),
    (49800,126,"2022-04-19"),
    (30380,127,"2021-12-15"),
    (55836,127,"2022-06-28"),
    (12354,128,"2021-12-25"),
    (47292,129,"2022-07-25"),
    (7666,130,"2021-09-28"),
    (38730,130,"2022-06-05"),
    (39086,130,"2022-04-26"),
    (40280,131,"2022-01-22"),
    (13295,132,"2022-05-10"),
    (67185,133,"2021-12-11"),
    (14451,134,"2022-10-10"),
    (84699,135,"2021-11-18"),
    (5839,136,"2021-10-25"),
    (85775,136,"2022-04-04"),
    (44208,134,"2021-10-24"),
    (30441,129,"2022-07-12")).toDF("visit_id","provider_id","date_of_service")

  val duplicate_provider_ids_4_visit_test = Seq(
    (Seq("Bessie", "Julien"), Seq("Kuphal", "Daugherty"),Seq("B", "B"),2,123,Seq("Lord Of The Rings","Indiana Jones")),
  ).toDF("first_name_list","last_name_list","middle_name_list","occurrence","provider_id","provider_specialty_list")

  val test_providerDf = Seq(
    (123,"Lord Of The Rings","Bessie","B","Kuphal"),
    (124,"Lord Of The Rings","Candice","C","Block"),
    (125,"Lord Of The Rings","Margaret","B","Bartell"),
    (126,"Lord Of The Rings","Lenny","B","Brown"),
    (127,"Indiana Jones","Julien","B","Daugherty"),
    (128,"Indiana Jones","Rowena","A","Mertz"),
    (129,"Indiana Jones","Napoleon","A","Barrows"),
    (130,"Indiana Jones","Roy","C","Reichel"),
    (131,"Star Wars","Lavon","C","Stokes"),
    (132,"Star Wars","Tiara","A","Leannon"),
    (133,"Star Wars","Brenna","A","Hayes"),
    (134,"Star Wars","Jose","A","Stoltenberg"),
    (135,"Star Wars","Garfield","C","Kunde"),
    (136,"Star Wars","Bernadette","B","Mertz")
  ).toDF("provider_id","provider_specialty","first_name","middle_name","last_name")

  val provider_cal_results_NO_dups = Seq(
    (123,2,"Lord Of The Rings","Bessie","B","Kuphal"),
    (124,2,"Lord Of The Rings","Candice","C","Block"),
    (125,2,"Lord Of The Rings","Margaret","B","Bartell"),
    (126,2,"Lord Of The Rings","Lenny","B","Brown"),
    (127,2,"Indiana Jones","Julien","B","Daugherty"),
    (128,1,"Indiana Jones","Rowena","A","Mertz"),
    (129,2,"Indiana Jones","Napoleon","A","Barrows"),
    (130,3,"Indiana Jones","Roy","C","Reichel"),
    (131,1,"Star Wars","Lavon","C","Stokes"),
    (132,1,"Star Wars","Tiara","A","Leannon"),
    (133,1,"Star Wars","Brenna","A","Hayes"),
    (134,2,"Star Wars","Jose","A","Stoltenberg"),
    (135,1,"Star Wars","Garfield","C","Kunde"),
    (136,2,"Star Wars","Bernadette","B","Mertz")).toDF("provider_id","total_visits","provider_specialty","first_name","middle_name","last_name")

  val test_providerDf_dup = Seq(
    (123,"Lord Of The Rings","Bessie","B","Kuphal"),
    (124,"Lord Of The Rings","Candice","C","Block"),
    (125,"Lord Of The Rings","Margaret","B","Bartell"),
    (126,"Lord Of The Rings","Lenny","B","Brown"),
    (123,"Indiana Jones","Julien","B","Daugherty"),
    (128,"Indiana Jones","Rowena","A","Mertz"),
    (129,"Indiana Jones","Napoleon","A","Barrows"),
    (130,"Indiana Jones","Roy","C","Reichel"),
    (130,"Star Wars","Lavon","C","Stokes"),
    (132,"Star Wars","Tiara","A","Leannon"),
    (133,"Star Wars","Brenna","A","Hayes"),
    (134,"Star Wars","Jose","A","Stoltenberg"),
    (135,"Star Wars","Garfield","C","Kunde"),
    (136,"Star Wars","Bernadette","B","Mertz")
  ).toDF("provider_id","provider_specialty","first_name","middle_name","last_name")

  val clean_provider_ids = Seq(("Margaret","Bartell","B",125,"Lord Of The Rings",2),
    ("Candice","Block","C",124,"Lord Of The Rings",2),
    ("Napoleon","Barrows","A",129,"Indiana Jones",2),
    ("Lenny","Brown","B",126,"Lord Of The Rings",2),
    ("Rowena","Mertz","A",128,"Indiana Jones",1),
    ("Jose","Stoltenberg","A",134,"Star Wars",2),
    ("Bernadette","Mertz","B",136,"Star Wars",2),
    ("Garfield","Kunde","C",135,"Star Wars",1),
    ("Tiara","Leannon","A",132,"Star Wars",1),
    ("Brenna","Hayes","A",133,"Star Wars",1))
    .toDF("first_name","last_name","middle_name","provider_id","provider_specialty","total_visits")

val provider_id_specialty_visit = Seq(
  ("Margaret","Bartell","B",125,"Lord Of The Rings",2),
  ("Bessie","Kuphal","B",123,"Lord Of The Rings",2),
  ("Candice","Block","C",124,"Lord Of The Rings",2),
  ("Julien","Daugherty","B",123,"Indiana Jones",2),
  ("Napoleon","Barrows","A",129,"Indiana Jones",2),
  ("Lenny","Brown","B",126,"Lord Of The Rings",2),
  ("Rowena","Mertz","A",128,"Indiana Jones",1),
  ("Jose","Stoltenberg","A",134,"Star Wars",2),
  ("Bernadette","Mertz","B",136,"Star Wars",2),
  ("Roy","Reichel","C",130,"Indiana Jones",3),
  ("Garfield","Kunde","C",135,"Star Wars",1),
  ("Tiara","Leannon","A",132,"Star Wars",1),
  ("Brenna","Hayes","A",133,"Star Wars",1),
  ("Lavon","Stokes","C",130,"Star Wars",3))
  .toDF("first_name","last_name","middle_name","provider_id","provider_specialty","total_visits")

  val provider_id_from_duplicate_data = Seq(
    ("Margaret","Bartell","B",125,"Lord Of The Rings",2),
    ("Candice","Block","C",124,"Lord Of The Rings",2),
    ("Bessie","Kuphal","B",123,"Lord Of The Rings",2),
    ("Napoleon","Barrows","A",129,"Indiana Jones",2),
    ("Julien","Daugherty","B",123,"Indiana Jones",2),
    ("Lenny","Brown","B",126,"Lord Of The Rings",2),
    ("Rowena","Mertz","A",128,"Indiana Jones",1),
    ("Bernadette","Mertz","B",136,"Star Wars",2),
    ("Jose","Stoltenberg","A",134,"Star Wars",2),
    ("Roy","Reichel","C",130,"Indiana Jones",3),
    ("Garfield","Kunde","C",135,"Star Wars",1),
    ("Tiara","Leannon","A",132,"Star Wars",1),
    ("Brenna","Hayes","A",133,"Star Wars",1),
    ("Lavon","Stokes","C",130,"Star Wars",3))
    .toDF("first_name","last_name","middle_name","provider_id","provider_specialty","total_visits")

  //aggregated duplicates
  val removed_duplicate_records = Seq(
    (Seq("Bessie", "Julien"), Seq("Kuphal", "Daugherty"),Seq("B", "B"),2,123,Seq("Lord Of The Rings","Indiana Jones")),
    (Seq("Roy", "Lavon"),Seq("Reichel", "Stokes"),Seq("C", "C"),2,130,Seq("Indiana Jones", "Star Wars"))
  ).toDF("first_name_list","last_name_list","middle_name_list","occurrence","provider_id","provider_specialty_list")

  override def beforeEach(): Unit = {
    FileUtils.deleteDirectory(new File("target/test"))
  }

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File("target/test"))
  }

  describe("process") {
    it("should run a test") {
      //if you want to run code all the way through uncomment
     // ProviderRoster.main(Array[String]())
    }
  }

  describe("test the reader implementation") {
    it("should match expected providers") {
      //I could write test all day comparing different params
      assertSmallDataFrameEquality(ProviderRoster.dataFrameReader(spark, "src/test/resources/providers.csv", "|"), test_providerDf, true)
    }
    it("should match expected visits"){
      val visitDf = ProviderRoster.dataFrameReader(spark, "src/test/resources/visits.csv", ",", List("visit_id", "provider_id", "date_of_service"), false)
      assertSmallDataFrameEquality(visitDf.orderBy(col("visit_id")), test_visitsDF.orderBy(col("visit_id")), true)
    }
  }

  describe("test the writer logic") {
    it("should write a dataframe to the provided location") {
      assert(!new File("target/test/*").isDirectory)
      ProviderRoster.writeDf(test_visitsDF, "target/test/visits.json")
      assert(new File("target/test/visits.json").isDirectory)
    }
    it("should write a dataFrame partitioned on specialty") {
      ProviderRoster.writeDf(test_providerDf, "target/test/provider.json", Seq("provider_specialty"))
      assert(new File("target/test/provider.json/_provider_specialty=Star Wars").isDirectory)
    }
  }

  describe("test provider calculations file outputs"){
    describe("without duplicate provider_ids") {
      it("should result is None ") {
        val t = ProviderRoster.providerIdCalculations(test_providerDf, test_visitsDF, "target/test")
        assert(t.isEmpty)
        assert(new File("target/test/provider_ids.json/_provider_specialty=Star Wars").isDirectory)
        assert(new File("target/test/provider_ids.json/_provider_specialty=Indiana Jones").isDirectory)
        assert(new File("target/test/provider_ids.json/_provider_specialty=Lord Of The Rings").isDirectory)
        //cleaned data results should not exist
        assert(!new File("target/test/cleaned/provider_ids.json").isDirectory)
        assert(!new File("target/test/cleaned/provider_id_specialty_visits.json").isDirectory)
        assert(!new File("target/test/cleaned/providers_dup_with_NA_visits.json").isDirectory)
        assert(!new File("target/test/errorDataset/duplicate_id_info.json").isDirectory)
        //since the partition by column has to remain in the result set for consumption later drop it on read
        val processedDF = spark.read.option("inferSchema","true").json("target/test/provider_ids.json").drop("_provider_specialty")
        val reorderedDf = reorderAndCastTypes(Provider(processedDF,provider_cal_results_NO_dups))
        assertSmallDataFrameEquality(reorderedDf,provider_cal_results_NO_dups,true)
      }
    }
    describe("with duplicate provider_ids") {
      it("should result in Some since duplicates are added ") {
        val t = ProviderRoster.providerIdCalculations(test_providerDf_dup, test_visitsDF, "target/test")
        assert(t.isDefined)
        spark.sparkContext.setLogLevel("ERROR")
        //with duplicate
        assert(new File("target/test/provider_ids.json/_provider_specialty=Star Wars").isDirectory)
        assert(new File("target/test/provider_ids.json/_provider_specialty=Indiana Jones").isDirectory)
        assert(new File("target/test/provider_ids.json/_provider_specialty=Lord Of The Rings").isDirectory)
        //cleaned data results
        assert(new File("target/test/cleaned/provider_ids.json").isDirectory)
        assert(new File("target/test/cleaned/provider_id_specialty_visits.json").isDirectory)
        assert(new File("target/test/cleaned/providers_dup_with_NA_visits.json").isDirectory)
        assert(new File("target/test/errorDataset/duplicate_id_info.json").isDirectory)

        //cleaned provider_id grouped with specialty
        val reordered_spec_id = reorderAndCastTypes(Provider(spark.read.json("target/test/cleaned/provider_id_specialty_visits.json"),provider_id_specialty_visit))
        assertSmallDataFrameEquality(
          reordered_spec_id,
          provider_id_specialty_visit.orderBy("provider_id","total_visits","provider_specialty","first_name","middle_name","last_name"),true)

        //clean provider_ids removed duplicate provider_ids
        val df_reordered_pro_id = reorderAndCastTypes(
          Provider(
            spark.read.json("target/test/cleaned/provider_ids.json"),
            clean_provider_ids))
        assertSmallDataFrameEquality(df_reordered_pro_id,clean_provider_ids.orderBy("provider_id","total_visits","provider_specialty","first_name","middle_name","last_name"),true)

        //dataframe comparions where duplicate provider ids eliminate inaccurate visit counts
        val expectedDf = spark.read.format("csv")
          .options(Map("header"->"true","inferSchema"->"true"))
          .load("src/test/resources/provider_duplicate_with_NA_visitCount.csv")
        val df_reordered_withNA = reorderAndCastTypes(
          Provider(
            spark.read.json("target/test/cleaned/providers_dup_with_NA_visits.json"),
            expectedDf))
        assertSmallDataFrameEquality(
          df_reordered_withNA,
          expectedDf.orderBy("provider_id","total_visits","provider_specialty","first_name","middle_name","last_name"),true)

        //compare filtered out data where provider_ids are duplicates
        //results are aggregated at a row level compressed into list of values
        val reordered_Error_records = reorderAndCastTypes(
          ErrorRecords(
            spark.read.json("target/test/errorDataset/duplicate_id_info.json"),
            removed_duplicate_records))
        assertSmallDataFrameEquality(
          reordered_Error_records,
          removed_duplicate_records.orderBy("provider_id","first_name_list","middle_name_list","last_name_list","occurrence","provider_specialty_list"),true)

        //no cleaning of data
        val reordered_df_with_duplicates =reorderAndCastTypes(
          Provider(
            spark.read.json("target/test/provider_ids.json"),
            provider_id_from_duplicate_data))
        assertSmallDataFrameEquality(
          reordered_df_with_duplicates,
          provider_id_from_duplicate_data.orderBy("provider_id","total_visits","provider_specialty","first_name","middle_name","last_name"),true)
      }
    }
  }

  describe("test visit counts"){
    describe("visit ids when no duplicates are detected"){
      it("should result with with two dataFrames one containing " +
        "number of visits per year per month " +
        "and the other with just months") {
        val duplicateDF = Option()
        ProviderRoster.visitsPerMonthCalc(test_visitsDF, Option.empty, "target/test/visitCalculations")
        assert(new File("target/test/visitCalculations/regularData/totalVisitsPerMonthPerYear.json").isDirectory)
        assert(new File("target/test/visitCalculations/regularData/totalVisitsPerMonth.json").isDirectory)
        assert(!new File("target/test/visitCalculations/cleanedData").isDirectory)
        val actual_per_month_per_year = spark.read.json("target/test/visitCalculations/regularData/totalVisitsPerMonthPerYear.json")
        val actual_per_month = spark.read.json("target/test/visitCalculations/regularData/totalVisitsPerMonth.json")
        val expected_per_month_per_year = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("src/test/resources/provider_visits_per_mon_per_yr_reg.csv")
        val expected_per_month = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("src/test/resources/provider_visits_per_month_reg.csv")


        //compare values to determine if results are accurate
        val reordered_visits_monthly = reorderAndCastTypes(Visits(actual_per_month, expected_per_month, false))
        assertSmallDataFrameEquality(
          reordered_visits_monthly,
          expected_per_month.orderBy("provider_id", "month", "total_visits_per_month"), true)


        val reordered_visits_month_year = reorderAndCastTypes(Visits(actual_per_month_per_year, expected_per_month_per_year, true))
        assertSmallDataFrameEquality(
          reordered_visits_month_year,
          expected_per_month_per_year.orderBy("provider_id", "month", "total_visits_per_month_per_year", "year"), true)
      }
    }
    describe("visit ids when provided with duplicates"){
      it("should result in 4 dataFrames two cleaned and to uncleaned for the sake of the assignment"){
        spark.sparkContext.setLogLevel("ERROR")
        ProviderRoster.visitsPerMonthCalc(test_visitsDF, Some(duplicate_provider_ids_4_visit_test), "target/test/visitCalculations")
        assert(new File("target/test/visitCalculations/regularData/totalVisitsPerMonth.json").isDirectory)
        assert(new File("target/test/visitCalculations/regularData/totalVisitsPerMonthPerYear.json").isDirectory)
        assert(new File("target/test/visitCalculations/cleanedData/totalVisitsPerMonthPerYear.json").isDirectory)
        assert(new File("target/test/visitCalculations/cleanedData/totalVisitsPerMonth.json").isDirectory)

        val actual_per_month_per_year = spark.read.json("target/test/visitCalculations/regularData/totalVisitsPerMonthPerYear.json")
        val actual_per_month = spark.read.json("target/test/visitCalculations/regularData/totalVisitsPerMonth.json")
        val expected_per_month_per_year = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("src/test/resources/provider_visits_per_mon_per_yr_reg.csv")
        val expected_per_month = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("src/test/resources/provider_visits_per_month_reg.csv")

        //compare values to determine if results are accurate
        val reordered_visits_monthly = reorderAndCastTypes(Visits(actual_per_month, expected_per_month, false))
        assertSmallDataFrameEquality(
          reordered_visits_monthly,
          expected_per_month.orderBy("provider_id", "month", "total_visits_per_month"), true)


        val reordered_visits_month_year = reorderAndCastTypes(Visits(actual_per_month_per_year, expected_per_month_per_year, true))
        assertSmallDataFrameEquality(
          reordered_visits_month_year,
          expected_per_month_per_year.orderBy("provider_id", "month", "total_visits_per_month_per_year", "year"), true)

        val actual_cleaned_per_month_per_year = spark.read.json("target/test/visitCalculations/cleanedData/totalVisitsPerMonthPerYear.json")
        val actual_cleaned_per_month = spark.read.json("target/test/visitCalculations/cleanedData/totalVisitsPerMonth.json")
        val expected_clean_per_month_per_year = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("src/test/resources/provider_visits_per_mon_per_yr_cleaned.csv")
        val expected_clean_per_month = spark.read.format("csv").options(Map("header" -> "true", "inferSchema" -> "true")).load("src/test/resources/provider_visits_per_month_cleaned.csv")

        //compare cleaned values to determine if results are accurate
        val reordered_visits_monthly_cleaned = reorderAndCastTypes(Visits(actual_cleaned_per_month, expected_clean_per_month, false))
        assertSmallDataFrameEquality(
          reordered_visits_monthly_cleaned,
          expected_clean_per_month.orderBy("provider_id", "month", "total_visits_per_month"), true)
        //prove that provider id 123 doesn't exist within returned dataframe
        assert(actual_cleaned_per_month.select("provider_id").filter(col("provider_id") === lit(123)).count() === 0)
        assert(actual_cleaned_per_month_per_year.select("provider_id").filter(col("provider_id") === lit(123)).count() === 0)

        val reordered_visits_month_year_cleaned = reorderAndCastTypes(Visits(actual_cleaned_per_month_per_year, expected_clean_per_month_per_year, true))
        assertSmallDataFrameEquality(
          reordered_visits_month_year_cleaned,
          expected_clean_per_month_per_year.orderBy("provider_id", "month", "total_visits_per_month_per_year", "year"), true)
      }
    }
  }

  def reorderAndCastTypes(p:ErrorRecords):DataFrame = {
    val staged = p.incoming.withColumn("first_name_list",col("first_name_list").cast(p.actual.schema("first_name_list").dataType))
      .withColumn("last_name_list",col("last_name_list").cast(p.actual.schema("last_name_list").dataType))
      .withColumn("middle_name_list",col("middle_name_list").cast(p.actual.schema("middle_name_list").dataType))
      .withColumn("occurrence",col("occurrence").cast(p.actual.schema("occurrence").dataType))
      .withColumn("provider_id",col("provider_id").cast(p.actual.schema("provider_id").dataType))
      .withColumn("provider_specialty_list",col("provider_specialty_list").cast(p.actual.schema("provider_specialty_list").dataType))
    staged.select(p.actual.columns.map(columnName => col(columnName)): _*)
      .orderBy("provider_id","first_name_list","middle_name_list","last_name_list","occurrence","provider_specialty_list")
  }

  def reorderAndCastTypes(p:Provider):DataFrame = {
    val staged = p.incoming.withColumn("provider_id",col("provider_id").cast(p.actual.schema("provider_id").dataType))
    .withColumn("total_visits",col("total_visits").cast(p.actual.schema("total_visits").dataType))
    .withColumn("provider_specialty",col("provider_specialty").cast(p.actual.schema("provider_specialty").dataType))
    .withColumn("first_name",col("first_name").cast(p.actual.schema("first_name").dataType))
    .withColumn("middle_name",col("middle_name").cast(p.actual.schema("middle_name").dataType))
    .withColumn("last_name",col("last_name").cast(p.actual.schema("last_name").dataType))

    staged.select(p.actual.columns.map(columnName => col(columnName)): _*)
      .orderBy("provider_id","total_visits","provider_specialty","first_name","middle_name","last_name")
  }
  def reorderAndCastTypes(v:Visits):DataFrame = {
    //fix column types that exist within both returned dataFrames
   val stageDf =  v.incoming.withColumn("provider_id",col("provider_id").cast(v.actual.schema("provider_id").dataType))
      .withColumn("month",col("month").cast(v.actual.schema("month").dataType))
   val stage2 = if (v.containsYear) {
     //fix column types that exist within dataFrames where year is taken into consideration
      stageDf.withColumn("year",col("year").cast(v.actual.schema("year").dataType))
        .withColumn("total_visits_per_month_per_year",col("total_visits_per_month_per_year")
          .cast(v.actual.schema("total_visits_per_month_per_year").dataType))
        .orderBy("provider_id","month","total_visits_per_month_per_year","year")
    } else {
     //fix column types that exist within dataFrames where year is not taken into consideration
      stageDf
        .withColumn("total_visits_per_month",col("total_visits_per_month")
        .cast(v.actual.schema("total_visits_per_month").dataType))
        .orderBy("provider_id","month","total_visits_per_month")
    }
    //reorder columns to mirror expected results
    stage2.select(v.actual.columns.map(columnName => col(columnName)): _*)
  }

  case class Provider(incoming:DataFrame,actual:DataFrame)
  case class Visits(incoming:DataFrame,actual:DataFrame,containsYear:Boolean = false)

  case class ErrorRecords(incoming:DataFrame,actual:DataFrame)
}
