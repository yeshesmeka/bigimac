package com.yeshesmeka.com.plays

import scala.io.Source

import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession, Column}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.yeshesmeka.plays.utils.ArtistBandParser

object ArtistBandPlayDataProcessor {

  // Method to convert epoch timestamp to date string in "YYYY-mm-dd" format
  def formatEpochToHumanDate(epochTime: Long): String = {

	val df = new SimpleDateFormat("yyyy-MM-dd")
	df.setTimeZone(TimeZone.getTimeZone("UTC"))
	val dt_conv = df.format(new Date(epochTime))

	dt_conv

  }

//  Method to parse the input data line into respective fields.
  def parseInput(line: String, std_ref:Map[String, Int]): ((String, String), Long) = {

	val fields = line.split("\t")
	val artistName = fields(0)
// Call to the Parser for Artist/Band name to find a match. The parser applies the Rules defined in the problem statement.
	val parsedTitle: String = ArtistBandParser.parse(artistName, std_ref).getOrElse("NO TITLE")
	val dt: String = formatEpochToHumanDate(fields(1).toLong * 1000)
	val noPlays: Long = fields(2).toLong

	((parsedTitle, dt), noPlays)

  }

//  Method to load and convert the Industry Standard reference file into a Map[String, Int] data structure.
//  This Map allows for faster lookups of Artist or Band name in the reference file.
  def loadStandardArtistBandFormatReference(filePath: String): Map[String, Int] = {

	var namesArtistOrBand: Map[String, Int] = Map()

	val lines = Source.fromFile("/Volumes/Data/learning_tech/musicians/src/input/standard_file.txt").getLines()
	for (line <- lines) {
	  namesArtistOrBand += (line -> 1)
	}

	namesArtistOrBand

  }

//  Method to create Row objects for processed records. Used in creating a Dataframe
  def row(line: List[String]): Row = Row(line(0), line(1), line(2).toInt)

// Main method for the application
  def main(args: Array[String]): Unit = {

	Logger.getLogger("org").setLevel(Level.ERROR)

/*	val inputDataPath: String = "/Volumes/Data/projects/PlayDataProcessor/src/input/input_data.txt"
	val referenceDataPath: String = "/Volumes/Data/projects/PlayDataProcessor/src/input/standard_file.txt"
	val outputPath: String = "/Volumes/Data/projects/PlayDataProcessor/src/output1/"
	val noInputRDDPartitions: Int = 4
*/
	val inputDataPath: String = args(0).toString()
	val referenceDataPath: String = args(1).toString()
	val outputPath: String = args(2).toString()
	val noInputRDDPartitions: Int = args(3).toInt

	val spark: SparkSession = SparkSession.builder().appName("PlayDataProcessor").getOrCreate()

	val sc: SparkContext = spark.sparkContext

//	RDD containing the Raw input data
	val inputRDD: RDD[String] = sc.textFile(inputDataPath, noInputRDDPartitions)

//	Broadcast variable for the Industry Standard reference data. This variables makes the Map available for all the executors
	val std_ref: Broadcast[Map[String, Int]] = sc.broadcast(loadStandardArtistBandFormatReference(referenceDataPath))

//	Pair RDD containing the parsed input data. The key is (Artist/Band name, Date) value is PlayCount
	val ArtistBandPlayCounterData: RDD[((String, String), Long)] = inputRDD.map(x => parseInput(x, std_ref.value))

//Pair RDD to aggregate the play count per artist/band per day. The Key is (artist/band name, date) and the value is total play count for a given day in the key
	val ArtistBandPlaysKVRDD: RDD[((String, String), Long)] = ArtistBandPlayCounterData.filter(x => x._1._1 != "NOT FOUND").reduceByKey((x, y) => x + y)

//	String RDD to transform the Pair RDD above into tab separated records
	val ABData: RDD[String] = ArtistBandPlaysKVRDD.map(x => x._1._1 + "\t" + x._1._2 + "\t" + x._2)

//	Schema for the resultant dataset. This is used in creating the dataframe from the String RDD above
	val playSchema = new StructType()
	  .add(StructField("Title", StringType, true))
	  .add(StructField("DateStamp", StringType, true))
	  .add(StructField("PlayCount", IntegerType, true))

	val data = ABData.map(_.split("\t").to[List]).map(row)

//	Creating a DataFrame for the resultant dataset
	val dataF = spark.createDataFrame(data, playSchema)

	dataF.repartition(new Column("DateStamp"), new Column("Title"))
	  .write.partitionBy("DateStamp", "Title")
	  .parquet(outputPath)
  }

}
