package com.yeshesmeka.com.plays.com.yeshesmeka.plays.utils

object ArtistBandParser {

// Method to capitalize every words in the processed artist/band name.
  def autoCapitalize(title: String): String = title.split("\\s").map(_.capitalize).mkString(" ")

// Method to add missing determiners like "The" to find a match in the reference dataset
  def addMissingDeterminers(title: String, std_ref: Map[String, Int]):Option[String] = {

	val determinersList: List[String] = List("The")

	var result: Option[String] = None

	for(determiner <- determinersList){

	  val determinerAddedTitle = s"$determiner $title"

	  if(foundInStandardFormat(determinerAddedTitle, std_ref)) {
		result = Some(determinerAddedTitle)
	  }
	  else {
		result = None
	  }
	}
	result
  }

// Method to convert conjuctions like "and" to symbollic form i.e., "&"
  def transformConjunctionsToSymbols(title: String): String = {

	val result = title.replaceAll("\\sand\\s", " & ")

	result

  }

// Method to remove any punctuations in the Artist/Band name
  def removePunctuations(title: String): String = title.replaceAll("[?.!:\"\']","")

//  Method to check if a title string has a match in the reference data
  def foundInStandardFormat(title:String, std_ref: Map[String, Int]): Boolean = std_ref.contains(title)

//  Method to remove commas (,) from the input string.
  def removeComma(title: String, reverseFlag: Boolean = false): String = {

	val result = title.split(",")
	var processedTitle = ""

	if(reverseFlag) {
	  processedTitle = result(1).trim() + " " + result(0).trim()
	} else {
	  processedTitle = result(0).trim() + " " + result(1).trim()
	}

	processedTitle

  }

// Method to process the artist/band name string according the rules defined in the problem.
// Please see README for details of rules to process the artist/band name.
  def parse(title:String, std_ref:Map[String, Int]):Option[String] = {

	var parsedTitle:Option[String] = Some("")

	val titleWithoutPunctuations: String = removePunctuations(title)

	if (foundInStandardFormat(titleWithoutPunctuations, std_ref)) {

	  parsedTitle = Some(titleWithoutPunctuations)

	} else if (titleWithoutPunctuations.contains(",")) {

	  val commaRemovedTitle = removeComma(titleWithoutPunctuations)

	  if(foundInStandardFormat(commaRemovedTitle, std_ref)){

		parsedTitle = Some(commaRemovedTitle)

	  } else {

		val commaRemovedWordsReversed = removeComma(titleWithoutPunctuations, true)

		if(foundInStandardFormat(commaRemovedWordsReversed, std_ref)){

		  parsedTitle = Some(commaRemovedWordsReversed)

		}

	  }

	} else if (titleWithoutPunctuations.contains(" and ")) {

	  val titleWithConjunctionsTransformed = transformConjunctionsToSymbols(titleWithoutPunctuations)

	  if(foundInStandardFormat(titleWithConjunctionsTransformed, std_ref)){

		parsedTitle = Some(titleWithConjunctionsTransformed)

	  }

	} else {

	  parsedTitle = addMissingDeterminers(titleWithoutPunctuations, std_ref)

	}

	val cleanParsedTitle = Option(autoCapitalize(parsedTitle.getOrElse("NOT FOUND")))

	cleanParsedTitle

  }

}
