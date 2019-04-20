package cs6240.rsjoin.youtube

import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Hello world!
 *
 */
object GroupFinder extends App {

  override def main(args: Array[String]) {
    val logger: org.apache.log4j.Logger = LogManager.getRootLogger
    if (args.length != 3) {
      logger.error("Usage:\nwc.FollowersCountMain <user> <group dir> <output dir>")
      System.exit(1)
    }
    val conf = new SparkConf().setAppName("Followers Count")
    val sc = new SparkContext(conf)

    val userTextFile = sc.textFile(args(0))
    val friends = userTextFile.map(line => line.split(",")) // split line by comma
      .map(record => (record(0), ("friend-"+record(1)))) // emit 1 for userid being followed
    val reverseFriends = userTextFile.map(line => line.split(",")) // split line by comma
      .map(record => (record(1), ("friend-"+record(0)))) // emit 1 for userid being followed
    val friendList = friends.union(reverseFriends)


    //friendList.foreach(println)

    val groupTextFile = sc.textFile(args(1))
    val userGroupList = groupTextFile.map(line => line.split(","))
      .map(record => (record(0), ("group-"+record(1)))).cache()


    //userGroupList.foreach(println)

    val data = friendList.union(userGroupList)
      .groupByKey()
      .mapValues(_.toList)

    //data.foreach(println)

    val suggestedList = data.flatMap{case(user, list) => getSuggestionsList(user,list)}
    //val suggestedList = sc.parallelize(suggestedListMap.toSeq)

    val intermediate = suggestedList.union(userGroupList).groupByKey().mapValues(_.toList.distinct)

    //intermediate.foreach(println)

    val finalResult = intermediate.flatMap{case (user, list) => removeAlreadyExistingGroups(user,list)}
    finalResult.foreach(println)
  }

  def getSuggestionsList(user:String, list:List[String]): List[(String, String)] = {

    var result: List[(String, String)] = List()
    var groups : List[String] = List()
    var friends : List[String] = List()

    for(item <- list)
    {
      val tokens = item.split("-")

      if (tokens(0).equals("friend"))
      {
        friends = friends :+ tokens(1)
      } else if (tokens(0).equals("group"))
      {
        groups = groups:+ tokens(1)
      }
    }

    for (friend <- friends){
      for (group <- groups){
        result = result :+ (friend,"suggestion-"+group)
      }
    }

    return result.distinct
  }

  def removeAlreadyExistingGroups (user:String, list:List[String]): List[(String, String)] = {

    var result: List[(String, String)] = List()
    var existingGroups : List[String] = List()
    var suggestionGroups : List[String] = List()
    var finalSuggestions : List[String] = List()

    for(item <- list)
    {
      val tokens = item.split("-")

      if (tokens(0).equals("group"))
      {
        existingGroups = existingGroups :+ tokens(1)
      } else if (tokens(0).equals("suggestion"))
      {
        suggestionGroups = suggestionGroups:+ tokens(1)
      }
    }

    for (group <- suggestionGroups.distinct){

      if (!existingGroups.contains(group) && !finalSuggestions.contains(group)){
        finalSuggestions = finalSuggestions :+ group
      }
    }

    for (group <- finalSuggestions.distinct){
      result = result :+ (user,group)
    }

    return result

  }

}
