package org.neo4j.qa

import scala.util.parsing.json.JSON
import cuke4duke.{EN, ScalaDsl}
import dispatch._
import Http._

import java.io.{FileOutputStream, File}
import org.scalatest.matchers.{MatchResult, Matcher, ShouldMatchers}
import org.neo4j.qa.util.{CustomMatchers, ArchiveHelper}
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._
import scala.util.parsing.json.JSON._

/**
 * Operations steps for working with Neo4j server.
 *
 */
class Neo4jServerExamplesSteps(neo4j: Neo4jEnvironment) extends ScalaDsl with EN with ShouldMatchers with CustomMatchers
{
  var directoryForFind: File = null

  Given("""^environment variable "([^"]*)" pointing to a Neo4j Server installation$""")
  {
    (envVar: String) =>
      val neo4jHomeValue = System.getenv(envVar)
      neo4jHomeValue should not be (null)
      neo4j.home = new File(neo4jHomeValue)
      neo4j.home should exist

      new File(neo4j.home, "bin" + File.separator + "neo4j") should exist

  }

  When("""^I look in the "([^"]*)" directory under NEO4J_HOME$""")
  {
    (dir: String) =>
      directoryForFind = new File(neo4j.home, dir)
      directoryForFind should exist
  }

  Then("""^it should have at least (\d+) (\w+) file that contains /(.+)/$""")
  {
    (fileCount: Int, extension: String, regex: String) =>

      val filesToSearch = FileUtils.listFiles(directoryForFind, Array(extension), true)

      filesToSearch.filter(f =>
      {
        FileUtils.readFileToString(f) contains regex
      }).size should be >= (fileCount)

  }

  Then("""^it should contain a \"([^\"]*)\" (\w+)$""")
  {
    (partialName: String, extension: String) =>
      val filesToSearch = FileUtils.listFiles(directoryForFind, Array(extension), false)

      filesToSearch.exists(f =>
      {
       f.getName contains partialName
      }) should be (true)
  }

  When("""^I install the \"([^\"]*)\" (\w+) from (.+) into \"([^\"]*)\"$""")
  {
    (partialFilename:String, extension:String, sourceDir:String, destDir:String) =>

      val filesToSearch = FileUtils.listFiles(new File(neo4j.home, sourceDir), Array(extension), true)
      val foundFile = filesToSearch.find(f =>
          f.getName contains partialFilename
          ).getOrElse(fail(partialFilename + " not found under " + sourceDir))
    FileUtils.copyFileToDirectory(foundFile, new File(neo4j.home, destDir))
  }
  
  When("""^I browse the REST API to the database extensions$""")
  {
      () =>
      val http = new Http
      val db = :/("localhost", 7474)
      val body = http(db /  "db" / "data/" as_str)
      val json:Option[Any] = JSON.parseFull(body)
      val map:Map[String,Any] = json.get.asInstanceOf[Map[String, Any]]
      val extensions:Map[String, Any] = map.get("extensions").get.asInstanceOf[Map[String, Any]]
      val get_all:Map[String, Any] = extensions.get("GetAll").get.asInstanceOf[Map[String, Any]]
      get_all should not be None
      get_all.get("get_all_nodes") should not be None
      
  }

}

