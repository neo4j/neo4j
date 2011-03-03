package org.neo4j.qa

import cuke4duke.{EN, ScalaDsl}
import dispatch._
import java.io.{FileOutputStream, File}
import org.scalatest.matchers.{MatchResult, Matcher, ShouldMatchers}
import neo4j.qa.util.{CustomMatchers, ArchiveHelper}
import org.apache.commons.io.FileUtils
import scala.collection.JavaConversions._

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
      neo4j.neo4jHome = new File(neo4jHomeValue)
      neo4j.neo4jHome should exist

      new File(neo4j.neo4jHome, "bin" + File.separator + "neo4j") should exist

  }

  When("""^I look in the "([^"]*)" directory under NEO4J_HOME$""")
  {
    (dir: String) =>
      directoryForFind = new File(neo4j.neo4jHome, dir)
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

      val filesToSearch = FileUtils.listFiles(new File(neo4j.neo4jHome, sourceDir), Array(extension), true)
      val foundFile = filesToSearch.find(f =>
          f.getName contains partialFilename
          ).getOrElse(fail(partialFilename + " not found under " + sourceDir))
    FileUtils.copyFileToDirectory(foundFile, new File(neo4j.neo4jHome, destDir))
  }

}

