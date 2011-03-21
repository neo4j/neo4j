package org.neo4j.qa

import cuke4duke.{EN, ScalaDsl}
import dispatch._
import org.scalatest.matchers.ShouldMatchers
import org.apache.http.conn.HttpHostConnectException
import org.neo4j.qa.util.{Neo4jHelper, Platform, ArchiveHelper}
import java.io.{FilenameFilter, FileOutputStream, File}
import java.lang.String

/**
 * Operations steps for working with Neo4j server.
 *
 */
class Neo4jOpsSteps(neo4j: Neo4jEnvironment) extends ScalaDsl with EN with ShouldMatchers
{

  Given("""^a platform supported by Neo4j$""")
  {
    () =>
      neo4j.hostPlatform should not be (Platform.Unknown)
  }

  Given("""^Neo4j version based on system property "([^"]*)"$""")
  {
    (propertyName: String) =>
      val propertyValue = System.getProperty(propertyName)
      propertyValue should not (be (null))
      neo4j.version = propertyValue
  }

  Given("""^Neo4j Home based on system property \"([^\"]*)\"$""")
  {
    (propertyName: String) =>
    val propertyValue = System.getProperty(propertyName)
    propertyValue should not (be (null))
    neo4j.home = new File(propertyValue)
  }

  Given("""^Neo4j version "([^"]*)"$""")
  {
    (version: String) =>
      neo4j.version = version;

  }

  Given("""^a web site at host "([^"]*)"$""")
  {
    (hostAddress: String) =>
      val http = new Http
      val req = :/(hostAddress)
      http x (req as_str)
      {
        case _ => "website is reachable"
      }
      neo4j.downloadHost = hostAddress
  }

  Given("""^that Neo4j Server is not running$""")
  {
    () =>
      val http = new Http
      val req = :/("localhost", 7474)
      print(req as_str)
      var thrownException:Option[Throwable] = None
      try
      {
        http x (req as_str)
        {
          case _ => "neo4j is running"
        }
        None
      } catch {
        case e => thrownException = Some(e)
      }
      if (thrownException == None) fail ("Neo4j server is running!")

  }

  When("""^I download Neo4j.*$""")
  {
    () =>
      neo4j.archiveName match
      {
        case Some(validArchive) =>
        {
          if ( !(new File(validArchive).exists) )
          {
            val http = new Http
            val req = :/(neo4j.downloadHost) / validArchive
            http(req >>> new FileOutputStream(validArchive))
          }
        }
        case None => fail("Could not determine archive to download.")
      }

  }

  When("""^I unpack the archive into Neo4j Home$""")
  {
    () =>
      val destination = neo4j.home
      neo4j.archiveName match
      {
        case Some(validArchive) => ArchiveHelper.unarchive(validArchive, destination.getAbsolutePath)
        case None => fail("Could not determine archive to download.")
      }
  }

  When("""I start the server""")
  {
    () =>
    neo4j.hostPlatform match
    {
      case Platform.Windows => fail("not yet implemented") /* launch windows */
      case Platform.Unix => Runtime.getRuntime().exec(neo4j.home.getAbsolutePath + "/bin/neo4j start")
    }
  }

  Then("""^the current directory should contain a Neo4j archive$""")
  {
    () =>
    neo4j.archiveName match
    {
      case Some(expectedArchive) =>
      {
        new File(expectedArchive) should be a ('file)
      }
      case None => fail("Could not determine what archive to expect.")
    }
  }

  Then("""^Neo4j Home should contain a Neo4j Server installation$""")
  {
    () =>
    val isInstalled = Neo4jHelper.neo4jIsInstalledIn(neo4j.home)
    isInstalled should be (true)
  }

  Then("""^the Neo4j version of the installation should be correct$""")
  {
    Neo4jHelper.badlyVersionedJarsIn(neo4j.libDir, neo4j.version) should have size(0)
    Neo4jHelper.badlyVersionedJarsIn(neo4j.systemLibDir, neo4j.version) should have size(0)
  }
}



