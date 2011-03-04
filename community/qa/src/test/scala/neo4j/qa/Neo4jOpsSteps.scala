package org.neo4j.qa

import cuke4duke.{EN, ScalaDsl}
import dispatch._
import java.io.{FileOutputStream, File}
import neo4j.qa.util.ArchiveHelper
import org.scalatest.matchers.ShouldMatchers
import org.apache.http.conn.HttpHostConnectException

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
      propertyValue should not be (null, "Expected system property \"" + propertyName + "\" is null.")
      neo4j.version = propertyValue
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

  When("""^I unpack the archive into "([^"]*)"$""")
  {
    (destination: String) =>
      neo4j.archiveName match
      {
        case Some(validArchive) => ArchiveHelper.unarchive(validArchive, destination)
        case None => fail("Could not determine archive to download.")
      }
  }

  When("""I start the server""")
  {
    () =>
    neo4j.hostPlatform match
    {
      case Platform.Windows => fail("not yet implemented") /* launch windows */
      case Platform.Unix => Runtime.getRuntime().exec("sh " + neo4j.neo4jHome.getAbsolutePath + "/bin/neo4j start")
    }
  }

  Then("""^the current directory should contain a Neo4j archive$""")
  {
    neo4j.archiveName match
    {
      case Some(expectedArchive) =>
      {
        new File(expectedArchive) should be a ('file)
      }
      case None => fail("Could not determine what archive to expect.")
    }
  }
}

object Platform extends Enumeration
{
  type Platform = Value
  val Windows = Value("windows")
  val Unix = Value("unix")
  val Unknown = Value("unknown")
}

