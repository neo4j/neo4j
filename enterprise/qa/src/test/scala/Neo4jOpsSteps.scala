import cuke4duke.{EN, ScalaDsl}
import dispatch._
import java.io.{FileOutputStream, File}
import neo4j.qa.util.ArchiveHelper
import org.scalatest.matchers.ShouldMatchers

/**
 * Operations steps for working with Neo4j server.
 *
 */
class Neo4jOpsSteps(neo4j: Neo4jEnvironment) extends ScalaDsl with EN with ShouldMatchers
{

  val WindowsPlatformRE = """.*([Ww]in).*""".r
  val MacPlatformRE = """.*([mM]ac).*""".r
  val UnixPlatformRE = """.*(n[iu]x).*""".r

  Given("""^a platform supported by Neo4j$""")
  {
    neo4j.hostPlatform =
        {
          System.getProperty("os.name") match
          {
            case WindowsPlatformRE(_) => Platform.Windows
            case MacPlatformRE(_) => Platform.Unix
            case UnixPlatformRE(_) => Platform.Unix
            case unsupportedPlatform => fail("Unsupported platform " + unsupportedPlatform)
          }
        }
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

  Then("""^the current directory should contain a Neo4j archive$""")
  {
    neo4j.archiveName match {
      case Some(expectedArchive) => {
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

class Neo4jEnvironment
{
  val UNSPECIFIED = "unspecified"

  var version = UNSPECIFIED
  var downloadHost = UNSPECIFIED
  var hostPlatform = Platform.Unknown

  def archiveName =
  {
    hostPlatform match
    {
      case Platform.Windows => Some("neo4j-" + version + "-windows.zip")
      case Platform.Unix => Some("neo4j-" + version + "-unix.tar.gz")
      case unsupportedPlatform => None
    }
  }


  override def toString: String =
  {
    return "{" +
        "version: " + version +
        ", " +
        "downloadHost: " + downloadHost +
        ", " +
        "hostPlatform: " + hostPlatform +
        ", " +
        "archiveName: " + archiveName +
        "}"
  }
}
