package org.neo4j.qa

import java.io.File

class Neo4jEnvironment
{

  val WindowsPlatformRE = """.*([Ww]in).*""".r
  val MacPlatformRE = """.*([mM]ac).*""".r
  val UnixPlatformRE = """.*(n[iu]x).*""".r

  val UNSPECIFIED = "unspecified"

  var version = UNSPECIFIED
  var downloadHost = UNSPECIFIED
  var neo4jHome:File = null

  def archiveName =
  {
    hostPlatform match
    {
      case Platform.Windows => Some("neo4j-" + version + "-windows.zip")
      case Platform.Unix => Some("neo4j-" + version + "-unix.tar.gz")
      case unsupportedPlatform => None
    }
  }

  def hostPlatform =
  {
    System.getProperty("os.name") match
    {
      case WindowsPlatformRE(_) => Platform.Windows
      case MacPlatformRE(_) => Platform.Unix
      case UnixPlatformRE(_) => Platform.Unix
      case _ => Platform.Unknown
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
