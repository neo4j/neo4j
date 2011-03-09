package org.neo4j.qa

import java.io.File
import org.neo4j.qa.util.Platform

class Neo4jEnvironment
{

  val UNSPECIFIED = "unspecified"

  var version = UNSPECIFIED
  var downloadHost = UNSPECIFIED
  var home:File = new File(UNSPECIFIED)

  def libDir = new File(home, "lib")
  def systemDir = new File(home, "system")
  def systemLibDir = new File(systemDir, "lib")

  def archiveName =
  {
    hostPlatform match
    {
      case Platform.Windows => Some("neo4j-" + version + "-windows.zip")
      case Platform.Unix => Some("neo4j-" + version + "-unix.tar.gz")
      case unsupportedPlatform => None
    }
  }

  def hostPlatform = Platform.current

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
