package org.neo4j.qa

import java.io.File
import org.neo4j.qa.util.Platform

class Neo4jEnvironment
{

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
