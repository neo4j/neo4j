package org.neo4j.qa.util

import java.io.{FilenameFilter, File}

object Neo4jHelper
{
  def neo4jIsInstalledIn(directory:File) =
  {
    if (directory.exists)
    {
      true
    }
    else false
  }

  def badlyVersionedJarsIn(dir:File, version:String) =
  {
    val badlyVersionedJars = for (lib <- dir.listFiles(new FilenameFilter() {
      def accept(dir: File, name: String) = (name.startsWith("neo4j") && name.endsWith(".jar"))
    }) if !lib.getName.contains(version)) yield lib
    badlyVersionedJars
  }
}