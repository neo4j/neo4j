/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.commons

import scala.reflect.io.File
import java.io.PrintWriter

trait CreateTempFileTestSupport extends CypherTestSupport {
  self: CypherTestSuite =>

  private var files: Seq[File] = Seq.empty

  override protected def stopTest(): Unit = {
    try {
      files.foreach(_.delete())
    }
    finally
    {
      super.stopTest()
    }
  }

  def createTempFile(name: String, ext: String, f: PrintWriter => Unit): String = synchronized {
    val file = File.makeTemp(name, ext)
    val writer = file.printWriter()
    f(writer)
    writer.flush()
    writer.close()
    files = files :+ file
    file.toAbsolute.path
  }

  def createTempFileURL(name: String, ext: String, f: PrintWriter => Unit): String = synchronized {
    val file = File.makeTemp(name, ext)
    val writer = file.printWriter()
    f(writer)
    writer.flush()
    writer.close()
    files = files :+ file
    file.toURI.toURL.toString
  }
}
