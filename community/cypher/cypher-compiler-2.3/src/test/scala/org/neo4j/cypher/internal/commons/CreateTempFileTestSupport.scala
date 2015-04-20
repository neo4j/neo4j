/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.PrintWriter

import scala.io.Codec
import scala.reflect.io.File

trait CreateTempFileTestSupport extends CypherTestSupport {
  self: CypherFunSuite =>

  private var files: Seq[File] = Seq.empty

  override protected def stopTest(): Unit = {
    try {
      files.foreach(_.delete())
    } finally {
      super.stopTest()
    }
  }

  def createCSVTempFileURL(f: PrintWriter => Unit): String =
    createCSVTempFileURL()(f)

  def createCSVTempFileURL(filename: String = "cypher", dir: String = null)(f: PrintWriter => Unit): String =
    createTempFileURL(filename, ".csv")(f)

  def createTempFile(name: String, ext: String, f: PrintWriter => Unit): String = synchronized {
    withTempFileWriter(name, ext)(f).toAbsolute.path
  }

  def createTempFileURL(name: String, ext: String)(f: PrintWriter => Unit): String = synchronized {
    withTempFileWriter(name, ext)(f).toURI.toURL.toString
  }

  private def withTempFileWriter(name: String, ext: String)(f: PrintWriter => Unit): File =  {
    val file = new File(java.io.File.createTempFile(name, ext, null))
    try {
      val writer = new PrintWriter(file.bufferedWriter(append = false, Codec.UTF8), true)
      try {
        f(writer)
      } finally {
        writer.close()
      }
    } finally {
      files = files :+ file
    }
    file
  }
}
