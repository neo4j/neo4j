/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.test_helpers

import java.io.{FileOutputStream, OutputStreamWriter, PrintWriter}
import java.nio.file.{Files, Path}
import java.util.zip.{GZIPOutputStream, ZipEntry, ZipOutputStream}

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.io.fs.FileUtils

import scala.io.Codec
import scala.reflect.io.File

trait CreateTempFileTestSupport extends CypherTestSupport {
  self: CypherFunSuite =>

  private var paths: Seq[Path] = Seq.empty

  override protected def stopTest(): Unit = {
    try {
      paths.filter(Files.exists(_)).foreach { p =>
        if (Files.isRegularFile(p)) p.toFile.delete() else FileUtils.deletePathRecursively(p)
      }
    } finally {
      super.stopTest()
    }
  }

  def createCSVTempFileURL(f: PrintWriter => Unit)(implicit writer: File => PrintWriter = normalWriter): String =
    createCSVTempFileURL("cypher", null)(f)

  def createCSVTempFileURL(filename: String)(f: PrintWriter => Unit)(implicit writer: File => PrintWriter): String = createCSVTempFileURL(filename, null)(f)

  def createCSVTempFileURL(filename: String, dir: String)(f: PrintWriter => Unit)(implicit writer: File => PrintWriter): String =
    createTempFileURL(filename, ".csv")(f)

  def createGzipCSVTempFileURL(f: PrintWriter => Unit)(implicit writer: File => PrintWriter): String =
    createGzipCSVTempFileURL()(f)

  def createGzipCSVTempFileURL(filename: String = "cypher", dir: String = null)(f: PrintWriter => Unit)(implicit writer: (File => PrintWriter)): String =
    createTempFileURL(filename, ".csv.gz")(f)(gzipWriter)

  def createZipCSVTempFileURL(f: PrintWriter => Unit)(implicit writer: File => PrintWriter): String =
    createZipCSVTempFileURL()(f)

  def createZipCSVTempFileURL(filename: String = "cypher", dir: String = null)(f: PrintWriter => Unit)(implicit writer: (File => PrintWriter)): String =
    createTempFileURL(filename, ".csv.zip")(f)(zipWriter)

  def createTempFile(name: String, ext: String, f: PrintWriter => Unit)(implicit writer: (File => PrintWriter)): String = synchronized {
    withTempFileWriter(name, ext)(f).toAbsolute.path
  }

  def createTempDirectory(name: String): Path = synchronized {
    val dir = java.nio.file.Files.createTempDirectory(name)
    paths = paths :+ dir
    dir
  }

  def createTempFileURL(name: String, ext: String)(f: PrintWriter => Unit)(implicit writer: File => PrintWriter): String = synchronized {
    withTempFileWriter(name, ext)(f).toURI.toURL.toString
  }

  private def withTempFileWriter(name: String, ext: String)(f: PrintWriter => Unit)(implicit writer: File => PrintWriter) = {
    val path = Files.createTempFile(name, ext)
    val file = new File(path.toFile)
    try {
      fileWrite(file)(f)
    } finally {
      paths = paths :+ path
    }
    file
  }

  def pathWrite(path: Path)(f: PrintWriter => Unit): Path = {
    Files.createDirectories(path.getParent)
    fileWrite(new File(path.toFile))(f)
    path
  }

  def fileWrite(file: File)(f: PrintWriter => Unit)(implicit writerFactory: File => PrintWriter): File = {
    val writer = writerFactory(file)
    try {
      f(writer)
      file
    } finally {
      writer.close()
    }
  }

  implicit def normalWriter(file: File): PrintWriter = new PrintWriter(file.bufferedWriter(append = false, Codec.UTF8))

  def gzipWriter(file: File): PrintWriter = new PrintWriter(new OutputStreamWriter(
    new GZIPOutputStream(
      new FileOutputStream(file.jfile, false)
    ), Codec.UTF8.charSet))

  def zipWriter(file: File): PrintWriter = {
    val zos = new ZipOutputStream(new FileOutputStream(file.jfile))
    val ze = new ZipEntry(file.name)
    zos.putNextEntry(ze)

    new PrintWriter(new OutputStreamWriter(zos))
  }

}
