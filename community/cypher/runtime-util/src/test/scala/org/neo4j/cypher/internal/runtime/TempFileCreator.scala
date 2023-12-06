/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime

import org.neo4j.io.fs.FileUtils

import java.io.File
import java.io.FileOutputStream
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.nio.file.Files
import java.nio.file.Path
import java.util.zip.GZIPOutputStream
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream

import scala.io.Codec
import scala.language.implicitConversions

trait TempFileCreator {

  private var paths: Seq[Path] = Seq.empty

  protected def deleteTemporaryFiles(): Unit = {
    paths.filter(Files.exists(_)).foreach { p =>
      if (Files.isRegularFile(p)) p.toFile.delete() else FileUtils.deleteDirectory(p)
    }
  }

  def createCSVTempFileURL(f: PrintWriter => Unit)(implicit writer: File => PrintWriter = normalWriter): String =
    createCSVTempFileURL("cypher", null)(f)

  def createCSVTempFileURL(filename: String)(f: PrintWriter => Unit)(implicit writer: File => PrintWriter): String =
    createCSVTempFileURL(filename, null)(f)

  def createCSVTempFileURL(filename: String, dir: String)(f: PrintWriter => Unit)(implicit
  writer: File => PrintWriter): String =
    createTempFileURL(filename, ".csv")(f)

  def createGzipCSVTempFileURL(f: PrintWriter => Unit): String =
    createGzipCSVTempFileURL()(f)

  def createGzipCSVTempFileURL(filename: String = "cypher", dir: String = null)(f: PrintWriter => Unit): String =
    createTempFileURL(filename, ".csv.gz")(f)(gzipWriter)

  def createZipCSVTempFileURL(f: PrintWriter => Unit): String =
    createZipCSVTempFileURL()(f)

  def createZipCSVTempFileURL(filename: String = "cypher", dir: String = null)(f: PrintWriter => Unit): String =
    createTempFileURL(filename, ".csv.zip")(f)(zipWriter)

  def createTempFile(name: String, ext: String, f: PrintWriter => Unit)(implicit writer: File => PrintWriter): String =
    synchronized {
      withTempFileWriter(name, ext)(f).getAbsolutePath
    }

  def createTempDirectory(name: String): Path = synchronized {
    val dir = java.nio.file.Files.createTempDirectory(name)
    paths = paths :+ dir
    dir
  }

  def createTempFileURL(name: String, ext: String)(f: PrintWriter => Unit)(implicit
  writer: File => PrintWriter): String = synchronized {
    withTempFileWriter(name, ext)(f).toURI.toURL.toString
  }

  private def withTempFileWriter(name: String, ext: String)(f: PrintWriter => Unit)(implicit
  writer: File => PrintWriter) = {
    val path = Files.createTempFile(name, ext)
    val file = path.toFile
    try {
      fileWrite(file)(f)
    } finally {
      paths = paths :+ path
    }
    file
  }

  def pathWrite(path: Path)(f: PrintWriter => Unit): Path = {
    Files.createDirectories(path.getParent)
    fileWrite(path.toFile)(f)
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

  implicit def normalWriter(file: File): PrintWriter = new PrintWriter(Files.newBufferedWriter(file.toPath))

  def gzipWriter(file: File): PrintWriter = new PrintWriter(new OutputStreamWriter(
    new GZIPOutputStream(
      new FileOutputStream(file, false)
    ),
    Codec.UTF8.charSet
  ))

  def zipWriter(file: File): PrintWriter = {
    val zos = new ZipOutputStream(new FileOutputStream(file))
    val ze = new ZipEntry(file.getName)
    zos.putNextEntry(ze)

    new PrintWriter(new OutputStreamWriter(zos))
  }

}
