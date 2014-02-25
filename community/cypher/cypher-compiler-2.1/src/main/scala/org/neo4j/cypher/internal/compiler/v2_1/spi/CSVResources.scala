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
package org.neo4j.cypher.internal.compiler.v2_1.spi

import java.net.URL
import java.io._
import au.com.bytecode.opencsv.CSVReader
import org.neo4j.cypher.internal.compiler.v2_1.TaskCloser
import org.neo4j.cypher.LoadExternalResourceException
import org.neo4j.cypher.internal.compiler.v2_1.pipes.ExternalResource

class CSVResources(cleaner: TaskCloser) extends ExternalResource {

  override def getCsvIterator(url: URL): Iterator[Array[String]] = {
    val inputStream = ToStream(url).stream
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    val csvReader = new CSVReader(reader)

    cleaner.addTask(_ => {
      csvReader.close()
    })

    new Iterator[Array[String]] {
      var nextRow: Array[String] = csvReader.readNext()

      def hasNext: Boolean = nextRow != null

      def next(): Array[String] = {
        if (!hasNext) Iterator.empty.next()
        val row = nextRow
        nextRow = csvReader.readNext()
        row
      }
    }
  }
}

object ToStream {
  def apply(url: URL, separator: Char = File.separatorChar) = {
    val isWindows = System.getProperty("os.name").startsWith("Windows")
    if (isWindows) WindowsToStream(url, separator) else UnixToStream(url, separator)
  }

  final case class UnixToStream(url: URL, separator: Char) extends ToStream(url, separator) {
    val path = url.getPath
  }

  final case class WindowsToStream(url: URL, separator: Char) extends ToStream(url, separator) {
    val path = {
      val urlPath = url.getPath
      // This is to handle Windows file paths correctly.
      if (urlPath.startsWith("/") && urlPath.contains(":/") && host == "" && separator == '\\')
        urlPath.drop(1)
      else
        urlPath
    }
  }
}

sealed abstract class ToStream(url: URL, separator: Char) {
  val protocol = url.getProtocol
  val host = url.getHost
  val path: String

  val isFile = "file" == protocol

  def file =
    if (isFile) {
      if (host.nonEmpty) new File(host, path) else new File(path)
    } else
      throw new IllegalStateException(s"URL is not a file: $url")

  def stream: InputStream = try {
    if (isFile)
      new FileInputStream(file) else url.openStream
  } catch {
    case e: IOException =>
      throw new LoadExternalResourceException(s"Couldn't load the external resource at: $url", e)
  }
}

