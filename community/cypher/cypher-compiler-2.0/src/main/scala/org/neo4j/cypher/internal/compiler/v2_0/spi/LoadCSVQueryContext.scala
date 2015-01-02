/**
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
package org.neo4j.cypher.internal.compiler.v2_0.spi

import java.net.URL
import org.neo4j.cypher.internal.compiler.v2_0.CleanUpper
import java.io._
import au.com.bytecode.opencsv.CSVReader

class LoadCSVQueryContext(inner: QueryContext, cleaner: CleanUpper = new CleanUpper) extends DelegatingQueryContext(inner) {

  override def close(success: Boolean) {
    try {
      super.close(success)
    } finally {
      cleaner.cleanUp()
    }
  }

  def getCsvIterator(url: URL): Iterator[Array[String]] = {
    val inputStream = ToStream(url).stream
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    val csvReader = new CSVReader(reader)

    cleaner.addCleanupTask(() => {
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

case class ToStream(url: URL, separator: Char = File.separatorChar) {

  def isFile: Boolean = "file" == url.getProtocol

  def file = {
    val host: String = url.getHost
    var path: String = url.getPath

    // This is to handle Windows file paths correctly.
    if (path.startsWith("/") && path.contains(":/") && host == "" && separator == '\\')
      path = path.drop(1)

    if (isFile)
      if(host.nonEmpty) new File(host, path) else new File(path)
    else
      throw new IllegalStateException("url is not a file: " + url)
  }

  def stream: InputStream = if (isFile) new FileInputStream(file) else url.openStream
}
