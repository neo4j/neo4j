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
import org.neo4j.cypher.internal.compiler.v2_1.CleanUpper
import org.neo4j.cypher.LoadExternalResourceException

class LoadCSVQueryContext(inner: QueryContext, cleaner: CleanUpper = new CleanUpper) extends DelegatingQueryContext(inner) {

  override def close(success: Boolean) {
    try {
      super.close(success)
    } finally {
      cleaner.cleanUp()
    }
  }

  override def getCsvIterator(url: URL): Iterator[Array[String]] = {
    val inputStream = openStream(url)
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

  private def openStream(url: URL, connectionTimeout: Int = 2000, readTimeout: Int = 2000): InputStream = {
    try {
      val con = url.openConnection()
      con.setConnectTimeout(connectionTimeout)
      con.setReadTimeout(readTimeout)
      con.getInputStream()
    } catch {
      case e: IOException =>
        throw new LoadExternalResourceException(s"Couldn't load the external resource at: $url", e)
    }
  }
}
