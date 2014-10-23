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
package org.neo4j.cypher.internal.compiler.v2_2.spi

import java.io._
import java.net.{CookieHandler, CookieManager, CookiePolicy, URL}

import au.com.bytecode.opencsv.CSVReader
import org.neo4j.csv.reader._
import org.neo4j.cypher.internal.compiler.v2_2.{LoadExternalResourceException, TaskCloser}
import org.neo4j.cypher.internal.compiler.v2_2.pipes.ExternalResource

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object CSVResources {
  val DEFAULT_FIELD_TERMINATOR: Char = ','
}

class CSVResources(cleaner: TaskCloser) extends ExternalResource {

  def getCsvIterator(url: URL, fieldTerminator: Option[String] = None): Iterator[Array[String]] = {
    val inputStream = openStream(url)
    val reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"))
    val csvReader = new CSVReader(reader, fieldTerminator.map(_.charAt(0)).getOrElse(CSVResources.DEFAULT_FIELD_TERMINATOR))
    val terminator: Char = fieldTerminator.map(_.charAt(0)).getOrElse(CSVResources.DEFAULT_FIELD_TERMINATOR)
    val seeker = QuoteAwareCharSeeker.quoteAware(CharSeekers.charSeeker(reader, true), '"')
    val extractors = new Extractors(terminator)

    cleaner.addTask(_ => {
      seeker.close()
    })

    new Iterator[Array[String]] {
      private def readNextRow: Array[String] = {
        val mark = new Mark
        val buffer = new ArrayBuffer[String]
        breakable {
          while (seeker.seek( mark, Array(terminator.toInt))) {
          buffer += seeker.extract(mark, extractors.string()).value()

          if (mark.isEndOfLine) break
        }}

        if (buffer.isEmpty) {
          null
        } else {
          buffer.toArray
        }
      }

      var nextRow: Array[String] = readNextRow

      def hasNext: Boolean = nextRow != null

      def next(): Array[String] = {
        if (!hasNext) Iterator.empty.next()
        val row = nextRow
        nextRow = readNextRow
        row
      }
    }
  }

  private def openStream(url: URL, connectionTimeout: Int = 2000, readTimeout: Int = 10 * 60 * 1000): InputStream = {
    try {
      if (url.getProtocol.startsWith("http"))
        TheCookieManager.ensureEnabled()
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

object TheCookieManager {
  private lazy val theCookieManager = create

  def ensureEnabled() {
    // Force lazy val to be evaluated
    theCookieManager != null
  }

  private def create = {
    val cookieManager = new CookieManager
    CookieHandler.setDefault(cookieManager)
    cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ALL)
    cookieManager
  }
}

