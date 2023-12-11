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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.csv.reader.BufferOverflowException
import org.neo4j.csv.reader.CharSeekers
import org.neo4j.csv.reader.Configuration
import org.neo4j.csv.reader.Extractors
import org.neo4j.csv.reader.Mark
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LoadCsvIterator
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.LoadExternalResourceException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.internal.kernel.api.AutoCloseablePlus
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

import java.net.CookieHandler
import java.net.CookieManager
import java.net.CookiePolicy
import java.net.MalformedURLException
import java.net.URL

import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object CSVResources {
  val DEFAULT_FIELD_TERMINATOR: Char = ','
  val DEFAULT_BUFFER_SIZE: Int = 2 * 1024 * 1024
  val DEFAULT_QUOTE_CHAR: Char = '"'

  private def config(legacyCsvQuoteEscaping: Boolean, csvBufferSize: Int) = Configuration.newBuilder()
    .withQuotationCharacter(DEFAULT_QUOTE_CHAR)
    .withBufferSize(csvBufferSize)
    .withMultilineFields(true)
    .withTrimStrings(false)
    .withEmptyQuotedStringsAsNull(true)
    .withLegacyStyleQuoting(legacyCsvQuoteEscaping)
    .build()
}

case class CSVResource(url: URL, resource: AutoCloseable) extends DefaultCloseListenable with AutoCloseablePlus {
  override def closeInternal(): Unit = resource.close()

  // This is not correct, but hopefully the defensive answer. We don't expect this to be called,
  // but splitting isClosed and setCloseListener into different interfaces leads to
  // multiple inheritance problems instead.
  override def isClosed = false
}

class CSVResources(resourceManager: ResourceManager) extends ExternalCSVResource {

  override def getCsvIterator(
    urlString: String,
    query: QueryContext,
    fieldTerminator: Option[String],
    legacyCsvQuoteEscaping: Boolean,
    bufferSize: Int,
    headers: Boolean = false
  ): LoadCsvIterator = {

    val (url, reader) = Try {
      val url = new URL(urlString)
      (url, query.getImportDataConnection(url))
    } match {
      case Success(readable)                               => readable
      case Failure(error: AuthorizationViolationException) => throw error
      case Failure(error: MalformedURLException) =>
        throw new LoadExternalResourceException(s"Invalid URL '$urlString': ${error.getMessage}", error)
      case Failure(error) =>
        throw new LoadExternalResourceException(s"Cannot load from URL '$urlString': ${error.getMessage}", error)
    }
    val delimiter: Char = fieldTerminator.map(_.charAt(0)).getOrElse(CSVResources.DEFAULT_FIELD_TERMINATOR)
    val seeker = CharSeekers.charSeeker(reader, CSVResources.config(legacyCsvQuoteEscaping, bufferSize), false)
    val extractor = new Extractors(delimiter).textValue()
    val intDelimiter = delimiter.toInt
    val mark = new Mark

    val resource = CSVResource(url, seeker)
    resourceManager.trace(resource)

    new LoadCsvIterator {
      var lastProcessed = 0L
      var readAll = false

      override protected[this] def closeMore(): Unit = resource.close()

      private def readNextRow: Array[Value] = {
        val buffer = new ArrayBuffer[Value]

        try {
          while (seeker.seek(mark, intDelimiter)) {
            val value = seeker.tryExtract(mark, extractor)
            buffer += (if (!extractor.isEmpty(value)) value else Values.NO_VALUE)
            if (mark.isEndOfLine) return if (buffer.isEmpty) null else buffer.toArray
          }
        } catch {
          case e: BufferOverflowException => throw new CypherExecutionException(
              """Tried to read a field larger than the current buffer size.
                | Make sure that the field doesn't have an unterminated quote,
                | if it doesn't you can try increasing the buffer size via `dbms.import.csv.buffer_size`.""".stripMargin,
              e
            )
        }

        if (buffer.isEmpty) {
          null
        } else {
          buffer.toArray
        }
      }

      var nextRow: Array[Value] = readNextRow

      override def innerHasNext: Boolean = nextRow != null

      override def next(): Array[Value] = {
        if (!hasNext) Iterator.empty.next()
        val row = nextRow
        nextRow = readNextRow
        lastProcessed += 1
        readAll = !hasNext
        row
      }
    }
  }
}

object TheCookieManager {
  private lazy val theCookieManager = create

  def ensureEnabled(): Unit = {
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
