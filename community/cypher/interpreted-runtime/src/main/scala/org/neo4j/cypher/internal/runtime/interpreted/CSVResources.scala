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
import org.neo4j.cypher.internal.notification.InsecureProtocol
import org.neo4j.cypher.internal.runtime.ResourceManager
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExternalCSVResource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LoadCsvIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.LoadExternalResourceException
import org.neo4j.graphdb.security.AuthorizationViolationException
import org.neo4j.graphdb.security.URLAccessValidationError
import org.neo4j.internal.kernel.api.DefaultCloseListenable
import org.neo4j.lang.AutoCloseablePlus
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

import java.net.CookieHandler
import java.net.CookieManager
import java.net.CookiePolicy
import java.net.URI
import java.net.URISyntaxException
import java.util.Locale

import scala.collection.mutable.ArrayBuffer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object CSVResources {
  val DEFAULT_FIELD_TERMINATOR: Char = ','
  val DEFAULT_BUFFER_SIZE: Int = 2 * 1024 * 1024
  val DEFAULT_QUOTE_CHAR: Char = '"'

  private def config(legacyCsvQuoteEscaping: Boolean, csvBufferSize: Int) =
    Configuration
      .newBuilder()
      .withQuotationCharacter(DEFAULT_QUOTE_CHAR)
      .withBufferSize(csvBufferSize)
      .withLegacyMultilineBehaviour()
      .withTrimStrings(false)
      .withEmptyQuotedStringsAsNull(true)
      .withLegacyStyleQuoting(legacyCsvQuoteEscaping)
      .build()
}

case class CSVResource(url: URI, resource: AutoCloseable)
    extends DefaultCloseListenable
    with AutoCloseablePlus {
  override def closeInternal(): Unit = resource.close()

  // This is not correct, but hopefully the defensive answer. We don't expect this to be called,
  // but splitting isClosed and setCloseListener into different interfaces leads to
  // multiple inheritance problems instead.
  override def isClosed = false
}

class CSVResources(resourceManager: ResourceManager) extends ExternalCSVResource {
  private[this] var accumulatedFileLinesRead: Long = 0L
  private[this] val activeIterators: java.util.ArrayList[LoadCsvIterator] = new java.util.ArrayList()

  override def getFileLinesRead: Long = {
    activeIterators.synchronized {
      var fileLinesRead = accumulatedFileLinesRead
      var i = 0
      while (i < activeIterators.size) {
        fileLinesRead += activeIterators.get(i).lastProcessed
        i += 1
      }
      fileLinesRead
    }
  }

  override def getCsvIterator(
    urlString: String,
    state: QueryState,
    fieldTerminator: Option[String],
    legacyCsvQuoteEscaping: Boolean,
    bufferSize: Int,
    headers: Boolean = false
  ): LoadCsvIterator = {
    val (uri, reader) = Try {
      val uri = new URI(urlString)
      checkForUnsecureProtocols(uri, state)
      (uri, state.query.getImportDataConnection(uri))
    } match {
      case Success(readable)                               => readable
      case Failure(error: AuthorizationViolationException) => throw error
      // even though we're working with URIs - report as URL errors for compat with original error messages
      case Failure(error: URISyntaxException) =>
        throw LoadExternalResourceException.invalidUrl(urlString, error)
      case Failure(error: URLAccessValidationError) =>
        if (error.getMessage.contains("unknown protocol:")) {
          // for backwards compatability with old error messages
          throw LoadExternalResourceException.withInnerErrorMessage(urlString, error)
        } else {
          throw LoadExternalResourceException.cannotLoadFromUrl(urlString, error)
        }
      case Failure(error) =>
        throw LoadExternalResourceException.cannotLoadFromUrl(urlString, error)
    }
    val delimiter: Char = fieldTerminator.map(_.charAt(0)).getOrElse(CSVResources.DEFAULT_FIELD_TERMINATOR)
    val seeker = CharSeekers.charSeeker(reader, CSVResources.config(legacyCsvQuoteEscaping, bufferSize), false)
    val extractor = new Extractors().textValue()
    val intDelimiter = delimiter.toInt
    val mark = new Mark

    val resource = CSVResource(uri, seeker)
    resourceManager.trace(resource)

    val iterator = new LoadCsvIterator {
      var lastProcessed = 0L
      var readAll = false

      override protected[this] def closeMore(): Unit = {
        activeIterators.synchronized {
          accumulatedFileLinesRead += lastProcessed // When we close the iterator update number of lines read
          activeIterators.remove(this)
        }
        resource.close()
      }

      private def readNextRow: Array[Value] = {
        val buffer = new ArrayBuffer[Value]

        try {
          while (seeker.seek(mark, intDelimiter)) {
            val value = seeker.tryExtract(mark, extractor)
            buffer += (if (!extractor.isEmpty(value)) value else Values.NO_VALUE)
            if (mark.isEndOfLine) return if (buffer.isEmpty) null else buffer.toArray
          }
        } catch {
          case e: BufferOverflowException =>
            throw CypherExecutionException.csvBufferSizeOverflow(e);
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
    activeIterators.synchronized {
      activeIterators.add(iterator)
    }
    iterator
  }

  private def checkForUnsecureProtocols(uri: URI, state: QueryState): Unit = {
    val scheme = uri.getScheme.toLowerCase(Locale.ROOT)
    scheme match {
      case "http" | "ftp" =>
        state.newRuntimeNotification(InsecureProtocol)
      case _ =>
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
