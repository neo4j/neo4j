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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.ir.CSVFormat
import org.neo4j.cypher.internal.ir.HasHeaders
import org.neo4j.cypher.internal.ir.NoHeaders
import org.neo4j.cypher.internal.runtime.ArrayBackedMap
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ResourceLinenumber
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.LoadExternalResourceException
import org.neo4j.memory.HeapEstimator
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

import java.net.URI

import scala.jdk.CollectionConverters.MapHasAsJava

case class LoadCSVPipe(
  source: Pipe,
  format: CSVFormat,
  urlExpression: Expression,
  variable: String,
  fieldTerminator: Option[String],
  legacyCsvQuoteEscaping: Boolean,
  bufferSize: Int
)(val id: Id = Id.INVALID_ID)
    extends AbstractLoadCSVPipe(source, format, urlExpression, fieldTerminator, legacyCsvQuoteEscaping, bufferSize) {

  final override def writeRow(
    filename: String,
    linenumber: Long,
    last: Boolean,
    argumentRow: CypherRow,
    value: AnyValue
  ): CypherRow = {
    val newRow = rowFactory.copyWith(argumentRow, variable, value)
    newRow.setLinenumber(Some(ResourceLinenumber(
      filename,
      linenumber,
      last
    ))) // Always overwrite linenumber if we have nested LoadCsvs
    newRow
  }
}

abstract class AbstractLoadCSVPipe(
  source: Pipe,
  format: CSVFormat,
  urlExpression: Expression,
  fieldTerminator: Option[String],
  legacyCsvQuoteEscaping: Boolean,
  bufferSize: Int
) extends PipeWithSource(source) {

  protected def writeRow(
    filename: String,
    linenumber: Long,
    last: Boolean,
    argumentRow: CypherRow,
    value: AnyValue
  ): CypherRow

  // Uses an ArrayBackedMap to store header-to-values mapping
  private class IteratorWithHeaders(
    headers: Seq[Value],
    argumentRow: CypherRow,
    filename: String,
    inner: LoadCsvIterator
  ) extends ClosingIterator[CypherRow] {

    private val internalMap = new ArrayBackedMap[String, AnyValue](
      headers.map(a => if (a eq Values.NO_VALUE) null else a.asInstanceOf[TextValue].stringValue()).zipWithIndex.toMap,
      nullValue = Values.NO_VALUE
    )
    private val internalMapSize = ArrayBackedMap.SHALLOW_SIZE + HeapEstimator.shallowSizeOfObjectArray(headers.size)
    private var newRow: CypherRow = _
    private var needsUpdate = true

    override protected[this] def closeMore(): Unit = inner.close()

    override def innerHasNext: Boolean = {
      if (needsUpdate) {
        newRow = computeNextRow()
        needsUpdate = false
      }
      newRow != null
    }

    override def next(): CypherRow = {
      if (!hasNext) Iterator.empty.next()
      needsUpdate = true
      newRow
    }

    private def computeNextRow() = {
      if (inner.hasNext) {
        val row = inner.next()
        internalMap.putValues(row.asInstanceOf[Array[AnyValue]])
        // we need to make a copy here since someone may hold on this
        // reference, e.g. EagerPipe
        val internalMapCopy = internalMap.copy.asJava
        // NOTE: The header key values will be the same for every row, so we do not include it in the memory tracking payload size
        //       since it will result in overestimation of heap usage
        var payloadSize = 0L
        var i = 0
        while (i < row.length) {
          payloadSize += row(i).estimatedHeapUsage()
          i += 1
        }
        val mapValue = VirtualValues.fromMap(internalMapCopy, internalMapSize, payloadSize)
        writeRow(filename, inner.lastProcessed, inner.readAll, argumentRow, mapValue)
      } else null
    }
  }

  private class IteratorWithoutHeaders(argumentRow: CypherRow, filename: String, inner: LoadCsvIterator)
      extends ClosingIterator[CypherRow] {

    override protected[this] def closeMore(): Unit = inner.close()

    override def innerHasNext: Boolean = inner.hasNext

    override def next(): CypherRow = {
      // Make sure to pull on inner.next before calling inner.lastProcessed to get the right line number
      val value = VirtualValues.list(inner.next().asInstanceOf[Array[AnyValue]]: _*)
      writeRow(filename, inner.lastProcessed, inner.readAll, argumentRow, value)
    }
  }

  private def getLoadCSVIterator(state: QueryState, urlString: String, useHeaders: Boolean): LoadCsvIterator = {

    state.resources.getCsvIterator(
      urlString,
      state.query,
      fieldTerminator,
      legacyCsvQuoteEscaping,
      bufferSize,
      useHeaders
    )
  }

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    input.flatMap(row => {
      val urlString: String = urlExpression(row, state).asInstanceOf[TextValue].stringValue()
      val uri = getImportURI(urlString)
      val fileName = fileNameFromUri(uri)

      format match {
        case HasHeaders =>
          val iterator = getLoadCSVIterator(state, urlString, useHeaders = true)
          val headers =
            if (iterator.nonEmpty) iterator.next().toIndexedSeq else IndexedSeq.empty // First row is headers
          new IteratorWithHeaders(replaceNoValues(headers), row, fileName, iterator)
        case NoHeaders =>
          new IteratorWithoutHeaders(
            row,
            fileName,
            getLoadCSVIterator(state, urlString, useHeaders = false)
          )
      }
    })
  }

  private def getImportURI(urlString: String): URI = {
    // even though we're working with URIs - report as URL errors for compat with original error messages
    val uri: URI =
      try {
        new URI(urlString)
      } catch {
        case e: java.net.URISyntaxException =>
          if (e.getMessage.startsWith("Expected scheme name")) {
            // this captures the previous behaviour when creating a URL
            throw new LoadExternalResourceException(s"Invalid URL '$urlString': no protocol: $urlString", e)
          } else {
            throw new LoadExternalResourceException(s"Invalid URL '$urlString': ${e.getMessage}", e)
          }
      }

    // this also captures the previous behaviour when creating a URL
    if (!uri.isAbsolute || uri.getScheme == null) {
      throw new LoadExternalResourceException(s"Invalid URL '$urlString': no protocol: $urlString")
    }

    uri
  }

  private def fileNameFromUri(uri: URI): String = {
    if (uri.getScheme == "file") {
      uri.getSchemeSpecificPart
    } else {
      val path = uri.getPath
      val ix = if (path == null) -1 else path.lastIndexOf("/")
      if (ix == -1) "N/A" else path.substring(ix + 1)
    }
  }

  private def replaceNoValues(headers: IndexedSeq[Value]): IndexedSeq[Value] = headers.map {
    case noValue if noValue eq Values.NO_VALUE => Values.stringValue("")
    case other                                 => other
  }
}
