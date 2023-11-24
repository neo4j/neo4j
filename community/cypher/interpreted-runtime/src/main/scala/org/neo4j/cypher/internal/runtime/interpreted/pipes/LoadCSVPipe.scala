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
import org.neo4j.cypher.internal.runtime.QueryContext
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

import java.net.URL

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

  protected def getImportURL(urlString: String, context: QueryContext): URL = {
    val url: URL =
      try {
        new URL(urlString)
      } catch {
        case e: java.net.MalformedURLException =>
          throw new LoadExternalResourceException(s"Invalid URL '$urlString': ${e.getMessage}", e)
      }

    context.getImportURL(url) match {
      case Left(error) =>
        throw new LoadExternalResourceException(s"Cannot load from URL '$urlString': $error")
      case Right(urlToLoad) =>
        urlToLoad
    }
  }

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

  private def getLoadCSVIterator(state: QueryState, url: URL, useHeaders: Boolean): LoadCsvIterator = {
    state.resources.getCsvIterator(
      url,
      state.query.getConfig,
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
      val urlString: TextValue = urlExpression(row, state).asInstanceOf[TextValue]
      val url = getImportURL(urlString.stringValue(), state.query)

      format match {
        case HasHeaders =>
          val iterator = getLoadCSVIterator(state, url, useHeaders = true)
          val headers =
            if (iterator.nonEmpty) iterator.next().toIndexedSeq else IndexedSeq.empty // First row is headers
          new IteratorWithHeaders(replaceNoValues(headers), row, url.getFile, iterator)
        case NoHeaders =>
          new IteratorWithoutHeaders(row, url.getFile, getLoadCSVIterator(state, url, useHeaders = false))
      }
    })
  }

  private def replaceNoValues(headers: IndexedSeq[Value]): IndexedSeq[Value] = headers.map {
    case noValue if noValue eq Values.NO_VALUE => Values.stringValue("")
    case other                                 => other
  }
}
