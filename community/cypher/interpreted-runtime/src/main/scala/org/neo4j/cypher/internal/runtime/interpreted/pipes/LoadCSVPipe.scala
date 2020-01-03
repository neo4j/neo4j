/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import java.net.URL

import org.neo4j.cypher.internal.ir.{CSVFormat, HasHeaders, NoHeaders}
import org.neo4j.cypher.internal.runtime.{ArrayBackedMap, ExecutionContext, QueryContext}
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.v4_0.util.attribution.Id
import org.neo4j.exceptions.LoadExternalResourceException
import org.neo4j.values._
import org.neo4j.values.storable.{TextValue, Value, Values}
import org.neo4j.values.virtual.{MapValueBuilder, VirtualValues}

case class LoadCSVPipe(source: Pipe,
                       format: CSVFormat,
                       urlExpression: Expression,
                       variable: String,
                       fieldTerminator: Option[String],
                       legacyCsvQuoteEscaping: Boolean,
                        bufferSize: Int)
                      (val id: Id = Id.INVALID_ID)
  extends PipeWithSource(source) {

  urlExpression.registerOwningPipe(this)

  protected def getImportURL(urlString: String, context: QueryContext): URL = {
    val url: URL = try {
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

  private def copyWithLinenumber(filename: String, linenumber: Long, last: Boolean, row: ExecutionContext, key: String, value: AnyValue): ExecutionContext = {
    val newCtx = executionContextFactory.copyWith(row, key, value)
    newCtx.setLinenumber(filename, linenumber, last)
    newCtx
  }

  //Uses an ArrayBackedMap to store header-to-values mapping
  private class IteratorWithHeaders(headers: Seq[Value], context: ExecutionContext, filename: String, inner: LoadCsvIterator) extends Iterator[ExecutionContext] {
    private val internalMap = new ArrayBackedMap[String, AnyValue](headers.map(a => if (a eq Values.NO_VALUE) null else a.asInstanceOf[TextValue].stringValue()).zipWithIndex.toMap)
    private var nextContext: ExecutionContext = _
    private var needsUpdate = true

    override def hasNext: Boolean = {
      if (needsUpdate) {
        nextContext = computeNextRow()
        needsUpdate = false
      }
      nextContext != null
    }

    override def next(): ExecutionContext = {
      if (!hasNext) Iterator.empty.next()
      needsUpdate = true
      nextContext
    }

    private def computeNextRow() = {
      if (inner.hasNext) {
        val row = inner.next().map(s => Values.stringOrNoValue(s))
        internalMap.putValues(row.asInstanceOf[Array[AnyValue]])
        //we need to make a copy here since someone may hold on this
        //reference, e.g. EagerPipe


        val builder = new MapValueBuilder
        for ((key, maybeNull) <- internalMap) {
          val value = if (maybeNull == null) Values.NO_VALUE else maybeNull
          builder.add(key, value)
        }
        copyWithLinenumber(filename, inner.lastProcessed, inner.readAll, context, variable, builder.build())
      } else null
    }
  }

  private class IteratorWithoutHeaders(context: ExecutionContext, filename: String, inner: LoadCsvIterator) extends Iterator[ExecutionContext] {
    override def hasNext: Boolean = inner.hasNext

    override def next(): ExecutionContext = {
      // Make sure to pull on inner.next before calling inner.lastProcessed to get the right line number
      val value = VirtualValues.list(inner.next().map(s => Values.stringOrNoValue(s)): _*)
      copyWithLinenumber(filename, inner.lastProcessed, inner.readAll, context, variable, value)
    }
  }

  private def getLoadCSVIterator(state: QueryState, url: URL, useHeaders: Boolean): LoadCsvIterator ={
    state.resources.getCsvIterator(
      url, fieldTerminator, legacyCsvQuoteEscaping, bufferSize, useHeaders
    )
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap(context => {
      val urlString: TextValue = urlExpression(context, state).asInstanceOf[TextValue]
      val url = getImportURL(urlString.stringValue(), state.query)

      format match {
        case HasHeaders =>
          val iterator = getLoadCSVIterator(state, url, useHeaders = true)
          val headers = if (iterator.nonEmpty) iterator.next().map(s => Values.stringOrNoValue(s)).toIndexedSeq else IndexedSeq.empty // First row is headers
          new IteratorWithHeaders(headers, context, url.getFile, iterator)
        case NoHeaders =>
          new IteratorWithoutHeaders(context, url.getFile, getLoadCSVIterator(state, url, useHeaders = false))
      }
    })
  }
}
