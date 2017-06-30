/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import java.net.URL

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.ArrayBackedMap
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.LoadExternalResourceException
import org.neo4j.cypher.internal.ir.v3_3.{CSVFormat, HasHeaders, NoHeaders}
import org.neo4j.cypher.internal.spi.v3_3.QueryContext
import org.neo4j.values._
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.JavaConverters._

case class LoadCSVPipe(source: Pipe,
                       format: CSVFormat,
                       urlExpression: Expression,
                       variable: String,
                       fieldTerminator: Option[String],
                       legacyCsvQuoteEscaping: Boolean)
                      (val id: Id = new Id)
  extends PipeWithSource(source) {

  urlExpression.registerOwningPipe(this)

  protected def getImportURL(urlString: String, context: QueryContext): URL = {
    val url: URL = try {
      new URL(urlString)
    } catch {
      case e: java.net.MalformedURLException =>
        throw new LoadExternalResourceException(s"Invalid URL '$urlString': ${e.getMessage}")
    }

    context.getImportURL(url) match {
      case Left(error) =>
        throw new LoadExternalResourceException(s"Cannot load from URL '$urlString': $error")
      case Right(urlToLoad) =>
        urlToLoad
    }
  }

  //Uses an ArrayBackedMap to store header-to-values mapping
  private class IteratorWithHeaders(headers: Seq[TextValue], context: ExecutionContext, inner: Iterator[Array[TextValue]]) extends Iterator[ExecutionContext] {
    private val internalMap = new ArrayBackedMap[String, AnyValue](headers.map(_.stringValue()).zipWithIndex.toMap)
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
        val row = inner.next()
        internalMap.putValues(row.asInstanceOf[Array[AnyValue]])
        //we need to make a copy here since someone may hold on this
        //reference, e.g. EagerPipe
        context.newWith1(variable, VirtualValues.map(internalMap.copy.asJava))
      } else null
    }
  }

  private class IteratorWithoutHeaders(context: ExecutionContext, inner: Iterator[Array[TextValue]]) extends Iterator[ExecutionContext] {
    override def hasNext: Boolean = inner.hasNext

    override def next(): ExecutionContext = context.newWith1(variable, VirtualValues.list(inner.next():_*))
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap(context => {
      implicit val s = state
      val urlString: String = urlExpression(context).asInstanceOf[String]
      val url = getImportURL(urlString, state.query)

      val iterator: Iterator[Array[TextValue]] = state.resources.getCsvIterator(url, fieldTerminator, legacyCsvQuoteEscaping).map(_.map(s => Values.stringValue(s)))
      format match {
        case HasHeaders =>
          val headers = if (iterator.nonEmpty) iterator.next().toIndexedSeq else IndexedSeq.empty // First row is headers
          new IteratorWithHeaders(headers, context, iterator)
        case NoHeaders =>
          new IteratorWithoutHeaders(context, iterator)
      }
    })
  }
}
