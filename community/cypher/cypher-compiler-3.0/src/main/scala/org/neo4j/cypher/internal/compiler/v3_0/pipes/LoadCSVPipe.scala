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
package org.neo4j.cypher.internal.compiler.v3_0.pipes

import java.net.URL

import org.neo4j.cypher.internal.compiler.v3_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_0.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v3_0.helpers.ArrayBackedMap
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_0.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v3_0.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_0.LoadExternalResourceException
import org.neo4j.cypher.internal.frontend.v3_0.symbols.{AnyType, ListType, MapType}

sealed trait CSVFormat
case object HasHeaders extends CSVFormat
case object NoHeaders extends CSVFormat

case class LoadCSVPipe(source: Pipe,
                       format: CSVFormat,
                       urlExpression: Expression,
                       variable: String,
                       fieldTerminator: Option[String])
                      (val estimatedCardinality: Option[Double] = None)(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with RonjaPipe {

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
  private class IteratorWithHeaders(headers: Seq[String], context: ExecutionContext, inner: Iterator[Array[String]]) extends Iterator[ExecutionContext] {
    private val internalMap = new ArrayBackedMap[String, String](headers.zipWithIndex.toMap)
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
        internalMap.putValues(row)
        //we need to make a copy here since someone may hold on this
        //reference, e.g. EagerPipe
        context.newWith(variable -> internalMap.copy)
      } else null
    }
  }

  private class IteratorWithoutHeaders(context: ExecutionContext, inner: Iterator[Array[String]]) extends Iterator[ExecutionContext] {
    override def hasNext: Boolean = inner.hasNext

    override def next(): ExecutionContext = context.newWith(variable -> inner.next().toSeq)
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap(context => {
      implicit val s = state
      val urlString: String = urlExpression(context).asInstanceOf[String]
      val url = getImportURL(urlString, state.query)

      val iterator: Iterator[Array[String]] = state.resources.getCsvIterator(url, fieldTerminator)
      format match {
        case HasHeaders =>
          val headers = iterator.next().toSeq // First row is headers
          new IteratorWithHeaders(headers, context, iterator)
        case NoHeaders =>
          new IteratorWithoutHeaders(context, iterator)
      }
    })
  }

  override def symbols: SymbolTable = format match {
    case HasHeaders => source.symbols.add(variable, MapType.instance)
    case NoHeaders => source.symbols.add(variable, ListType(AnyType.instance))
  }

  override def localEffects = Effects()

  override def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  override def planDescriptionWithoutCardinality: InternalPlanDescription =
    source.planDescription.andThen(this.id, "LoadCSV", variables)

  override def withEstimatedCardinality(estimated: Double): Pipe with RonjaPipe = copy()(Some(estimated))
}
