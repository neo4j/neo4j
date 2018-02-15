/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import java.net.URL

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_3.helpers.ArrayBackedMap
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.LoadExternalResourceException
import org.neo4j.cypher.internal.frontend.v2_3.symbols.{AnyType, CollectionType, MapType}

sealed trait CSVFormat
case object HasHeaders extends CSVFormat
case object NoHeaders extends CSVFormat

case class LoadCSVPipe(source: Pipe,
                  format: CSVFormat,
                  urlExpression: Expression,
                  identifier: String,
                  fieldTerminator: Option[String])(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) {

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
    private var nextContext: ExecutionContext = null
    private var needsUpdate = true

    def hasNext: Boolean = {
      if (needsUpdate) {
        nextContext = computeNextRow()
        needsUpdate = false
      }
      nextContext != null
    }

    def next(): ExecutionContext = {
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
        context.newWith(identifier -> internalMap.copy)
      } else null
    }
  }

  private class IteratorWithoutHeaders(context: ExecutionContext, inner: Iterator[Array[String]]) extends Iterator[ExecutionContext] {
    override def hasNext: Boolean = inner.hasNext

    override def next(): ExecutionContext = context.newWith(identifier -> inner.next().toSeq)
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    input.flatMap(context => {
      implicit val s = state
      val url = getImportURL(urlExpression(context).asInstanceOf[String], state.query)

      val iterator: Iterator[Array[String]] = state.resources.getCsvIterator(url, fieldTerminator, format match {case HasHeaders => true; case _ => false})
      format match {
        case HasHeaders =>
          val headers = if (iterator.nonEmpty) iterator.next().toIndexedSeq else IndexedSeq.empty // First row is headers
          new IteratorWithHeaders(headers, context, iterator)
        case NoHeaders =>
          new IteratorWithoutHeaders(context, iterator)
      }
    })
  }

  def planDescription: InternalPlanDescription =
    source.planDescription.andThen(this.id, "LoadCSV", identifiers)

  def symbols: SymbolTable = format match {
    case HasHeaders => source.symbols.add(identifier, MapType.instance)
    case NoHeaders => source.symbols.add(identifier, CollectionType(AnyType.instance))
  }

  override def localEffects = Effects()

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }
}
