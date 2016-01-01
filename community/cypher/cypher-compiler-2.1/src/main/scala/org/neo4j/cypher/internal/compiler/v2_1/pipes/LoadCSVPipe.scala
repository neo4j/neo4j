/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import java.net.URL

import org.neo4j.cypher.LoadExternalResourceException
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_1.executionplan.Effects
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription.Arguments.IntroducedIdentifier
import org.neo4j.cypher.internal.compiler.v2_1.spi.QueryContext
import org.neo4j.cypher.internal.compiler.v2_1.symbols.{AnyType, CollectionType, MapType, SymbolTable}

sealed trait CSVFormat
case object HasHeaders extends CSVFormat
case object NoHeaders extends CSVFormat

case class LoadCSVPipe(source: Pipe,
                  format: CSVFormat,
                  urlExpression: Expression,
                  identifier: String,
                  fieldTerminator: Option[String])(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) {
  private val protocolWhiteList: Seq[String] = Seq("file", "http", "https", "ftp")

  protected def checkURL(urlString: String, context: QueryContext): URL = {
    val url: URL = try {
      new URL(urlString)
    } catch {
      case e: java.net.MalformedURLException =>
        throw new LoadExternalResourceException(s"Invalid URL specified (${e.getMessage})", null)
    }

    val protocol = url.getProtocol
    if (!protocolWhiteList.contains(protocol)) {
      throw new LoadExternalResourceException(s"Unsupported URL protocol: $protocol", null)
    }
    if (url.getProtocol == "file" && !context.hasLocalFileAccess) {
      throw new LoadExternalResourceException("Accessing local files not allowed by the configuration")
    }
    url
  }

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap(context => {
      implicit val s = state
      val url = checkURL(urlExpression(context).asInstanceOf[String], state.query)

      val iterator: Iterator[Array[String]] = state.resources.getCsvIterator(url, fieldTerminator)

      val nextRow: Array[String] => Iterable[Any] = format match {
        case HasHeaders =>
          val headers = iterator.next().toSeq
          (row: Array[String]) => (headers zip row).toMap
        case NoHeaders =>
          (row: Array[String]) => row.toSeq
      }

      iterator.map {
        value => context.newWith(identifier -> nextRow(value))
      }
    })
  }

  def planDescription: PlanDescription =
    source.planDescription.andThen(this, "LoadCSV", IntroducedIdentifier(identifier))

  def symbols: SymbolTable = format match {
    case HasHeaders => source.symbols.add(identifier, MapType.instance)
    case NoHeaders => source.symbols.add(identifier, CollectionType(AnyType.instance))
  }

  override def localEffects = Effects.NONE

  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)
  }
}

