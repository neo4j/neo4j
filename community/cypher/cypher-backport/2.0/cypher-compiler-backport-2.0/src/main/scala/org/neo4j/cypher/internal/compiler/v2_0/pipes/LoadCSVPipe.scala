/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0.symbols.{CollectionType, AnyType, MapType}
import org.neo4j.cypher.internal.compiler.v2_0.{ExecutionContext, PlanDescription}
import java.io._
import java.net.URL
import org.neo4j.cypher.internal.compiler.v2_0.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_0.spi.LoadCSVQueryContext

sealed trait CSVFormat
case object HasHeaders extends CSVFormat
case object NoHeaders extends CSVFormat

class LoadCSVPipe(source: Pipe, format: CSVFormat, url: URL, identifier: String) extends PipeWithSource(source) {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    input.flatMap(context => {
      val iterator:Iterator[Array[String]] = state.query.asInstanceOf[LoadCSVQueryContext].getCsvIterator(url)

      val nextRow: Array[String] => Iterable[Serializable]  = format match {
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

  def executionPlanDescription: PlanDescription = {
    source.executionPlanDescription.andThen(this, "LoadCSV")
  }

  def symbols: SymbolTable = format match {
    case HasHeaders => source.symbols.add(identifier, MapType.instance)
    case NoHeaders => source.symbols.add(identifier, CollectionType(AnyType.instance))
  }

  override def readsFromDatabase = false
}

