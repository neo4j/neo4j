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
package org.neo4j.cypher.internal.compiler.v1_9.pipes

import scala.math.signum
import org.neo4j.cypher.internal.compiler.v1_9.commands.SortItem
import java.lang.String
import org.neo4j.cypher.internal.compiler.v1_9.{ExecutionContext, Comparer}
import collection.mutable.Map
import org.neo4j.cypher.internal.compiler.v1_9.symbols.SymbolTable

class SortPipe(source: Pipe, sortDescription: List[SortItem]) extends PipeWithSource(source) with ExecutionContextComparer {
  def symbols = source.symbols

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) =
    input.toList.
      sortWith((a, b) => compareBy(a, b, sortDescription)).iterator

  def throwIfSymbolsMissing(symbols: SymbolTable) {
    sortDescription.foreach {
      case SortItem(e,_) => e.throwIfSymbolsMissing(source.symbols)
    }
  }

  override def executionPlanDescription = source.executionPlanDescription.andThen(this, "Sort", "descr" -> sortDescription)

  override def isLazy = false
}

trait ExecutionContextComparer extends Comparer {
  def compareBy(a: Map[String, Any], b: Map[String, Any], order: Seq[SortItem]): Boolean = order match {
    case Nil => false
    case head :: tail => {
      val key = head.columnName
      val aVal = a(key)
      val bVal = b(key)
      signum(compare(aVal, bVal)) match {
        case 1 => !head.ascending
        case -1 => head.ascending
        case 0 => compareBy(a, b, tail)
      }
    }
  }

}
