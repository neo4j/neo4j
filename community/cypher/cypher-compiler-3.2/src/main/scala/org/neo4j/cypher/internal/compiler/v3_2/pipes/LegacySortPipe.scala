/*
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
package org.neo4j.cypher.internal.compiler.v3_2.pipes

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.commands.SortItem
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.LegacyExpressions

import scala.math.signum

case class LegacySortPipe(source: Pipe, sortDescription: List[SortItem])
              (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with ExecutionContextComparer with NoEffectsPipe {
  def symbols = source.symbols

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    //register as parent so that stats are associated with this pipe
    state.decorator.registerParentPipe(this)

    input.toList.
      sortWith((a, b) => compareBy(a, b, sortDescription)(state)).iterator
  }

  def planDescription = {
    val sorting = sortDescription.zipWithIndex.map {
      case (sortItem, idx) => s"o$idx" -> sortItem.expression
    }

    source.planDescription.andThen(this.id, "Sort", variables, LegacyExpressions(sorting.toMap))
  }

  override def dup(sources: List[Pipe]): Pipe = {
    val (source :: Nil) = sources
    copy(source = source)
  }
}

trait ExecutionContextComparer extends Comparer {
  def compareBy(a: ExecutionContext, b: ExecutionContext, order: Seq[SortItem])(implicit qtx: QueryState): Boolean = order match {
    case Nil => false
    case head :: tail => {
      val aVal = head(a)
      val bVal = head(b)
      signum(compare(aVal, bVal)) match {
        case 1 => !head.ascending
        case -1 => head.ascending
        case 0 => compareBy(a, b, tail)
      }
    }
  }

}
