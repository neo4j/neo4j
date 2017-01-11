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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.KeyNames
import org.neo4j.cypher.internal.compiler.v2_3.{Comparer, ExecutionContext}

import scala.math.Ordering

sealed trait SortDescription {
  def id: String
  def compareAny(a: Any, b: Any)(implicit qtx: QueryState): Int
}

case class Ascending(id:String) extends SortDescription with Comparer {
  override def compareAny(a: Any, b: Any)(implicit qtx: QueryState) = compare(a, b)
}

case class Descending(id:String) extends SortDescription with Comparer {
  override def compareAny(a: Any, b: Any)(implicit qtx: QueryState) = compare(b, a)
}

case class SortPipe(source: Pipe, orderBy: Seq[SortDescription])
                   (val estimatedCardinality: Option[Double] = None)(implicit monitor: PipeMonitor)
  extends PipeWithSource(source, monitor) with RonjaPipe with NoEffectsPipe {
  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    val array = input.toArray
    java.util.Arrays.sort(array, ordering(orderBy)(state))
    array.toIterator
  }

  def planDescriptionWithoutCardinality = source.planDescription.andThen(this.id, "Sort", identifiers, KeyNames(orderBy.map(_.id)))

  def symbols = source.symbols

  private def ordering(order: Seq[SortDescription])
                      (implicit qtx: QueryState): Ordering[ExecutionContext] = if (order.isEmpty) emptyOrdering
                      else new InnerOrdering(order)


  def dup(sources: List[Pipe]): Pipe = {
    val (head :: Nil) = sources
    copy(source = head)(estimatedCardinality)
  }

  def withEstimatedCardinality(estimated: Double) = copy()(Some(estimated))
}

private object emptyOrdering extends scala.Ordering[ExecutionContext] {
  override def compare(x: ExecutionContext, y: ExecutionContext): Int = -1
}

private class InnerOrdering(order: Seq[SortDescription])(implicit qtx: QueryState) extends scala.Ordering[ExecutionContext] {
  assert(order.nonEmpty)
  private var cmp = -1

  override def compare(a: ExecutionContext, b: ExecutionContext): Int = {
    val iterator: Iterator[SortDescription] = order.iterator
    //we know iterator contains at least one value
    do nextCmp(iterator, a, b)
    while (iterator.hasNext && cmp == 0)
    cmp
  }

  private def nextCmp(it: Iterator[SortDescription], a: ExecutionContext, b: ExecutionContext) = {
    val sort = it.next()
    val column = sort.id
    val aVal = a(column)
    val bVal = b(column)
    cmp = sort.compareAny(aVal, bVal)
  }
}
