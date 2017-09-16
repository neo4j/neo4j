/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.pipes

import java.util.Comparator

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{Pipe, PipeWithSource, QueryState}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.DefaultComparatorTopTable
import org.neo4j.cypher.internal.v3_3.logical.plans.LogicalPlanId
import org.neo4j.values.storable.NumberValue

import scala.collection.JavaConverters._

/*
 * TopSlottedPipe is used when a query does a ORDER BY ... LIMIT query. Instead of ordering the whole result set and then
 * returning the matching top results, we only keep the top results in heap, which allows us to release memory earlier
 */
abstract class TopSlottedPipe(source: Pipe, orderBy: Seq[ColumnOrder])
  extends PipeWithSource(source)  {

  protected val comparator = orderBy
    .map(ExecutionContextOrdering.comparator(_))
    .reduceLeft[Comparator[ExecutionContext]]((a, b) => a.thenComparing(b))
}

case class TopNSlottedPipe(source: Pipe, orderBy: Seq[ColumnOrder], countExpression: Expression)
                           (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends TopSlottedPipe(source, orderBy) {

  countExpression.registerOwningPipe(this)

  protected override def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty)
      Iterator.empty
    else if (orderBy.isEmpty)
      input
    else {
      val first = input.next()
      val count = countExpression(first, state).asInstanceOf[NumberValue].longValue().toInt
      val topTable = new DefaultComparatorTopTable(comparator, count)
      topTable.add(first)

      input.foreach {
        ctx =>
          topTable.add(ctx)
      }

      topTable.sort()

      topTable.iterator.asScala
    }
  }
}

/*
 * Special case for when we only have one element, in this case it is no idea to store
 * an array, instead just store a single value.
 */
case class Top1SlottedPipe(source: Pipe, orderBy: List[ColumnOrder])
                          (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends TopSlottedPipe(source, orderBy) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty)
      Iterator.empty
    else if (orderBy.isEmpty)
      input
    else {

      val first = input.next()
      var result = first

      input.foreach {
        ctx =>
          if (comparator.compare(ctx, result) < 0) {
            result = ctx
          }
      }
      Iterator.single(result)
    }
  }
}

/*
 * Special case for when we only want one element, and all others that have the same value (tied for first place)
 */
case class Top1WithTiesSlottedPipe(source: Pipe, orderBy: List[ColumnOrder])
                                  (val id: LogicalPlanId = LogicalPlanId.DEFAULT)
  extends TopSlottedPipe(source, orderBy) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    if (input.isEmpty)
      Iterator.empty
    else {
      val first = input.next()
      var best = first
      var matchingRows = init(best)

      input.foreach {
        ctx =>
          val comparison = comparator.compare(ctx, best)
          if (comparison < 0) { // Found a new best
            best = ctx
            matchingRows.clear()
            matchingRows += ctx
          }

          if (comparison == 0) { // Found a tie
            matchingRows += ctx
          }
      }
      matchingRows.result().iterator
    }
  }

  @inline
  private def init(first: ExecutionContext) = {
    val builder = Vector.newBuilder[ExecutionContext]
    builder += first
    builder
  }
}