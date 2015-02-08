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

import org.neo4j.cypher.internal.compiler.v2_0._
import commands.SortItem
import commands.expressions.Expression
import data.SimpleVal

import symbols._
import scala.math._
import java.util.Comparator

/*
 * TopPipe is used when a query does a ORDER BY ... LIMIT query. Instead of ordering the whole result set and then
 * returning the matching top results, we only keep the top results in heap, which allows us to release memory earlier
 */
class TopPipe(source: Pipe, sortDescription: List[SortItem], countExpression: Expression) extends PipeWithSource(source) with Comparer {

  val sortItems = sortDescription.toArray
  val sortItemsCount = sortItems.size

  type SortDataWithContext = (Array[Any],ExecutionContext)

  class LessThanComparator(comparer: Comparer)(implicit qtx : QueryState) extends Ordering[SortDataWithContext] {
    override def compare(a: SortDataWithContext, b: SortDataWithContext): Int = {
      val v1 = a._1
      val v2 = b._1
      var i = 0
      while (i < sortItemsCount) {
        val res = signum(comparer.compare(v1(i), v2(i)))
        if (res != 0) {
          val sortItem = sortItems(i)
          return if (sortItem.ascending) res else -res
        }
        i += 1
      }
      0
    }
  }

  def binarySearch(array: Array[SortDataWithContext], comparator: Comparator[SortDataWithContext])(key: SortDataWithContext) = {
    java.util.Arrays.binarySearch(array.asInstanceOf[Array[SortDataWithContext]],key, comparator)
  }

  def arrayEntry(ctx : ExecutionContext)(implicit qtx : QueryState) : SortDataWithContext = (sortItems.map(_(ctx)),ctx)

  protected def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    implicit val s = state
    if (input.isEmpty)
      Iterator.empty
    else if (sortDescription.isEmpty)
      input
    else {

      val lessThan = new LessThanComparator(this)

      val first = input.next()
      val count = countExpression(first).asInstanceOf[Number].intValue()
      var result = new Array[SortDataWithContext](count)
      result(0) = arrayEntry(first)
      var last : Int = 0

      while ( last < count - 1 && input.hasNext ) {
        last += 1
        result(last) = arrayEntry(input.next())
      }

      if (input.isEmpty) {
        result.slice(0,last + 1).sorted(lessThan).iterator.map(_._2)
      } else {
        result = result.sorted(lessThan)

        val search = binarySearch(result, lessThan) _
        input.foreach {
          ctx =>
            val next = arrayEntry(ctx)
            if (lessThan.compare(next, result(last)) < 0) {
              val idx = search(next)
              val insertPosition = if (idx < 0 )  - idx - 1 else idx + 1
              if (insertPosition >= 0 && insertPosition < count) {
                Array.copy(result, insertPosition, result, insertPosition + 1, count - insertPosition - 1)
                result(insertPosition) = next
              }
            }
        }
        result.toIterator.map(_._2)
      }
    }
  }

  def executionPlanDescription =
    source.executionPlanDescription
      .andThen(this, "Top",
        "orderBy" -> SimpleVal.fromIterable(sortDescription),
        "limit" -> SimpleVal.fromStr(countExpression))

  def symbols = source.symbols

  override val isLazy = false
}
