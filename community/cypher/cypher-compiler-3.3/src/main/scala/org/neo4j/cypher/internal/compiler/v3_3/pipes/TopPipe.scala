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
package org.neo4j.cypher.internal.compiler.v3_3.pipes

import java.util.Comparator

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id

import scala.math._

/*
 * TopPipe is used when a query does a ORDER BY ... LIMIT query. Instead of ordering the whole result set and then
 * returning the matching top results, we only keep the top results in heap, which allows us to release memory earlier
 */
abstract class TopPipe(source: Pipe, sortDescription: List[SortDescription])(implicit pipeMonitor: PipeMonitor)
  extends PipeWithSource(source, pipeMonitor) with Comparer {

  val sortItems: Array[SortDescription] = sortDescription.toArray
  private val sortItemsCount: Int = sortItems.length

  type SortDataWithContext = (Array[Any], ExecutionContext)

  class LessThanComparator(comparer: Comparer)(implicit qtx : QueryState) extends Ordering[SortDataWithContext] {
    override def compare(a: SortDataWithContext, b: SortDataWithContext): Int = {
      val v1 = a._1
      val v2 = b._1
      var i = 0
      while (i < sortItemsCount) {
        val res = sortItems(i).compareAny(v1(i), v2(i))

        if (res != 0)
          return res
        i += 1
      }
      0
    }
  }

  def binarySearch(array: Array[SortDataWithContext], comparator: Comparator[SortDataWithContext])(key: SortDataWithContext) = {
    java.util.Arrays.binarySearch(array.asInstanceOf[Array[SortDataWithContext]], key, comparator)
  }

  def arrayEntry(ctx : ExecutionContext)(implicit qtx : QueryState) : SortDataWithContext =
    (sortItems.map(column => ctx(column.id)), ctx)
}

case class TopNPipe(source: Pipe, sortDescription: List[SortDescription], countExpression: Expression)
                   (val id: Id = new Id)
                   (implicit pipeMonitor: PipeMonitor) extends TopPipe(source, sortDescription)(pipeMonitor) {

  countExpression.registerOwningPipe(this)

  protected override def internalCreateResults(input:Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    implicit val s = state
    if (input.isEmpty)
      Iterator.empty
    else if (sortDescription.isEmpty)
      input
    else {

      val first = input.next()
      val count = countExpression(first).asInstanceOf[Number].intValue()

      if (count <= 0) {
        Iterator.empty
      } else {

        var result = new Array[SortDataWithContext](count)
        result(0) = arrayEntry(first)
        var last : Int = 0

        while ( last < count - 1 && input.hasNext ) {
          last += 1
          result(last) = arrayEntry(input.next())
        }

        val lessThan = new LessThanComparator(this)
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
  }
}

/*
 * Special case for when we only have one element, in this case it is no idea to store
 * an array, instead just store a single value.
 */
case class Top1Pipe(source: Pipe, sortDescription: List[SortDescription])
                   (val id: Id = new Id)
                   (implicit pipeMonitor: PipeMonitor)
  extends TopPipe(source, sortDescription)(pipeMonitor) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                      state: QueryState): Iterator[ExecutionContext] = {
    implicit val s = state
    if (input.isEmpty)
      Iterator.empty
    else if (sortDescription.isEmpty)
      input
    else {

      val lessThan = new LessThanComparator(this)

      val first = input.next()
      var result = arrayEntry(first)

      input.foreach {
        ctx =>
          val next = arrayEntry(ctx)
          if (lessThan.compare(next, result) < 0) {
            result = next
          }
      }
      Iterator.single(result._2)
    }
  }
}

/*
 * Special case for when we only want one element, and all others that have the same value (tied for first place)
 */
case class Top1WithTiesPipe(source: Pipe, sortDescription: List[SortDescription])
                           (val id: Id = new Id)
                           (implicit pipeMonitor: PipeMonitor)
  extends TopPipe(source, sortDescription)(pipeMonitor) {

  protected override def internalCreateResults(input: Iterator[ExecutionContext],
                                               state: QueryState): Iterator[ExecutionContext] = {
    implicit val s = state
    if (input.isEmpty)
      Iterator.empty
    else {
      val lessThan = new LessThanComparator(this)

      val first = input.next()
      var best = arrayEntry(first)
      var matchingRows = init(best)

      input.foreach {
        ctx =>
          val next = arrayEntry(ctx)
          val comparison = lessThan.compare(next, best)
          if (comparison < 0) { // Found a new best
            best = next
            matchingRows = init(next)
          }

          if (comparison == 0) {  // Found a tie
            matchingRows += next._2
          }
      }
      matchingRows.result().iterator
    }
  }

  @inline
  private def init(first: SortDataWithContext) = {
    val builder = Vector.newBuilder[ExecutionContext]
    builder += first._2
    builder
  }
}
