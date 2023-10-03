/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.cardinality.assumeIndependence

import org.neo4j.cypher.internal.ir.ExhaustiveNodeConnection
import org.neo4j.cypher.internal.ir.ExhaustivePathPattern.NodeConnections
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.util.Fby
import org.neo4j.cypher.internal.util.Last
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.UpperBound

trait NodeConnectionManipulation {

  /**
   * Generates a lazy, non-empty, and finite list of increasingly larger patterns by repeatedly incrementing the upper bounds of the quantified path patterns contained in the given path pattern.
   *
   * For example, given following path pattern:
   * {{{
   *   ((:A)-[:R]->(:B))+((:C)<-[:S]-(:D)){0,2}
   * }}}
   * This function will generate, in order:
   * {{{
   *   ((:A)-[:R]->(:B)){1,1}((:C)<-[:S]-(:D)){0,1}
   *   ((:A)-[:R]->(:B)){1,2}((:C)<-[:S]-(:D)){0,2}
   *   ((:A)-[:R]->(:B)){1,3}((:C)<-[:S]-(:D)){0,2}
   *   …
   *   ((:A)-[:R]->(:B)){1,32}((:C)<-[:S]-(:D)){0,2}
   * }}}
   */
  protected def increasinglyLargerPatterns(
    nodeConnections: NodeConnections[ExhaustiveNodeConnection]
  ): LazyList[NodeConnections[ExhaustiveNodeConnection]] =
    transpose(nodeConnections.connections.map(increasinglyLargerConnection)).map(NodeConnections.apply)

  protected def increasinglyLargerConnection(connection: ExhaustiveNodeConnection): LazyList[ExhaustiveNodeConnection] =
    connection match {
      case rel: PatternRelationship   => LazyList(rel)
      case qpp: QuantifiedPathPattern =>
        // We can't instantiate a quantifier with an upper bound of 0, so we start with at least 1
        val minimumUpperBound = math.max(qpp.repetition.min, 1)
        val maximumUpperBound = qpp.repetition.max match {
          case UpperBound.Unlimited  => RepetitionCardinalityModel.MAX_VAR_LENGTH
          case UpperBound.Limited(n) => math.min(n, RepetitionCardinalityModel.MAX_VAR_LENGTH)
        }
        LazyList.range(
          start = minimumUpperBound,
          end = maximumUpperBound + 1 // `end` is exclusive, it is "the first value NOT contained"
        ).map { upperBound =>
          qpp.copy(repetition = qpp.repetition.copy(max = UpperBound.Limited(upperBound)))
        }
    }

  /**
   * Zips the lazy lists together, returning a list the size of the longest input list.
   * If a list is shorter than the longest one, its last value gets repeated.
   * All the lazy lists in the input must have at least one element, to allow for it to be repeated, an exception will be raised otherwise.
   * It is a sort of matrix transposition.
   *
   * {{{
   *   [[1,2,3],[0],[10..]] => [[1,0,10], [2,0,11], [3,0,12], [3,0,13], [3,0,14], …]
   * }}}
   */
  protected def transpose[A](input: NonEmptyList[LazyList[A]]): LazyList[NonEmptyList[A]] =
    input match {
      case Fby(firstColumn, otherColumns) =>
        val firstRow = NonEmptyList.cons(firstColumn.head, otherColumns.map(_.head))
        val remainingInput = NonEmptyList.cons(firstColumn.tail, otherColumns.map(_.tail))
        firstRow #:: LazyList.unfold((firstRow, remainingInput)) {
          case (previousRow, columns) =>
            if (columns.forall(_.isEmpty))
              None
            else {
              val row = columns.map(_.headOption).zipWith(previousRow) {
                case (cell, previousValue) => cell.getOrElse(previousValue)
              }
              val next = columns.map(col => if (col.nonEmpty) col.tail else LazyList.empty)
              Some((row, (row, next)))
            }
        }

      case Last(column) =>
        column.map(NonEmptyList.singleton)
    }
}
