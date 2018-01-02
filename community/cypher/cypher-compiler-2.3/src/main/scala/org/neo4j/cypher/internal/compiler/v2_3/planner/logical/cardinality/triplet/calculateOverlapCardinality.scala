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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.cardinality.triplet

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Cardinality
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{PatternRelationship, IdName}

import scala.annotation.tailrec

object calculateOverlapCardinality {

  def apply(qg: QueryGraph, nodeCardinalities: Map[IdName, Cardinality], argumentCardinality: Cardinality): Cardinality = {
    val arguments = qg.argumentIds
    val patterns = qg.patternRelationships

    @tailrec
    def recurse(remaining: List[PatternRelationship], nodeOverlaps: Map[IdName, Int] = Map.empty, argumentOverlaps: Int = 0): Cardinality = remaining match {
      case (pattern: PatternRelationship) :: tl =>
        val (left, right) = pattern.nodes

        val leftIsArg = arguments.contains(left)
        val rightIsArg = arguments.contains(right)

        val argumentOverlapIncrement = if (leftIsArg || rightIsArg) 1 else 0
        val leftOverlapIncrement = if (leftIsArg) None else Some(incrementedOverlapFor(nodeOverlaps, left))
        val rightOverlapIncrement = if (rightIsArg) None else Some(incrementedOverlapFor(nodeOverlaps, right))
        val newOverlaps = addOverlap(addOverlap(nodeOverlaps, leftOverlapIncrement), rightOverlapIncrement)

        recurse(tl, newOverlaps, argumentOverlaps + argumentOverlapIncrement)

      case Nil =>
        val argumentOverlapCardinality = argumentCardinality ^ argumentOverlaps
        val nodeOverlapCardinalities = nodeOverlaps.collect {
          case (node, overlaps) => nodeCardinalities(node) ^ (overlaps - 1)
        }.toSeq
        val nodeOverlapCardinality = nodeOverlapCardinalities.foldLeft(Cardinality.SINGLE)(_ * _)
        val totalOverlapCardinality = nodeOverlapCardinality * argumentOverlapCardinality
        totalOverlapCardinality
    }

    recurse(patterns.toList)
  }

  private def addOverlap(overlaps: Map[IdName, Int], newOverlap: Option[(IdName, Int)]) = overlaps ++ newOverlap.toSeq

  private def incrementedOverlapFor(overlaps: Map[IdName, Int], node: IdName) = node -> (overlaps.getOrElse(node, 0) + 1)
}
