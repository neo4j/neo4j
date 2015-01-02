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
package org.neo4j.cypher.internal.compiler.v2_2.planner

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{IdName, PatternRelationship}

import scala.collection.mutable

object conservativeQueryAcceptor extends (UnionQuery => Boolean) {

  def apply(query: UnionQuery): Boolean = {
    !query.queries.exists(query => rejectQuery (query) )
  }

  private def rejectQuery(pq: PlannerQuery): Boolean =
    containsVarLength(pq.graph) ||
      pq.graph.optionalMatches.exists(containsVarLength) ||
      containsCycles(pq.graph) ||
      pq.graph.optionalMatches.exists(containsCycles) ||
      pq.tail.exists(rejectQuery)


  private def containsVarLength(qg: QueryGraph): Boolean = qg.patternRelationships.exists(!_.length.isSimple)

  private def containsCycles(qg: QueryGraph): Boolean = {
    val visitedNodes = mutable.HashSet[IdName]()
    val visitedRels = mutable.HashSet[PatternRelationship]()

    val startPoints = qg.patternRelationships.map(r => (r.left, r))
    val stack = mutable.Stack[(IdName, PatternRelationship)]()
    stack.pushAll(startPoints)
    while (!stack.isEmpty) {
      val (node, rel) = stack.pop()
      if (visitedNodes(node)) return true
      visitedNodes += node
      visitedRels += rel
      val nextNode = rel.otherSide(node)

      //avoid self loops
      if (nextNode == node) return true

      val nextRels = qg.findRelationshipsEndingOn(nextNode).filterNot(visitedRels)
      stack.pushAll(nextRels.map((nextNode, _)))
    }

    false
  }
}
