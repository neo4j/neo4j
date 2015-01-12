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

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.{PatternRelationship, IdName}
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.mutable

object conservativeQueryAcceptor extends (UnionQuery => Boolean) {

  def apply(query: UnionQuery): Boolean = {
    !query.queries.exists(query => rejectQuery (query) )
  }

  private def rejectQuery(pq: PlannerQuery): Boolean = {
    containsVarLength(pq.graph) ||
      pq.graph.optionalMatches.exists(containsVarLength) ||
      containsCycles(pq.graph) ||
      pq.graph.optionalMatches.exists(containsCycles) ||
      pq.tail.exists(rejectQuery)
  }

  private def containsVarLength(qg: QueryGraph): Boolean = qg.patternRelationships.exists(!_.length.isSimple)

  private def containsCycles(qg: QueryGraph): Boolean = {
    val visitedNodes = mutable.HashSet[IdName]()
    val vistedRels = mutable.HashSet[PatternRelationship]()

    //put one (node,relationship) pair on the stack
    val toVisit = mutable.Stack[(IdName, PatternRelationship)]()
    qg.patternRelationships.headOption.foreach(r => toVisit.push((r.left, r)))

    while(toVisit.nonEmpty) {
      //pop rel and follow it along its relationships
      val (node, rel) = toVisit.pop()
      visitedNodes += node
      vistedRels += rel

      val otherNode = rel.otherSide(node)
      if (visitedNodes(otherNode)) return true
      visitedNodes += otherNode

      //dfs step, follow all relationships ending on node
      qg.findRelationshipsEndingOn(otherNode)
        .filterNot(vistedRels)
        .foreach(r => toVisit.push((otherNode, r)))

      //if we have exhausted subgraph, find next disconnected component
      if (toVisit.isEmpty) {
        val leftToVisit = qg.patternRelationships -- vistedRels
        leftToVisit.headOption.foreach(r => toVisit.push((r.left, r)))
        visitedNodes.clear()
      }
    }

    false
  }
}
