/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir

import org.neo4j.cypher.internal.v4_0.expressions.Property

/**
 * Encapsulates a query graph with info necessary for certain eager-analysis.
 * @param qg A query graph.
 * @param unstableLeaves The unstable leaves of the considered plan.
 * @param stableIdentifier The identifier of the node found in the stable iterator.
 */
case class QgWithInfo(qg: QueryGraph, unstableLeaves: Set[String], stableIdentifier: Option[String]) {

  def unstablePatternNodes: Set[String] = qg.allPatternNodesRead -- stableIdentifier

  def unstableLeafPatternNodes: Set[String] = unstablePatternNodes intersect unstableLeaves

  def unstableNonArgumentPatternNodes: Set[String] = unstablePatternNodes -- qg.argumentIds

  def unstablePatternRelationships: Set[PatternRelationship] = qg.allPatternRelationshipsRead.filterNot(rel => stableIdentifier.contains(rel.name))

  def allKnownUnstableNodeProperties: Set[Property] = {
    val matchedNodes = qg.patternNodes -- stableIdentifier
    matchedNodes.flatMap(qg.knownProperties) ++ qg.optionalMatches.map(om => copy(qg = om)).flatMap(_.allKnownUnstableNodeProperties)
  }

  def allKnownUnstableRelProperties: Set[Property] = {
    val matchedRels = qg.patternRelationships.map(_.name) -- stableIdentifier
    matchedRels.flatMap(qg.knownProperties) ++ qg.optionalMatches.map(om => copy(qg = om)).flatMap(_.allKnownUnstableRelProperties)
  }
}
