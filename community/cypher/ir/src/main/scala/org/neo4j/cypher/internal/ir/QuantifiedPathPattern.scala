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

import org.neo4j.cypher.internal.util.Repetition

/**
 * Describes the connection between two juxtaposed nodes - one inside of a [[QuantifiedPathPattern]]
 * and the other one outside.
 */
case class NodeBinding(inner: String, outer: String) {
  override def toString: String = s"(inner=$inner, outer=$outer)"
}

/**
 * Describes a variable that is exposed from a [[QuantifiedPath]].
 *
 * @param singletonName the name of the singleton variable inside the QuantifiedPath.
 * @param groupName     the name of the group variable exposed outside of the QuantifiedPath.
 */
case class VariableGrouping(singletonName: String, groupName: String) {
  override def toString: String = s"(singletonName=$singletonName, groupName=$groupName)"
}

final case class QuantifiedPathPattern(
  leftBinding: NodeBinding,
  rightBinding: NodeBinding,
  pattern: QueryGraph,
  repetition: Repetition,
  nodeVariableGroupings: Set[VariableGrouping],
  relationshipVariableGroupings: Set[VariableGrouping]
) extends NodeConnection {

  override val left: String = leftBinding.outer
  override val right: String = rightBinding.outer

  override val nodes: (String, String) = (left, right)

  override lazy val coveredIds: Set[String] = coveredNodeIds ++ groupings

  override def toString: String = {
    s"QPP($leftBinding, $rightBinding, $pattern, $repetition, $nodeVariableGroupings, $relationshipVariableGroupings)"
  }

  val dependencies: Set[String] = pattern.dependencies

  val groupings: Set[String] = nodeVariableGroupings.map(_.groupName) ++ relationshipVariableGroupings.map(_.groupName)
}
