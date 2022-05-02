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

case class NodeBinding(outer: String, inner: String) {
  override def toString: String = s"(outer=$outer, inner=$inner)"
}

final case class QuantifiedPathPattern(
  leftBinding: NodeBinding,
  rightBinding: NodeBinding,
  pattern: QueryGraph,
  repetition: Repetition
) extends NodeConnection {

  override val left: String = leftBinding.outer
  override val right: String = rightBinding.outer

  override val nodes: (String, String) = (left, right)

  override lazy val coveredIds: Set[String] = coveredNodeIds

  override def toString: String = {
    s"QPP($leftBinding, $rightBinding, $pattern, $repetition)"
  }

  val dependencies: Set[String] = pattern.dependencies
}
