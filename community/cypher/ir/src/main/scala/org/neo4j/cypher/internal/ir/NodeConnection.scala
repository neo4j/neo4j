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

/**
 * Part of a pattern that is connecting nodes (as in "connected components").
 * This is a generalisation of relationships to be able to plan quantified path patterns using the same algorithm.
 */
trait NodeConnection {

  val left: String
  val right: String

  val nodes: (String, String)

  lazy val coveredNodeIds: Set[String] = Set(left, right)

  def coveredIds: Set[String]

  def otherSide(node: String): String =
    if (node == left) {
      right
    } else if (node == right) {
      left
    } else {
      throw new IllegalArgumentException(
        s"Did not provide either side as an argument to otherSide. Rel: $this, argument: $node"
      )
    }
}
