/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.v4_0.util.attribution.IdGen

/**
 * Produce rows from a query state input stream. Only one input operator can
 * exist per logical plan tree, and it has to be the left-most leaf.
 *
 * @param nullable if there can be null values among the nodes, relationships or variables
 */
case class Input(nodes: Seq[String], relationships: Seq[String], variables: Seq[String], nullable: Boolean)(implicit idGen: IdGen) extends LogicalLeafPlan(idGen) {
  val availableSymbols: Set[String] = nodes.toSet ++ relationships.toSet ++ variables.toSet
  override def argumentIds: Set[String] = Set.empty
}

object Input {

  def apply(variables: Seq[String])(implicit idGen: IdGen): Input = new Input(Seq.empty, Seq.empty, variables, true)(idGen)
}
