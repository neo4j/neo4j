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
package org.neo4j.cypher.internal.compiler.v3_2.planner

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression
import org.neo4j.cypher.internal.ir.v3_2._

sealed trait MergePattern {
  self : MutatingPattern =>
  def matchGraph: QueryGraph
}

case class MergeNodePattern(createNodePattern: CreateNodePattern, matchGraph: QueryGraph, onCreate: Seq[SetMutatingPattern],
                            onMatch: Seq[SetMutatingPattern]) extends MutatingPattern with MergePattern {
  override def coveredIds = matchGraph.allCoveredIds
}

case class MergeRelationshipPattern(createNodePatterns: Seq[CreateNodePattern], createRelPatterns: Seq[CreateRelationshipPattern],
                                    matchGraph: QueryGraph, onCreate: Seq[SetMutatingPattern], onMatch: Seq[SetMutatingPattern]) extends MutatingPattern with MergePattern {
  override def coveredIds = matchGraph.allCoveredIds
}

case class ForeachPattern(variable: IdName, expression: Expression, innerUpdates: PlannerQuery) extends MutatingPattern with NoSymbols

