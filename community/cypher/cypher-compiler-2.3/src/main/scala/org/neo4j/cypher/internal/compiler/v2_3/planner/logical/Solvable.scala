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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.PatternRelationship

sealed trait Solvable {
  def solvables: Set[SolvableLeaf]

  def solvedRelationship: Option[PatternRelationship] = None
}

sealed trait SolvableLeaf extends Solvable {
  self =>

  override def solvables: Set[SolvableLeaf] = Set(self)
}

final case class SolvableBlock(solvables: Set[SolvableLeaf]) extends Solvable

final case class SolvableRelationship(relationship: PatternRelationship) extends SolvableLeaf {
  override def solvedRelationship = Some(relationship)
}

object Solvables {
  def apply(qg: QueryGraph): Set[Solvable] = qg.patternRelationships.map(SolvableRelationship)
}



