/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.SimpleLogicalPlanner._
import org.mockito.Mockito._

class CandidateListTest extends CypherFunSuite {
  val x = plan("x")
  val y = plan("y")
  val xAndY = plan("x", "y")

  test("prune with no overlaps returns the same candidates") {
    val candidates = CandidateList(Seq(x, y))
    candidates.pruned should equal(candidates)
  }

  test("prune with overlaps returns the first ones") {
    val candidates = CandidateList(Seq(x, xAndY))

    candidates.pruned should equal(CandidateList(Seq(x)))
  }

  test("empty prune is legal") {
    val candidates = CandidateList(Seq())

    candidates.pruned should equal(CandidateList(Seq()))
  }

  def plan(ids: String*): PlanTableEntry = {
    val plan = mock[PlanTableEntry]
    when(plan.coveredIds).thenReturn(ids.map(IdName.apply).toSet)
    when(plan.toString).thenReturn(ids.mkString)
    plan
  }
}


