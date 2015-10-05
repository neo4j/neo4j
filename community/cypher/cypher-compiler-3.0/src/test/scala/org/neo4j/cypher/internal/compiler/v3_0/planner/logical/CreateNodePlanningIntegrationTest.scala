/*
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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical

import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class CreateNodePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan single create") {
    planFor("CREATE (a)").plan should equal(
      EmptyResult(
        CreateNode(SingleRow()(solved), NodePattern(Some(Identifier("a") _), Seq.empty, None, naked = false)(pos))(solved))(solved)
    )
  }

  test("should plan single create with return") {
    planFor("CREATE (a) return a").plan should equal(
      Projection(
        CreateNode(SingleRow()(solved), NodePattern(Some(Identifier("a") _), Seq.empty, None, naked = false)(pos))(solved),
        Map("a" -> Identifier("a")_))
      (solved)
    )
  }

  test("should plan match and create") {
    planFor("MATCH (a) CREATE (b)").plan should equal(
      EmptyResult(
          CreateNode(AllNodesScan(IdName("a"), Set.empty)(solved), NodePattern(Some(Identifier("b") _), Seq.empty, None, naked = false)(pos))(solved)
      )(solved)
    )
  }

  test("should plan create in tail") {
    planFor("MATCH (a) CREATE (b) WITH * MATCH(c) CREATE (d)").plan should equal(
      EmptyResult(
        EagerApply(
          Projection(
            CreateNode(AllNodesScan(IdName("a"), Set.empty)(solved), NodePattern(Some(Identifier("b") _), Seq.empty, None, naked = false)(pos))(solved),
            Map("a" -> Identifier("a")_, "b" -> Identifier("b")_))(solved),
          CreateNode(RepeatableRead(
            AllNodesScan(IdName("c"), Set(IdName("a"), IdName("b")))(solved))(solved),
            NodePattern(Some(Identifier("d") _), Seq.empty, None, naked = false)(pos))(solved))(solved)
        )(solved)
    )
  }
}
