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

import org.neo4j.cypher.internal.compiler.v3_0.pipes.{LazyType, LazyLabel}
import org.neo4j.cypher.internal.compiler.v3_0.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.test_helpers.CypherFunSuite

class CreateRelationshipPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should plan single create") {
    planFor("CREATE (a)-[r:R]->(b)").plan should equal(
      EmptyResult(
        CreateRelationship(
          CreateNode(
            CreateNode(SingleRow()(solved), IdName("a"), Seq.empty, None)(solved),
            IdName("b"), Seq.empty, None)(solved),
          IdName("r"), IdName("a"), relType("R"), IdName("b"), None)(solved)
      )(solved)
    )
  }

  test("should plan complicated create") {
    planFor("CREATE (a)-[r1:R1]->(b)<-[r2:R2]-(c)-[r3:R3]->(d)").plan should equal(
      EmptyResult(
        CreateRelationship(
          CreateRelationship(
            CreateRelationship(
              CreateNode(
                CreateNode(
                  CreateNode(
                    CreateNode(SingleRow()(solved),IdName("a"),Seq.empty,None)(solved),
                    IdName("b"),Seq.empty,None)(solved),
                  IdName("c"),Seq.empty,None)(solved),
                IdName("d"),Seq.empty,None)(solved),
              IdName("r1"),IdName("a"),relType("R1"),IdName("b"),None)(solved),
            IdName("r2"),IdName("c"),relType("R2"),IdName("b"),None)(solved),
          IdName("r3"),IdName("c"),relType("R3"),IdName("d"),None)(solved)
      )(solved)
    )
  }

  test("should plan reversed create pattern") {
    planFor("CREATE (a)<-[r1:R1]-(b)<-[r2:R2]-(c)").plan should equal(
      EmptyResult(
        CreateRelationship(
          CreateRelationship(
            CreateNode(
              CreateNode(
                CreateNode(SingleRow()(solved),IdName("a"),Seq.empty,None)(solved),
                IdName("b"),Seq.empty,None)(solved),
              IdName("c"),Seq.empty,None)(solved),
            IdName("r1"),IdName("b"),relType("R1"),IdName("a"),None)(solved),
          IdName("r2"),IdName("c"),relType("R2"),IdName("b"),None)(solved)
      )(solved)
    )
  }

  private def relType(name: String): RelTypeName = RelTypeName(name)(pos)
}
