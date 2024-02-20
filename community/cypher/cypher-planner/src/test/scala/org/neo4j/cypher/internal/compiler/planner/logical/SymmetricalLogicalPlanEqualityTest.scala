/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.helpers.LogicalPlanBuilder
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SymmetricalLogicalPlanEqualityTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {
  private def planBuilder: LogicalPlanBuilder = plannerBuilder().setAllNodesCardinality(100).build().subPlanBuilder()

  test("simple Cartesian product should be symmetrical") {
    planBuilder
      .cartesianProduct()
      .|.allNodeScan("n")
      .allNodeScan("m")
      .build() should equal(
      planBuilder
        .cartesianProduct()
        .|.allNodeScan("m")
        .allNodeScan("n")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("nesting of Cartesian products should not matter") {
    planBuilder
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.allNodeScan("c")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build() should equal(
      planBuilder
        .cartesianProduct()
        .|.allNodeScan("c")
        .cartesianProduct()
        .|.allNodeScan("b")
        .allNodeScan("a")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Cartesian product leaves should all be treated equal") {
    planBuilder
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.allNodeScan("c")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build() should equal(
      planBuilder
        .cartesianProduct()
        .|.allNodeScan("b")
        .cartesianProduct()
        .|.allNodeScan("a")
        .allNodeScan("c")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("Count of Cartesian product leaves should matter") {
    planBuilder
      .cartesianProduct()
      .|.cartesianProduct()
      .|.|.allNodeScan("b")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build() shouldNot equal(
      planBuilder
        .cartesianProduct()
        .|.cartesianProduct()
        .|.|.allNodeScan("b")
        .|.allNodeScan("a")
        .allNodeScan("a")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("simple union should be symmetrical") {
    planBuilder
      .union()
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build() should equal(
      planBuilder
        .union()
        .|.allNodeScan("a")
        .allNodeScan("b")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("union below Cartesian product should be symmetrical") {
    planBuilder
      .cartesianProduct()
      .|.union()
      .|.|.allNodeScan("c")
      .|.allNodeScan("b")
      .allNodeScan("a")
      .build() should equal(
      planBuilder
        .cartesianProduct()
        .|.allNodeScan("a")
        .union()
        .|.allNodeScan("b")
        .allNodeScan("c")
        .build()
    )(SymmetricalLogicalPlanEquality)
  }
}
