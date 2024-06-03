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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ApplyPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  test("does not use Apply for aggregation and order by") {
    val cfg = plannerBuilder().setAllNodesCardinality(100).build()
    val plan = cfg.plan("MATCH (n) RETURN DISTINCT n.name")
    no(plan.flatten(CancellationChecker.neverCancelled())) should matchPattern {
      case _: Apply =>
    }
  }

  test("should not plan sort on top of apply plan when lhs is empty argument and rhs is ordered") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 10)
      .build()

    val plan = planner.plan(
      s"OPTIONAL MATCH (n:Label) RETURN n ORDER BY n"
    )

    plan should equal(
      planner.planBuilder()
        .produceResults("n")
        .optional()
        .nodeByLabelScan("n", "Label", indexOrder = IndexOrderAscending)
        .build()
    )
  }

  test("should unnest Apply after multiple rewrites on RHS, Trail->VarExpand->BFSPruningVarExpand") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:HAS]->()", 200)
      .build()

    val query =
      """
        |MATCH (x)
        |WITH * SKIP 0
        |MATCH (simplePort)<-[:HAS]-{1,9}(otherEndParentDevice)
        |RETURN DISTINCT simplePort.name AS sp_name
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .distinct("cacheN[simplePort.name] AS sp_name")
      .bfsPruningVarExpand("(simplePort)<-[:HAS*1..9]-(otherEndParentDevice)")
      .cacheProperties("cacheNFromStore[simplePort.name]")
      .apply()
      .|.allNodeScan("simplePort", "x")
      .skip(0)
      .allNodeScan("x")
      .build()
  }
}
