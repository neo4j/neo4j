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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.options.CypherDebugOption
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class DisallowSplittingTopIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport {

  test("option debug=disallowSplittingTop should disable sorting in leaves under limit") {
    val q =
      """MATCH (a)-[r1]->(b)-[r2]->(c)
        |RETURN b ORDER BY b LIMIT 10
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(2000)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(containPlanMatching({ case _: Sort => }) and containPlanMatching({ case _: Limit => }))
    forceTopPlan.should(containPlanMatching({ case _: Top => }) and not(containPlanMatching({ case _: Sort => })))
  }

  test("option debug=disallowSplittingTop should not affect plans without limit") {
    val q =
      """MATCH (a)-[r1]->(b)-[r2]->(c)
        |RETURN b ORDER BY b
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(2000)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(containPlanMatching({ case _: Sort => }))
    forceTopPlan.shouldEqual(defaultPlan)
  }

  test("option debug=disallowSplittingTop should not affect index-backed-order-by") {
    val q =
      """MATCH (a)-[r1]->(b:B {prop: 1})-[r2]->(c)
        |RETURN b ORDER BY b.prop
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("B", 500)
      .setAllRelationshipsCardinality(2000)
      .setRelationshipCardinality("()-[]-(:B)", 1000)
      .addNodeIndex("B", Seq("prop"), 1.0, 0.1, providesOrder = IndexOrderCapability.BOTH)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(not(containPlanMatching({ case _: Sort => })))
    forceTopPlan.shouldEqual(defaultPlan)
  }

  test("option debug=disallowSplittingTop should not affect index-backed-order-by under limit") {
    val q =
      """MATCH (a)-[r1]->(b:B {prop: 1})-[r2]->(c)
        |RETURN b ORDER BY b.prop LIMIT 10
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("B", 500)
      .setAllRelationshipsCardinality(2000)
      .setRelationshipCardinality("()-[]-(:B)", 1000)
      .addNodeIndex("B", Seq("prop"), 1.0, 0.1, providesOrder = IndexOrderCapability.BOTH)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(containPlanMatching({ case _: Limit => }) and not(containPlanMatching({ case _: Sort => })))
    forceTopPlan.shouldEqual(defaultPlan)
  }

  test("option debug=disallowSplittingTop should disable sorting in leaves of previous parts under limit") {
    val q =
      """MATCH (a)-[r1]->(b)-[r2]->(c)
        |WITH *, 1 AS x
        |MATCH (d)
        |RETURN b ORDER BY b LIMIT 10
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(2000)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(containPlanMatching({ case _: Sort => }) and containPlanMatching({ case _: Limit => }))
    forceTopPlan.should(containPlanMatching({ case _: Top => }) and not(containPlanMatching({ case _: Sort => })))
  }

  test("option debug=disallowSplittingTop should not affect explicit ordering in previous parts") {
    val q =
      """MATCH (a)-[r1]->(b)-[r2]->(c)
        |WITH *, 1 AS x ORDER BY b
        |MATCH (d)
        |WITH *, 1 AS y
        |RETURN b ORDER BY b LIMIT 10
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(2000)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(containPlanMatching({ case _: Sort => }) and containPlanMatching({ case _: Limit => }))
    forceTopPlan.shouldEqual(defaultPlan)
  }

  test("option debug=disallowSplittingTop should not affect explicit ordering from another part") {
    val q =
      """MATCH (a)-[r1]->(b)-[r2]->(c)
        |WITH *, 1 AS x
        |MATCH (d)
        |WITH *, 1 AS y ORDER BY b
        |RETURN b ORDER BY b LIMIT 10
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(2000)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(containPlanMatching({ case _: Sort => }) and containPlanMatching({ case _: Limit => }))
    forceTopPlan.shouldEqual(defaultPlan)
  }

  test("option debug=disallowSplittingTop should disable sorting in horizon of previous parts under limit") {
    val q =
      """MATCH (a)
        |WITH *, 1 AS x
        |MATCH (a)-[r1]->(b)-[r2]->(c)
        |RETURN a ORDER BY a.prop LIMIT 10
        |""".stripMargin

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(2000)

    val defaultPlan = cfg.build().plan(q)
    val forceTopPlan = cfg.enableDebugOption(CypherDebugOption.disallowSplittingTop).build().plan(q)

    defaultPlan.should(containPlanMatching({ case _: Sort => }) and containPlanMatching({ case _: Limit => }))
    forceTopPlan.should(containPlanMatching({ case _: Top => }) and not(containPlanMatching({ case _: Sort => })))
  }
}
