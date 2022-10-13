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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ByIdSeekPlanningIntegrationTest extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with BeLikeMatcher {

  override protected def plannerBuilder(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    super.plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)

  test("should use expand into for two nodes defined by id and a lot of relationships") {
    val query =
      """MATCH (src)-[:REL]-(dst)
        |  WHERE id(src)= 359340
        |    and id(dst)= 630950
        |RETURN count(*) as relationships""".stripMargin

    val planner = plannerBuilder()
      .setRelationshipCardinality("()-[:REL]->()", 100000)
      .setAllRelationshipsCardinality(100000)
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("relationships")
        .aggregation(Seq(), Seq("count(*) AS relationships"))
        .expandInto("(src)-[anon_0:REL]-(dst)")
        .cartesianProduct()
        .|.nodeByIdSeek("dst", Set(), 630950)
        .nodeByIdSeek("src", Set(), 359340)
        .build()
    )
  }

  test("should use id seek for unwound parameters") {
    val query =
      """UNWIND $ids as theId
        |MATCH (m:A)
        |WHERE id(m) = theId
        |RETURN m""".stripMargin

    val planner = plannerBuilder()
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("m")
        .filter("m:A")
        .apply()
        .|.nodeByIdSeek("m", Set("theId"), "theId")
        .unwind("$ids AS theId")
        .argument()
        .build()
    )
  }

  test("should use id seek for parameters") {
    val query =
      """MATCH (m:A)
        |WHERE id(m) in $ids
        |RETURN m""".stripMargin

    val planner = plannerBuilder()
      .build()

    planner.plan(query) should equal(
      planner.planBuilder()
        .produceResults("m")
        .filter("m:A")
        .nodeByIdSeek("m", Set.empty, parameter("ids", CTAny))
        .build()
    )
  }
}
