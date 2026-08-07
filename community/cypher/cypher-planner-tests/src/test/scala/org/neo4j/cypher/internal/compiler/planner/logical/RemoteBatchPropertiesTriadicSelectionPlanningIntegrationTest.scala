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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.compiler.ExecutionModel

class RemoteBatchPropertiesTriadicSelectionPlanningIntegrationTest
    extends AbstractRemoteBatchPropertiesPlanningIntegrationTest(ExecutionModel.default) {

  override protected val orderPreserving: Boolean = true

  private val triadicPlannerBase = spdPlanner
    .setAllNodesCardinality(20000)
    .setLabelCardinality("Person", 15000)
    .setRelationshipCardinality("()-[:IMPLIES_PERSON]->()", 10000)
    .setRelationshipCardinality("(:Person)-[:IMPLIES_PERSON]->()", 10000)
    .setRelationshipCardinality("()-[:IMPLIES_PERSON]->(:Person)", 10000)
    .setRelationshipCardinality("(:Person)-[:IMPLIES_PERSON]->(:Person)", 10000)

  test(
    "should plan triadic selection - case: no rbps on outerExpand and no rbps on innerExpand"
  ) {
    val query =
      """
        |MATCH (a:Person)-[r1:IMPLIES_PERSON]->(b:Person)-[r2:IMPLIES_PERSON]->(c:Person) USING SCAN a:Person
        |WHERE a.name = "a" AND NOT (a)-[:IMPLIES_PERSON]->(c)
        |RETURN a, b, c
        |""".stripMargin

    val triadicPlanner = triadicPlannerBase.build()
    val plan = triadicPlanner.plan(query).stripProduceResults
    plan shouldEqual triadicPlanner.subPlanBuilder()
      .filter("NOT r2 = r1", "c:Person")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2:IMPLIES_PERSON]->(c)")
      .|.argument("b", "r1")
      .filter("b:Person")
      .expandAll("(a)-[r1:IMPLIES_PERSON]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.name]")("a.name = 'a'")
      .nodeByLabelScan("a", "Person")
      .build()
  }

  test("should plan triadic selection - case: no rbps on outerExpand but rbps on innerExpand") {
    val query =
      """
        |MATCH (a:Person)-[r1:IMPLIES_PERSON]->(b:Person)-[r2:IMPLIES_PERSON]->(c:Person) USING SCAN a:Person
        |WHERE a.name = "a" AND c.name = "c" AND NOT (a)-[:IMPLIES_PERSON]->(c)
        |RETURN a, b, c
        |""".stripMargin

    val triadicPlanner = triadicPlannerBase.build()
    val plan = triadicPlanner.plan(query).stripProduceResults
    plan shouldEqual triadicPlanner.subPlanBuilder()
      .remoteBatchPropertiesWithFilter("cacheNFromStore[c.name]")("c.name = 'c'")
      .filter("NOT r2 = r1", "c:Person")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2:IMPLIES_PERSON]->(c)")
      .|.argument("b", "r1")
      .filter("b:Person")
      .expandAll("(a)-[r1:IMPLIES_PERSON]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.name]")("a.name = 'a'")
      .nodeByLabelScan("a", "Person")
      .build()
  }

  test("should plan triadic selection - case: rbps on outerExpand but no rbps on innerExpand") {
    val query =
      """
        |MATCH (a:Person)-[r1:IMPLIES_PERSON]->(b:Person)-[r2:IMPLIES_PERSON]->(c:Person) USING SCAN a:Person
        |WHERE a.name = "a" AND b.name IS NOT NULL AND NOT (a)-[:IMPLIES_PERSON]->(c)
        |RETURN a, b, c
        |""".stripMargin

    val triadicPlanner = triadicPlannerBase.build()
    val plan = triadicPlanner.plan(query).stripProduceResults
    plan shouldEqual triadicPlanner.subPlanBuilder()
      .filter("NOT r2 = r1", "c:Person")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.name]")("b.name IS NOT NULL")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2:IMPLIES_PERSON]->(c)")
      .|.argument("b", "r1")
      .filter("b:Person")
      .expandAll("(a)-[r1:IMPLIES_PERSON]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.name]")("a.name = 'a'")
      .nodeByLabelScan("a", "Person")
      .build()
  }

  test("should plan triadic selection - case: rbps on outerExpand and rbps on innerExpand") {
    val query =
      """
        |MATCH (a:Person)-[r1:IMPLIES_PERSON]->(b:Person)-[r2:IMPLIES_PERSON]->(c:Person) USING SCAN a:Person
        |WHERE a.name = "a" AND b.name IS NOT NULL AND c.name = "c" AND NOT (a)-[:IMPLIES_PERSON]->(c)
        |RETURN a, b, c
        |""".stripMargin

    val triadicPlanner = triadicPlannerBase
      //      .withSetting(GraphDatabaseInternalSettings.planning_selector_candidates_maximum, java.lang.Integer.valueOf(4))
      .build()
    val plan = triadicPlanner.plan(query).stripProduceResults
    plan shouldEqual triadicPlanner.subPlanBuilder()
      .remoteBatchPropertiesWithFilter("cacheNFromStore[c.name]")("c.name = 'c'")
      .filter(
        "NOT r2 = r1",
        "c:Person"
      ) // This will on the rhs of the triadicSelection with planning_selector_candidates_maximum set to at least 4 (see the test below)
      .remoteBatchPropertiesWithFilter("cacheNFromStore[b.name]")(
        "b.name IS NOT NULL"
      ) // will be placed on top with planning_selector_candidates_maximum set to at least 4 (see the test below)
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.expandAll("(b)-[r2:IMPLIES_PERSON]->(c)")
      .|.argument("b", "r1")
      .filter("b:Person")
      .expandAll("(a)-[r1:IMPLIES_PERSON]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.name]")("a.name = 'a'")
      .nodeByLabelScan("a", "Person")
      .build()
  }

  test(
    "should plan triadic selection - case: rbps on outerExpand and rbps on innerExpand - planning_selector_candidates_maximum=4"
  ) {
    val query =
      """
        |MATCH (a:Person)-[r1:IMPLIES_PERSON]->(b:Person)-[r2:IMPLIES_PERSON]->(c:Person) USING SCAN a:Person
        |WHERE a.name = "a" AND b.name IS NOT NULL AND c.name = "c" AND NOT (a)-[:IMPLIES_PERSON]->(c)
        |RETURN a, b, c
        |""".stripMargin

    val triadicPlanner = triadicPlannerBase
      .withSetting(GraphDatabaseInternalSettings.planning_selector_candidates_maximum, java.lang.Integer.valueOf(4))
      .build()
    val plan = triadicPlanner.plan(query).stripProduceResults
    plan shouldEqual triadicPlanner.subPlanBuilder()
      .filter("cacheN[b.name] IS NOT NULL")
      .remoteBatchProperties("cacheNFromStore[b.name]")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[c.name]")("c.name = 'c'")
      .triadicSelection(positivePredicate = false, "a", "b", "c")
      .|.filter("NOT r2 = r1", "c:Person")
      .|.expandAll("(b)-[r2:IMPLIES_PERSON]->(c)")
      .|.argument("b", "r1")
      .filter("b:Person")
      .expandAll("(a)-[r1:IMPLIES_PERSON]->(b)")
      .remoteBatchPropertiesWithFilter("cacheNFromStore[a.name]")("a.name = 'a'")
      .nodeByLabelScan("a", "Person")
      .build()
  }
}
