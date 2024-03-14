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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.internal.schema.constraints.SchemaValueType

class ResolveImplicitlySolvedPredicatesPlanningIntegrationTest
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanningAttributesTestSupport {

  test("nodeByLabelScan should implicitly solve .prop IS NOT NULL when existence constraint exists 1") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = cfg.plan("MATCH (a:A) WHERE a.prop IS NOT NULL RETURN a")
    plan shouldEqual cfg.planBuilder()
      .produceResults("a")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("nodeByLabelScan should implicitly solve .prop IS NOT NULL when existence constraint exists 2") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 500)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = cfg.plan("MATCH (a:A|B) WHERE a.prop IS NOT NULL RETURN a")
    plan shouldEqual cfg.planBuilder()
      .produceResults("a")
      .orderedDistinct(Seq("a"), "a AS a")
      .orderedUnion("a ASC")
      .|.filter("a.prop IS NOT NULL")
      .|.nodeByLabelScan("a", "B", IndexOrderAscending)
      .nodeByLabelScan("a", "A", IndexOrderAscending)
      .build()
  }

  test("nodeByLabelScan should implicitly solve .prop IS NOT NULL when existence constraint exists 3") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .setLabelCardinality("B", 500)
      .addNodeExistenceConstraint("A", "prop")
      .build()

    val plan = cfg.plan("MATCH (a:A&B) WHERE a.prop IS NOT NULL RETURN a")
    plan shouldEqual cfg.planBuilder()
      .produceResults("a")
      .filter("a:B")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("relationshipTypeScan should implicitly solve .prop IS NOT NULL when existence constraint exists 1") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(500)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = cfg.plan("MATCH (a)-[r]->(b) WHERE r:REL AND r.prop IS NOT NULL RETURN r")
    plan shouldEqual cfg.planBuilder()
      .produceResults("r")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("nodeByLabelScan should implicitly solve .prop isTyped when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodePropertyTypeConstraint("A", "prop", SchemaValueType.STRING)
      .build()

    val plan = cfg.plan("MATCH (a:A) WHERE a.prop :: String RETURN a")
    plan shouldEqual cfg.planBuilder()
      .produceResults("a")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("relationshipByTypeScan should implicitly solve .prop isTyped when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 50)
      .addRelationshipPropertyTypeConstraint("REL", "prop", SchemaValueType.DATE)
      .build()

    val plan = cfg.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop :: Date RETURN r")
    plan shouldEqual cfg.planBuilder()
      .produceResults("r")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("nodeByLabelScan should NOT implicitly solve .prop isTyped NOT NULL when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodePropertyTypeConstraint("A", "prop", SchemaValueType.STRING)
      .build()

    val plan = cfg.plan("MATCH (a:A) WHERE a.prop :: String NOT NULL RETURN a")
    plan shouldEqual cfg.planBuilder()
      .produceResults("a")
      .filter("a.prop IS :: STRING NOT NULL")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("relationshipByTypeScan should NOT implicitly solve .prop isTyped NOT NULL when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 50)
      .addRelationshipPropertyTypeConstraint("REL", "prop", SchemaValueType.DATE)
      .build()

    val plan = cfg.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop :: Date NOT NULL RETURN r")
    plan shouldEqual cfg.planBuilder()
      .produceResults("r")
      .filter("r.prop IS :: DATE NOT NULL")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }
}
