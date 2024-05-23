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
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.graphdb.schema.IndexType.RANGE
import org.neo4j.graphdb.schema.IndexType.TEXT

class IndexWithValuesPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport {

  private def nodeIndexConfig(
    isUnique: Boolean = false,
    isComposite: Boolean = false,
    existenceConstraints: Boolean = false,
    indexType: IndexType = RANGE
  ): StatisticsBackedLogicalPlanningConfiguration = {
    val props = if (isComposite) Seq("prop1", "prop2") else Seq("prop1")
    var b = plannerBuilder()
      .setAllNodesCardinality(2000)
      .setLabelCardinality("Awesome", 1000)
      .addNodeIndex(
        "Awesome",
        props,
        existsSelectivity = 1,
        uniqueSelectivity = 0.001,
        isUnique = isUnique,
        indexType = indexType
      )
      .setRelationshipCardinality("()-[]->()", 5000)
      .setRelationshipCardinality("(:Awesome)-[]->()", 5000)
      .setRelationshipCardinality("()-[]->(:Awesome)", 5000)

    if (existenceConstraints) {
      props.foreach { p =>
        b = b.addNodeExistenceConstraint("Awesome", p)
      }
    }

    b.build()
  }

  private def twoNodeIndexesConfig(): StatisticsBackedLogicalPlanningConfiguration = plannerBuilder()
    .setAllNodesCardinality(2000)
    .setLabelCardinality("Awesome", 1000)
    .addNodeIndex("Awesome", Seq("prop1"), existsSelectivity = 1, uniqueSelectivity = 0.001)
    .addNodeIndex("Awesome", Seq("prop2"), existsSelectivity = 1, uniqueSelectivity = 0.001)
    .build()

  private def relIndexConfig(
    isUnique: Boolean = false,
    isComposite: Boolean = false,
    existenceConstraints: Boolean = false,
    indexType: IndexType = RANGE
  ): StatisticsBackedLogicalPlanningConfiguration = {
    val props = if (isComposite) Seq("prop1", "prop2") else Seq("prop1")
    var b = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(10)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex(
        "REL",
        props,
        existsSelectivity = 1.0,
        uniqueSelectivity = 0.01,
        isUnique = isUnique,
        indexType = indexType
      )

    if (existenceConstraints) {
      props.foreach { p =>
        b = b.addRelationshipExistenceConstraint("REL", p)
      }
    }

    b.build()
  }

  // or planner between two indexes

  test("in an OR index plan should use cached values outside union for range predicates") {
    val planner = twoNodeIndexesConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 > 42 OR n.prop2 > 3 RETURN n.prop1, n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`", "cache[n.prop2] AS `n.prop2`")
      .cacheProperties("cache[n.prop1]", "cache[n.prop2] ")
      .distinct("n AS n")
      .union()
      .|.nodeIndexOperator("n:Awesome(prop2 > 3)", _ => GetValue)
      .nodeIndexOperator("n:Awesome(prop1 > 42)", _ => GetValue)
      .build())(SymmetricalLogicalPlanEquality)
  }

  test(
    "in an OR index plan should use cached values outside union for range predicates if they are on the same property"
  ) {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 > 42 OR n.prop1 < 3 RETURN n.prop1, n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`", "cache[n.prop2] AS `n.prop2`")
      .cacheProperties("cache[n.prop1]", "cacheFromStore[n.prop2] ")
      .distinct("n AS n")
      .union()
      .|.nodeIndexOperator("n:Awesome(prop1 > 42)", _ => GetValue)
      .nodeIndexOperator("n:Awesome(prop1 < 3)", _ => GetValue)
      .build())(SymmetricalLogicalPlanEquality)
  }

  test("in an OR index plan should use cached values outside union for equality predicates") {
    val planner = twoNodeIndexesConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 OR n.prop2 = 3 RETURN n.prop1, n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`", "cache[n.prop2] AS `n.prop2`")
      .cacheProperties("cache[n.prop1]", "cache[n.prop2] ")
      .distinct("n AS n")
      .union()
      .|.nodeIndexOperator("n:Awesome(prop2 = 3)", _ => GetValue)
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())(SymmetricalLogicalPlanEquality)

  }

  test("in an OR index plan with 4 indexes should get values for equality predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .setLabelCardinality("Awesome", 1000)
      .setLabelCardinality("Awesome2", 1000)
      .addNodeIndex("Awesome", Seq("prop1"), existsSelectivity = 1, uniqueSelectivity = 0.001)
      .addNodeIndex("Awesome", Seq("prop2"), existsSelectivity = 1, uniqueSelectivity = 0.001)
      .addNodeIndex("Awesome2", Seq("prop1"), existsSelectivity = 1, uniqueSelectivity = 0.001)
      .addNodeIndex("Awesome2", Seq("prop2"), existsSelectivity = 1, uniqueSelectivity = 0.001)
      .build()

    val plan = planner
      .plan("MATCH (n:Awesome:Awesome2) WHERE n.prop1 = 42 OR n.prop2 = 3 RETURN n.prop1, n.prop2")
      .stripProduceResults

    // We don't want to assert on the produce results or the projection in this test
    val Projection(unionPlan, _) = plan.asInstanceOf[Projection]

    unionPlan should (equal(planner.subPlanBuilder()
      .cacheProperties("cache[n.prop1]", "cache[n.prop2] ")
      .distinct("n AS n")
      .union()
      .|.filter("n:Awesome2")
      .|.nodeIndexOperator("n:Awesome(prop2 = 3)", _ => GetValue)
      .filter("n:Awesome2")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())(SymmetricalLogicalPlanEquality)
      or equal(planner.subPlanBuilder()
        .cacheProperties("cache[n.prop1]", "cache[n.prop2] ")
        .distinct("n AS n")
        .union()
        .|.filter("n:Awesome")
        .|.nodeIndexOperator("n:Awesome2(prop2 = 3)", _ => GetValue)
        .filter("n:Awesome")
        .nodeIndexOperator("n:Awesome2(prop1 = 42)", _ => GetValue)
        .build())(SymmetricalLogicalPlanEquality))
  }

  // Index exact seeks

  test("should plan index seek with GetValue when the property is projected") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())
  }

  test("should plan seek with GetValue when the relationship property is projected") {
    val planner = relIndexConfig()
    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 = 123 RETURN r.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.prop1] AS `r.prop1`")
      .relationshipIndexOperator("(a)-[r:REL(prop1 = 123)]-(b)", getValue = _ => GetValue)
      .build())
  }

  test("for exact seeks, should even plan index seek with GetValue when the index does not provide values") {
    val planner = nodeIndexConfig(indexType = TEXT)

    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = '42' RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator(
        "n:Awesome(prop1 = '42')",
        _ => GetValue,
        indexType = TEXT,
        supportPartitionedScan = false
      )
      .build())
  }

  test("should plan projection and index seek with DoNotGetValue when another property is projected") {
    val planner = twoNodeIndexesConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop2 AS `n.prop2`")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => DoNotGetValue)
      .build())
  }

  test("should plan projection and index seek with GetValue when two properties are projected") {
    val planner = twoNodeIndexesConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1, n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop2 AS `n.prop2`", "cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())
  }

  test("should plan index seek with GetValue when the property is projected after a renaming projection") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 WITH n AS m MATCH (m)-[r]-(o) RETURN m.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection(Map("m.prop1" -> cachedNodeProp("n", "prop1", "m")))
      .expandAll("(m)-[r]-(o)")
      .projection("n AS m")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())
  }

  test("should plan index seek with GetValue when the property is projected and renamed in a RETURN") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN n.prop1 AS foo")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS foo")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())
  }

  test("should plan index seek with GetValue when the property is projected and renamed in a WITH") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 WITH n.prop1 AS foo, true AS bar RETURN foo, bar AS baz")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("bar AS baz")
      .projection("cache[n.prop1] AS foo", "true AS bar")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())
  }

  test("should not be fooled to use a variable when the node variable is defined twice") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 WITH n AS m MATCH (m)-[r]-(n) RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop1 AS `n.prop1`")
      .expandAll("(m)-[r]-(n)")
      .projection("n AS m")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => DoNotGetValue)
      .build())
  }

  test(
    "should plan seek with DoNotGetValue when the a relationship property is projected, but from a different variable"
  ) {
    val query =
      """MATCH (a)-[r:REL]-(b)
        |WHERE r.prop1 = 123
        |WITH count(*) AS count
        |MATCH (a)-[r]-(b)
        |RETURN r.prop1""".stripMargin

    val planner = relIndexConfig()
    val plan = planner
      .plan(query)
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop1 AS `r.prop1`")
      .apply()
      .|.allRelationshipsScan("(a)-[r]-(b)", "count")
      .aggregation(Seq(), Seq("count(*) AS count"))
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 123)]-(b)",
        getValue = _ => DoNotGetValue,
        indexType = RANGE
      )
      .build())
  }

  test("should plan index seek with GetValue when the property is projected before the property access") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 WITH n MATCH (m)-[r]-(n) RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .expandAll("(n)-[r]-(m)")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())
  }

  test("should plan projection and index seek with GetValue when the property is projected inside of a function") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 'foo' RETURN toUpper(n.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("toUpper(cache[n.prop1]) AS `toUpper(n.prop1)`")
      .nodeIndexOperator("n:Awesome(prop1 = 'foo')", _ => GetValue)
      .build())
  }

  test("should plan projection and index seek with GetValue when the property is used in ORDER BY") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 'foo' RETURN n.foo ORDER BY toUpper(n.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.foo AS `n.foo`")
      .sort("`toUpper(n.prop1)` ASC")
      .projection("toUpper(cache[n.prop1]) AS `toUpper(n.prop1)`")
      .nodeIndexOperator("n:Awesome(prop1 = 'foo')", _ => GetValue, IndexOrderAscending)
      .build())
  }

  test("should plan index seek with GetValue when the property is part of an aggregating column") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN sum(n.prop1), n.foo AS nums")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq("n.foo AS nums"), Seq("sum(cache[n.prop1]) AS `sum(n.prop1)`"))
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
      .build())
  }

  test("should plan seek (relationship) with GetValue when the property is used in avg function") {
    val planner = relIndexConfig()
    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 = 123 RETURN avg(r.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("avg(cacheR[r.prop1]) AS `avg(r.prop1)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop1 = 123)]-(b)", getValue = _ => GetValue)
      .build())
  }

  test("should plan seek (relationship) with GetValue when the property is used in sum function") {
    val planner = relIndexConfig()

    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 = 123 RETURN sum(r.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("sum(cacheR[r.prop1]) AS `sum(r.prop1)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop1 = 123)]-(b)", getValue = _ => GetValue)
      .build())
  }

  test(
    "should plan projection and index seek with GetValue when the property is used in key column of an aggregation and in ORDER BY"
  ) {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 'foo' RETURN sum(n.foo), n.prop1 ORDER BY n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .orderedAggregation(Seq("cache[n.prop1] AS `n.prop1`"), Seq("sum(n.foo) AS `sum(n.foo)`"), Seq("cacheN[n.prop1]"))
      .nodeIndexOperator("n:Awesome(prop1 = 'foo')", _ => GetValue, IndexOrderAscending)
      .build())
  }

  test("should plan index seek with GetValue when the property is part of a distinct column") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 RETURN DISTINCT n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .orderedDistinct(Seq("cacheN[n.prop1]"), "cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue, IndexOrderAscending)
      .build())
  }

  test("should plan projection and index seek with GetValue when the property is used in an unwind projection") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 'foo' UNWIND [n.prop1] AS foo RETURN foo")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .unwind("[cache[n.prop1]] AS foo")
      .nodeIndexOperator("n:Awesome(prop1 = 'foo')", _ => GetValue)
      .build())
  }

  test("should plan projection and index seek with GetValue when the property is used in a procedure call") {
    val signature = ProcedureSignature(
      QualifiedName(Seq.empty, "fooProcedure"),
      IndexedSeq(FieldSignature("input", CTString)),
      Some(IndexedSeq(FieldSignature("value", CTString))),
      None,
      ProcedureReadOnlyAccess,
      id = 42
    )

    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .setLabelCardinality("Awesome", 1000)
      .addNodeIndex("Awesome", Seq("prop1"), existsSelectivity = 1, uniqueSelectivity = 0.001)
      .addProcedure(signature)
      .build()

    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 'foo' CALL fooProcedure(n.prop1) YIELD value RETURN value")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .procedureCall("fooProcedure(cache[n.prop1]) YIELD value")
      .nodeIndexOperator("n:Awesome(prop1 = 'foo')", _ => GetValue)
      .build())
  }

  // RANGE seek

  test("should plan projection and index seek with GetValue when another predicate uses the property") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 <= 42 AND n.prop1 % 2 = 0 RETURN n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop2 AS `n.prop2`")
      .filter("cache[n.prop1] % 2 = 0")
      .nodeIndexOperator("n:Awesome(prop1 <= 42)", _ => GetValue)
      .build())
  }

  test("should plan projection and index seek with GetValue when another predicate uses the property 2") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome)-[r]->(m) WHERE n.prop1 <= 42 AND n.prop1 % m.prop2 = 0 RETURN n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop2 AS `n.prop2`")
      .filter("cache[n.prop1] % m.prop2 = 0")
      .expandAll("(n)-[r]->(m)")
      .nodeIndexOperator("n:Awesome(prop1 <= 42)", _ => GetValue)
      .build())
  }

  test("should plan index seek with GetValue when the property is used in a predicate after a renaming projection") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 > 42 WITH n AS m MATCH (m)-[r]-(o) WHERE m.prop1 < 50 RETURN o")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .expandAll("(m)-[r]-(o)")
      .filterExpression(lessThan(cachedNodeProp("n", "prop1", "m"), literalInt(50)))
      .projection("n AS m")
      .nodeIndexOperator("n:Awesome(prop1 > 42)", _ => GetValue)
      .build())
  }

  test("should plan range seek with GetValue when the property is projected") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 > 'foo' RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 > 'foo')", _ => GetValue)
      .build())
  }

  test("should plan projection and range seek with DoNotGetValue when another property is projected") {
    val planner = twoNodeIndexesConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 > 'foo' RETURN n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop2 AS `n.prop2`")
      .nodeIndexOperator("n:Awesome(prop1 > 'foo')", _ => DoNotGetValue)
      .build())
  }

  test("should use cached access after projection of non returned property") {
    val planner = twoNodeIndexesConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 < 2 RETURN n.prop1 ORDER BY n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .sort("`n.prop2` ASC")
      .projection("n.prop2 AS `n.prop2`")
      .nodeIndexOperator("n:Awesome(prop1 < 2)", _ => GetValue)
      .build())
  }

  // RANGE seek on unique index

  test("should plan range seek with GetValue when the property is projected (unique index)") {
    val planner = nodeIndexConfig(isUnique = true)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 > 'foo' RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 > 'foo')", _ => GetValue, unique = true)
      .build())
  }

  test("should plan projection and range seek with DoNotGetValue when another property is projected (unique index)") {
    val planner = nodeIndexConfig(isUnique = true)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 > 'foo' RETURN n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop2 AS `n.prop2`")
      .nodeIndexOperator("n:Awesome(prop1 > 'foo')", _ => DoNotGetValue, unique = true)
      .build())
  }

  // seek on merge unique index

  test(
    "should plan seek with GetValue when the property is projected (merge unique index), but need a projection because of the Optional"
  ) {
    val planner = nodeIndexConfig(isUnique = true)
    val plan = planner
      .plan("MERGE (n:Awesome {prop1: 'foo'}) RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .merge(Seq(createNodeWithProperties("n", Seq("Awesome"), "{prop1: 'foo'}")))
      .nodeIndexOperator("n:Awesome(prop1 = 'foo')", _ => GetValue, unique = true)
      .build())
  }

  test(
    "for exact seeks, should even plan index seek with GetValue when the index does not provide values (merge unique index), but need a projection because of the Optional"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .setLabelCardinality("Awesome", 1000)
      .addNodeIndex(
        "Awesome",
        Seq("prop1"),
        existsSelectivity = 1,
        uniqueSelectivity = 0.001,
        indexType = TEXT,
        isUnique = true
      )
      .build()
    val plan = planner
      .plan("MERGE (n:Awesome {prop1: 'foo'}) RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .merge(Seq(createNodeWithProperties("n", Seq("Awesome"), "{prop1: 'foo'}")))
      .nodeIndexOperator(
        "n:Awesome(prop1 = 'foo')",
        _ => GetValue,
        unique = true,
        indexType = TEXT,
        supportPartitionedScan = false
      )
      .build())
  }

  test(
    "should plan projection and range seek with DoNotGetValue when another property is projected (merge unique index)"
  ) {
    val planner = nodeIndexConfig(isUnique = true)
    val plan = planner
      .plan("MERGE (n:Awesome {prop1: 'foo'}) RETURN n.foo")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.foo AS `n.foo`")
      .merge(Seq(createNodeWithProperties("n", Seq("Awesome"), "{prop1: 'foo'}")))
      .nodeIndexOperator("n:Awesome(prop1 = 'foo')", _ => DoNotGetValue, unique = true)
      .build())
  }

  // STARTS WITH seek

  test("should plan starts with seek with GetValue when the property is projected") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 STARTS WITH 'foo' RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 STARTS WITH 'foo')", _ => GetValue)
      .build())
  }

  test("should plan projection and starts with seek with DoNotGetValue when the index does not provide values") {
    val planner = nodeIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 STARTS WITH 'foo' RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop1 AS `n.prop1`")
      .nodeIndexOperator(
        "n:Awesome(prop1 STARTS WITH 'foo')",
        _ => DoNotGetValue,
        indexType = TEXT,
        supportPartitionedScan = false
      )
      .build())
  }

  test("should plan starts with seek with DoNotGetValue when the relationship index does not provide values") {
    val planner = relIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 STARTS WITH '123' RETURN r.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop1 AS `r.prop1`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 STARTS WITH '123')]-(b)",
        getValue = _ => DoNotGetValue,
        indexType = TEXT,
        supportPartitionedScan = false
      )
      .build())
  }

  test("should plan projection and starts with seek with DoNotGetValue when another property is projected") {
    val planner = twoNodeIndexesConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 STARTS WITH 'foo' RETURN n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop2 AS `n.prop2`")
      .nodeIndexOperator("n:Awesome(prop1 STARTS WITH 'foo')", _ => DoNotGetValue)
      .build())
  }

  // CONTAINS scan

  test("should plan projection and index contains scan with DoNotGetValue when the index does not provide values") {
    val planner = nodeIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 CONTAINS 'foo' RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop1 AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 CONTAINS 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT)
      .build())
  }

  test("should plan projection and index contains scan with DoNotGetValue when another property is projected") {
    val planner = nodeIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 CONTAINS 'foo' RETURN n.foo")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.foo AS `n.foo`")
      .nodeIndexOperator("n:Awesome(prop1 CONTAINS 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT)
      .build())
  }

  test(
    "should plan relationship projection and index contains scan with DoNotGetValue when the index does not provide values"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop CONTAINS 'foo' RETURN r.prop")
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop CONTAINS 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  test(
    "should plan relationship projection and index contains scan with DoNotGetValue when another property is projected"
  ) {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setAllRelationshipsCardinality(100)
      .setRelationshipCardinality("()-[:REL]-()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 0.01, indexType = IndexType.TEXT)
      .build()

    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop CONTAINS 'foo' RETURN r.foo")
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .projection("r.foo AS `r.foo`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop CONTAINS 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  // ENDS WITH scan

  test("should plan index ends with scan when the property is projected") {
    val planner = nodeIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 ENDS WITH 'foo' RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.prop1 AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1 ENDS WITH 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT)
      .build())
  }

  test("should plan projection and index ends with scan with DoNotGetValue when another property is projected") {
    val planner = nodeIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 ENDS WITH 'foo' RETURN n.foo")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.foo AS `n.foo`")
      .nodeIndexOperator("n:Awesome(prop1 ENDS WITH 'foo')", _ => DoNotGetValue, indexType = IndexType.TEXT)
      .build())
  }

  test(
    "should plan relationship projection and index ends with scan with DoNotGetValue when the index does not provide values"
  ) {
    val planner = relIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 ENDS WITH 'foo' RETURN r.prop1")
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .projection("r.prop1 AS `r.prop1`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop1 ENDS WITH 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  test(
    "should plan relationship projection and index ends with scan with DoNotGetValue when another property is projected"
  ) {
    val planner = relIndexConfig(indexType = TEXT)
    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 ENDS WITH 'foo' RETURN r.foo")
      .stripProduceResults

    plan should equal(
      planner.subPlanBuilder()
        .projection("r.foo AS `r.foo`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop1 ENDS WITH 'foo')]-(b)",
          indexOrder = IndexOrderNone,
          argumentIds = Set(),
          getValue = _ => DoNotGetValue,
          indexType = IndexType.TEXT
        )
        .build()
    )
  }

  // IS NOT NULL

  test("should plan scan scan with GetValue when the property is projected") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 IS NOT NULL RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1)", _ => GetValue)
      .build())
  }

  test(s"should plan scan scan with GetValue when the relationship property is projected") {
    val planner = relIndexConfig()
    val plan = planner
      .plan(s"MATCH (a)-[r:REL]-(b) WHERE r.prop1 IS NOT NULL RETURN r.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.prop1] AS `r.prop1`")
      .relationshipIndexOperator("(a)-[r:REL(prop1)]-(b)", getValue = _ => GetValue)
      .build())
  }

  test(
    s"should plan scan scan with DoNotGetValue when the a relationship property is projected, but from a different variable"
  ) {
    val planner = relIndexConfig()
    val plan = planner
      .plan(s"MATCH (a)-[r:REL]-(b) WHERE r.prop1 IS NOT NULL WITH count(*) AS count MATCH (a)-[r]-(b) RETURN r.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("r.prop1 AS `r.prop1`")
      .apply()
      .|.allRelationshipsScan("(a)-[r]-(b)", "count")
      .aggregation(Seq(), Seq("count(*) AS count"))
      .relationshipIndexOperator("(a)-[r:REL(prop1)]-(b)", getValue = _ => DoNotGetValue)
      .build())
  }

  // composite index

  test("should plan index seek with GetValue when the property is projected (composite index)") {
    val planner = nodeIndexConfig(isComposite = true)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 AND n.prop2 = 21 RETURN n.prop1, n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`", "cache[n.prop2] AS `n.prop2`")
      .nodeIndexOperator("n:Awesome(prop1 = 42, prop2 = 21)", _ => GetValue, supportPartitionedScan = false)
      .build())
  }

  test(
    "should plan projection and index seek with DoNotGetValue when another property is projected (composite index)"
  ) {
    val planner = nodeIndexConfig(isComposite = true)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 AND n.prop2 = 21 RETURN n.bar")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("n.bar AS `n.bar`")
      .nodeIndexOperator("n:Awesome(prop1 = 42, prop2 = 21)", _ => DoNotGetValue, supportPartitionedScan = false)
      .build())
  }

  test("should plan index seek with GetValue and DoNotGetValue when only one property is projected (composite index)") {
    val planner = nodeIndexConfig(isComposite = true)
    val plan = planner
      .plan("MATCH (n:Awesome) WHERE n.prop1 = 42 AND n.prop2 = 21 RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator(
        "n:Awesome(prop1 = 42, prop2 = 21)",
        Map("prop1" -> GetValue, "prop2" -> DoNotGetValue),
        supportPartitionedScan = false
      )
      .build())
  }

  test("should plan relationship index seek with GetValue when the property is projected (composite index)") {
    val planner = relIndexConfig(isComposite = true)
    val plan = planner
      .plan("MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 21 RETURN r.prop1, r.prop2")
      .stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .projection("cacheR[r.prop1] AS `r.prop1`", "cacheR[r.prop2] AS `r.prop2`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 42, prop2 = 21)]->(b)",
        getValue = _ => GetValue,
        indexType = RANGE,
        supportPartitionedScan = false
      )
      .build()
  }

  test(
    "should plan projection and index seek with DoNotGetValue when another property is projected (composite relationship index)"
  ) {
    val planner = relIndexConfig(isComposite = true)
    val plan = planner
      .plan("MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 21 RETURN r.otherProp")
      .stripProduceResults

    plan shouldBe planner.subPlanBuilder()
      .projection("r.otherProp AS `r.otherProp`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 42, prop2 = 21)]->(b)",
        getValue = _ => DoNotGetValue,
        indexType = RANGE,
        supportPartitionedScan = false
      )
      .build()
  }

  test(
    "should plan relationship index seek with GetValue and DoNotGetValue when only one property is projected (composite index)"
  ) {
    val planner = relIndexConfig(isComposite = true)
    val plan = planner
      .plan("MATCH (a)-[r:REL]->(b) WHERE r.prop1 = 42 AND r.prop2 = 21 RETURN r.prop2")
      .stripProduceResults

    plan.leftmostLeaf should matchPattern {
      case d: DirectedRelationshipIndexSeek
        if d.properties.map(p => p.propertyKeyToken.name -> p.getValueFromIndex) ==
          Seq("prop1" -> DoNotGetValue, "prop2" -> GetValue) => ()
    }
  }

  // EXISTENCE / NODE KEY CONSTRAINT

  test("should plan scan with GetValue when existence constraint on projected property") {
    val planner = nodeIndexConfig(existenceConstraints = true)
    val plan = planner
      .plan("MATCH (n:Awesome) RETURN n.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop1] AS `n.prop1`")
      .nodeIndexOperator("n:Awesome(prop1)", _ => GetValue)
      .build())
  }

  test("should plan scan (relationship) with GetValue when existence constraint on projected property") {
    val planner = relIndexConfig(existenceConstraints = true)
    val plan = planner
      .plan(s"MATCH (a)-[r:REL]-(b) RETURN r.prop1")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.prop1] AS `r.prop1`")
      .relationshipIndexOperator("(a)-[r:REL(prop1)]-(b)", getValue = _ => GetValue)
      .build())
  }

  test("should plan scan with GetValue when composite existence constraint on projected property") {
    val planner = nodeIndexConfig(existenceConstraints = true, isComposite = true)
    val plan = planner
      .plan("MATCH (n:Awesome) RETURN n.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cache[n.prop2] AS `n.prop2`")
      .nodeIndexOperator("n:Awesome(prop1, prop2)", Map("prop1" -> DoNotGetValue, "prop2" -> GetValue))
      .build())
  }

  test("should plan scan (relationship) with GetValue when composite existence constraint on projected property") {
    val planner = relIndexConfig(existenceConstraints = true, isComposite = true)
    val plan = planner
      .plan(s"MATCH (a)-[r:REL]-(b) RETURN r.prop2")
      .stripProduceResults

    withClue("Index scan on two properties should be planned if they are only available through constraint") {
      plan should equal(planner.subPlanBuilder()
        .projection("cacheR[r.prop2] AS `r.prop2`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop1, prop2)]-(b)",
          getValue = Map("prop1" -> DoNotGetValue, "prop2" -> GetValue),
          indexType = RANGE
        )
        .build())
    }
  }

  test("should plan scan (node) with GetValue when composite existence constraint on projected property") {
    val planner = nodeIndexConfig(existenceConstraints = true, isComposite = true)
    val plan = planner
      .plan(s"MATCH (a:Awesome) RETURN a.prop2")
      .stripProduceResults

    withClue("Index scan on two properties should be planned if they are only available through constraint") {
      plan should equal(planner.subPlanBuilder()
        .projection("cacheN[a.prop2] AS `a.prop2`")
        .nodeIndexOperator(
          "a:Awesome(prop1, prop2)",
          getValue = Map("prop1" -> DoNotGetValue, "prop2" -> GetValue)
        )
        .build())
    }
  }

  test("should plan seek (relationship) with GetValue when composite existence constraint on projected property") {
    val planner = relIndexConfig(existenceConstraints = true, isComposite = true)
    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 > 123 RETURN r.prop2")
      .stripProduceResults

    withClue("Index seek on two properties should be planned if they are only available through constraint") {
      plan shouldBe planner.subPlanBuilder()
        .projection("cacheR[r.prop2] AS `r.prop2`")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop1 > 123, prop2)]-(b)",
          getValue = Map("prop1" -> DoNotGetValue, "prop2" -> GetValue),
          supportPartitionedScan = false
        )
        .build()
    }
  }

  test(
    "should plan seek (relationship) with GetValue and DoNotGetValue when composite existence constraint"
  ) {
    val planner = relIndexConfig(existenceConstraints = true, isComposite = true)
    val plan = planner
      .plan("MATCH (a)-[r:REL]-(b) WHERE r.prop1 = 123 RETURN r.prop2")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .projection("cacheR[r.prop2] AS `r.prop2`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop1 = 123, prop2)]-(b)",
        getValue = Map("prop1" -> DoNotGetValue, "prop2" -> GetValue),
        supportPartitionedScan = false
      )
      .build())
  }

  // AGGREGATIONS (=> implicit exists)

  test("should plan scan with GetValue when the property is used in avg function") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) RETURN avg(n.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("avg(cache[n.prop1]) AS `avg(n.prop1)`"))
      .nodeIndexOperator("n:Awesome(prop1)", _ => GetValue)
      .build())
  }

  test("should plan scan (relationship) with GetValue when the property is used in avg function") {
    val planner = relIndexConfig()
    val plan = planner
      .plan(s"MATCH (a)-[r:REL]-(b) RETURN avg(r.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("avg(cacheR[r.prop1]) AS `avg(r.prop1)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop1)]-(b)", getValue = _ => GetValue)
      .build())
  }

  test("should plan scan with GetValue when the property is used in sum function") {
    val planner = nodeIndexConfig()
    val plan = planner
      .plan("MATCH (n:Awesome) RETURN sum(n.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("sum(cache[n.prop1]) AS `sum(n.prop1)`"))
      .nodeIndexOperator("n:Awesome(prop1)", _ => GetValue)
      .build())
  }

  test("should plan scan (relationship) with GetValue when the property is used in sum function") {
    val planner = relIndexConfig()
    val plan = planner
      .plan(s"MATCH (a)-[r:REL]-(b) RETURN sum(r.prop1)")
      .stripProduceResults

    plan should equal(planner.subPlanBuilder()
      .aggregation(Seq(), Seq("sum(cacheR[r.prop1]) AS `sum(r.prop1)`"))
      .relationshipIndexOperator("(a)-[r:REL(prop1)]-(b)", getValue = _ => GetValue)
      .build())
  }

  // other tests

  test("should plan an index scan in parallel runtime when property is accessed in ORDER BY") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Question", 1000)
      .addNodeIndex("Question", Seq("views"), existsSelectivity = 1, uniqueSelectivity = 0.1)
      .addNodeExistenceConstraint("Question", "views")
      .setExecutionModel(ExecutionModel.BatchedParallel(128, 1024))
      .build()

    val q =
      """
        |MATCH (q:Question)
        |WITH q ORDER BY q.views DESC
        |LIMIT 1000
        |RETURN q.name AS tag, count(*) AS count
        |ORDER BY count DESC
        |LIMIT 10
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .top(10, "count DESC")
      .aggregation(Seq("q.name AS tag"), Seq("count(*) AS count"))
      .top(1000, "`q.views` DESC")
      .projection("cacheN[q.views] AS `q.views`")
      .nodeIndexOperator("q:Question(views)", getValue = Map("views" -> GetValue))
      .build()
  }
}
