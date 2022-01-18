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
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.IndexType

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class IndexPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningIntegrationTestSupport with AstConstructionTestSupport {

  private def plannerBaseConfigForIndexOnLabelPropTests(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 100)
      .setRelationshipCardinality("()-[]->(:Label)", 50)
      .setRelationshipCardinality("(:Label)-[]->()", 50)
      .setRelationshipCardinality("()-[]->()", 50)

  private def plannerConfigForBtreeIndexOnLabelPropTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForIndexOnLabelPropTests()
      .addNodeIndex("Label", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .build()

  private def plannerConfigForRangeIndexOnLabelPropTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForIndexOnLabelPropTests()
      .addNodeIndex("Label", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
      .enablePlanningRangeIndexes()
      .build()

  private def plannerConfigForRangeIndexOnRelationshipTypePropTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForIndexOnLabelPropTests()
      .setRelationshipCardinality("()-[:Type]->()", 20)
      .addRelationshipIndex("Type", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
      .enablePlanningRangeIndexes()
      .build()

  test("should not plan btree index seek if predicate depends on variable from same QueryGraph") {
    val cfg = plannerConfigForBtreeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a)-[r]->(b:Label) WHERE b.prop $op a.prop RETURN a").stripProduceResults

      val planWithLabelScan = cfg.subPlanBuilder()
        .filter(s"b.prop $op a.prop")
        .expandAll("(b)<-[r]-(a)")
        .nodeByLabelScan("b", "Label")
        .build()

      val planWithIndexScan = cfg.subPlanBuilder()
        .filter(s"b.prop $op a.prop")
        .expandAll("(b)<-[r]-(a)")
        .nodeIndexOperator("b:Label(prop)", indexType = IndexType.BTREE)
        .build()

      plan should (be(planWithIndexScan) or be(planWithLabelScan))
    }
  }

  test("should plan btree index usage if predicate depends on simple variable from horizon") {
    val cfg = plannerConfigForBtreeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"WITH 'foo' AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .apply()
        .|.nodeIndexOperator(s"b:Label(prop $op ???)", paramExpr = Some(varFor("foo")), argumentIds = Set("foo"), indexType = IndexType.BTREE)
        .projection("'foo' AS foo")
        .argument()
        .build()
    }
  }

  test("should plan btree index usage if predicate depends on property of variable from horizon") {
    val cfg = plannerConfigForBtreeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"WITH {prop: 'foo'} AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo.prop RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .apply()
        .|.nodeIndexOperator(s"b:Label(prop $op ???)", paramExpr = Some(prop("foo", "prop")) , argumentIds = Set("foo"), indexType = IndexType.BTREE)
        .projection("{prop: 'foo'} AS foo")
        .argument()
        .build()
    }
  }

  test("should prefer range index over btree index if both are available") {
    val cfg =
      plannerBaseConfigForIndexOnLabelPropTests()
        .addNodeIndex("Label", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
        .addNodeIndex("Label", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
        .enablePlanningRangeIndexes()
        .build()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan = cfg.plan(s"MATCH (a:Label) WHERE a.prop $op 'test' RETURN a").stripProduceResults

      plan shouldEqual cfg.subPlanBuilder()
        .nodeIndexOperator(s"a:Label(prop $op 'test')", indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should not plan range index if feature flag is disabled") {
    val cfg =
      plannerBaseConfigForIndexOnLabelPropTests()
        .enablePlanningRangeIndexes(false)
        .addNodeIndex("Label", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
        .build()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan = cfg.plan(s"MATCH (a:Label) WHERE a.prop $op 'test' RETURN a").stripProduceResults

      plan shouldEqual cfg.subPlanBuilder()
        .filter(s"a.prop $op 'test'")
        .nodeByLabelScan("a", "Label", IndexOrderNone)
        .build()
    }
  }

  test("should plan range index scan for partial existence predicate if predicate is for points") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (predicate <- List("point.withinBBox(a.prop, point({x:1, y:1}), point({x:2, y:2}))", "point.distance(a.prop, point({x:1, y:1})) < 10")) {
      val plan = cfg.plan(s"MATCH (a:Label) WHERE $predicate RETURN a").stripProduceResults

      plan shouldEqual cfg.subPlanBuilder()
        .filter(predicate)
        .nodeIndexOperator("a:Label(prop)", indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan range index node scan for existence predicate") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    val plan = cfg.plan(s"MATCH (a:Label) WHERE a.prop IS NOT NULL RETURN a").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .nodeIndexOperator("a:Label(prop)", indexType = IndexType.RANGE)
      .build()
  }

  test("should plan range index relationship scan for existence predicate") {
    val cfg = plannerConfigForRangeIndexOnRelationshipTypePropTests()

    val plan = cfg.plan(s"MATCH (a)-[r:Type]->(b) WHERE r.prop IS NOT NULL RETURN r").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .relationshipIndexOperator("(a)-[r:Type(prop)]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test("should plan composite range node index") {
    val cfg = plannerBaseConfigForIndexOnLabelPropTests()
      .addNodeIndex("Label", Seq("prop1", "prop2"), 0.1, 0.01, indexType = IndexType.RANGE)
      .enablePlanningRangeIndexes()
      .build()

    // seekable first predicate
    for (op1 <- List("=")) {
      for (op2 <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
        val query = s"MATCH (a:Label) WHERE a.prop1 $op1 1 AND a.prop2 $op2 2 RETURN a"

        withClue(s"Failed planning range index for: '$query'") {
          val plan = cfg.plan(query)

          plan shouldEqual cfg.planBuilder()
            .produceResults("a")
            .nodeIndexOperator(s"a:Label(prop1 $op1 1, prop2 $op2 2)", indexType = IndexType.RANGE)
            .build()
        }
      }
    }

    // scannable first predicate
    for (op1 <- List("<", "<=", ">", ">=", "STARTS WITH")) {
      for (op2 <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
        val query = s"MATCH (a:Label) WHERE a.prop1 $op1 1 AND a.prop2 $op2 2 RETURN a"

        withClue(s"Failed planning range index for: '$query'") {
          val plan = cfg.plan(query)

          plan shouldEqual cfg.planBuilder()
            .produceResults("a")
            .filter(s"a.prop2 $op2 2")
            .nodeIndexOperator(s"a:Label(prop1 $op1 1, prop2)", indexType = IndexType.RANGE)
            .build()
        }
      }
    }
  }

  test("should plan composite range relationship index") {
    val cfg = plannerBaseConfigForIndexOnLabelPropTests()
      .addRelationshipIndex("Type", Seq("prop1", "prop2"), 0.1, 0.01, indexType = IndexType.RANGE)
      .setRelationshipCardinality("()-[:Type]->()", 20)
      .enablePlanningRangeIndexes()
      .build()

    // seekable first predicate
    for (op1 <- List("=")) {
      for (op2 <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
        val query = s"MATCH (a)-[r:Type]->(b) WHERE r.prop1 $op1 1 AND r.prop2 $op2 2 RETURN r"

        withClue(s"Failed planning range index for: '$query'") {
          val plan = cfg.plan(query)

          plan shouldEqual cfg.planBuilder()
            .produceResults("r")
            .relationshipIndexOperator(s"(a)-[r:Type(prop1 $op1 1, prop2 $op2 2)]->(b)", indexType = IndexType.RANGE)
            .build()
        }
      }
    }

    // scannable first predicate
    for (op1 <- List("<", "<=", ">", ">=", "STARTS WITH")) {
      for (op2 <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
        val query = s"MATCH (a)-[r:Type]->(b) WHERE r.prop1 $op1 1 AND r.prop2 $op2 2 RETURN r"

        withClue(s"Failed planning range index for: '$query'") {
          val plan = cfg.plan(query)

          plan shouldEqual cfg.planBuilder()
            .produceResults("r")
            .filter(s"r.prop2 $op2 2")
            .relationshipIndexOperator(s"(a)-[r:Type(prop1 $op1 1, prop2)]->(b)", indexType = IndexType.RANGE)
            .build()
        }
      }
    }
  }

  test("should use range index if value to compare with is of type point") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=")) {
      val plan = cfg.plan(
        s"""WITH point({x:1, y:1}) AS point
           |MATCH (a:Label)
           |WHERE a.prop $op point
           |RETURN a""".stripMargin).stripProduceResults

      plan shouldEqual cfg.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator(s"a:Label(prop $op point)", argumentIds = Set("point"), indexType = IndexType.RANGE)
        .projection("point({x: 1, y: 1}) AS point")
        .argument()
        .build()
    }
  }

  test("should not plan range index seek if predicate depends on variable from same QueryGraph") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan = cfg.plan(s"MATCH (a)-[r]->(b:Label) WHERE b.prop $op a.prop RETURN a").stripProduceResults

      val planWithLabelScan = cfg.subPlanBuilder()
        .filter(s"b.prop $op a.prop")
        .expandAll("(b)<-[r]-(a)")
        .nodeByLabelScan("b", "Label")
        .build()

      val planWithIndexScan = cfg.subPlanBuilder()
        .filter(s"b.prop $op a.prop")
        .expandAll("(b)<-[r]-(a)")
        .nodeIndexOperator("b:Label(prop)", indexType = IndexType.RANGE)
        .build()

      plan should be(planWithIndexScan)
    }
  }

  test("should plan range index usage if predicate depends on simple variable from horizon") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan = cfg.plan(s"WITH 'foo' AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .apply()
        .|.nodeIndexOperator(s"b:Label(prop $op ???)", paramExpr = Some(varFor("foo")), argumentIds = Set("foo"), indexType = IndexType.RANGE)
        .projection("'foo' AS foo")
        .argument()
        .build()
    }
  }

  test("should plan range index usage if predicate depends on property of variable from horizon") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan = cfg.plan(s"WITH {prop: 'foo'} AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo.prop RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .apply()
        .|.nodeIndexOperator(s"b:Label(prop $op ???)", paramExpr = Some(prop("foo", "prop")) , argumentIds = Set("foo"), indexType = IndexType.RANGE)
        .projection("{prop: 'foo'} AS foo")
        .argument()
        .build()
    }
  }

  test("should plan range index scan for partial existence predicate for string comparison predicates") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a)-[r]->(b:Label) WHERE b.prop $op 'test' RETURN a").stripProduceResults

      val planWithLabelScan = cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .filter(s"b.prop $op 'test'")
        .nodeIndexOperator("b:Label(prop)", indexType = IndexType.RANGE)
        .build()

      plan should be(planWithLabelScan)
    }
  }

  private def plannerBaseConfigForDistancePredicateTests(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    plannerBuilder()
    .setAllNodesCardinality(100)
    .setLabelCardinality("Place", 50)
    .setLabelCardinality("Preference", 50)
    .setRelationshipCardinality("(:Place)-[]->()", 20)
    .setRelationshipCardinality("(:Place)-[]->(:Preference)", 20)
    .setRelationshipCardinality("()-[]->(:Preference)", 20)

  private def plannerConfigWithBtreeIndexForDistancePredicateTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForDistancePredicateTests()
      .addNodeIndex("Place", Seq("location"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .build()

  private def plannerConfigWithRangeIndexForDistancePredicateTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForDistancePredicateTests()
      .addNodeIndex("Place", Seq("location"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
      .enablePlanningRangeIndexes()
      .build()

  test("should plan range index scan for partial existence predicate for distance predicate") {
    val query =
      """WITH 10 AS maxDistance
        |MATCH (p:Place)
        |WHERE point.distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance
        |RETURN p.location as point
        """.stripMargin

    val cfg = plannerConfigWithRangeIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter("point.distance(cacheNFromStore[p.location], point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance")
      .projection("10 AS maxDistance")
      .nodeIndexOperator("p:Place(location)", indexType = IndexType.RANGE)
      .build()
  }

  test("should not plan index usage if distance predicate depends on variable from same QueryGraph") {
    val query =
      """MATCH (p:Place)-[r]->(x:Preference)
        |WHERE point.distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
        |RETURN p.location as point
        """.stripMargin

    val cfg = plannerConfigWithBtreeIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter("x.maxDistance >= point.distance(cacheNFromStore[p.location], point({x: 0, y: 0, crs: 'cartesian'}))", "x:Preference")
      .expandAll("(p)-[r]->(x)")
      .nodeByLabelScan("p", "Place")
      .build()
  }

  test("should plan index usage if distance predicate depends on variable from the horizon") {
    val query =
      """WITH 10 AS maxDistance
        |MATCH (p:Place)
        |WHERE point.distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance
        |RETURN p.location as point
        """.stripMargin

    val cfg = plannerConfigWithBtreeIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter("point.distance(cacheNFromStore[p.location], point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance")
      .apply()
      .|.pointDistanceNodeIndexSeekExpr("p", "Place", "location", "{x: 0, y: 0, crs: 'cartesian'}", distanceExpr = varFor("maxDistance"), argumentIds = Set("maxDistance"), inclusive = true)
      .projection("10 AS maxDistance")
      .argument()
      .build()
  }

  test("should plan index usage if distance predicate depends on property read of variable from the horizon") {
    val query =
      """WITH {maxDistance: 10} AS x
        |MATCH (p:Place)
        |WHERE point.distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
        |RETURN p.location as point
        """.stripMargin

    val cfg = plannerConfigWithBtreeIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter("x.maxDistance >= point.distance(cacheNFromStore[p.location], point({x: 0, y: 0, crs: 'cartesian'}))")
      .apply()
      .|.pointDistanceNodeIndexSeekExpr("p", "Place", "location", "{x: 0, y: 0, crs: 'cartesian'}", distanceExpr = prop("x", "maxDistance"), argumentIds = Set("x"), inclusive = true)
      .projection("{maxDistance: 10} AS x")
      .argument()
      .build()
  }

  private def plannerBaseConfigForUsingHintTests(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("S", 500)
      .setLabelCardinality("T", 500)
      .setRelationshipCardinality("()-[]->(:S)", 1000)
      .setRelationshipCardinality("(:T)-[]->(:S)", 1000)
      .setRelationshipCardinality("(:T)-[]->()", 1000)

  private def plannerConfigWithBtreeIndexForUsingHintTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForUsingHintTests()
      .addNodeIndex("S", Seq("p"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .addNodeIndex("T", Seq("p"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.BTREE) // This index is enforced by hint
      .addNodeIndex("T", Seq("foo"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.BTREE) // This index would normally be preferred
      .build()

  private def plannerConfigWithRangeIndexForUsingHintTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForUsingHintTests()
      .addNodeIndex("S", Seq("p"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
      .addNodeIndex("T", Seq("p"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE) // This index is enforced by hint
      .addNodeIndex("T", Seq("foo"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE) // This index would normally be preferred
      .enablePlanningRangeIndexes()
      .build()

  test("should allow one join and one index hint on the same variable using btree indexes") {
    val query =
      """MATCH (s:S {p: 10})<-[r]-(t:T {foo: 2})
        |USING JOIN ON t
        |USING INDEX t:T(p)
        |WHERE 0 <= t.p <= 10
        |RETURN s, r, t
        """.stripMargin

    val cfg = plannerConfigWithBtreeIndexForUsingHintTests()
    val plan = cfg.plan(query)

    // t:T(p) is enforced by hint
    // t:T(foo) would normally be preferred
    plan shouldEqual cfg.planBuilder()
      .produceResults("s", "r", "t")
      .nodeHashJoin("t")
      .|.expandAll("(s)<-[r]-(t)")
      .|.nodeIndexOperator("s:S(p = 10)")
      .filter("t.foo = 2")
      .nodeIndexOperator("t:T(0 <= p <= 10)")
      .build()
  }

  test("should allow one join and one scan hint on the same variable using btree indexes") {
    val query =
      """MATCH (s:S {p: 10})<-[r]-(t:T {foo: 2})
        |USING JOIN ON t
        |USING SCAN t:T
        |RETURN s, r, t
        """.stripMargin

    val cfg = plannerConfigWithBtreeIndexForUsingHintTests()
    val plan = cfg.plan(query)

    // t:T(foo) would normally be preferred
    plan shouldEqual cfg.planBuilder()
      .produceResults("s", "r", "t")
      .nodeHashJoin("t")
      .|.expandAll("(s)<-[r]-(t)")
      .|.nodeIndexOperator("s:S(p = 10)")
      .filter("t.foo = 2")
      .nodeByLabelScan("t", "T")
      .build()
  }

  test("should allow one join and one index hint on the same variable using range indexes") {
    val query =
      """MATCH (s:S {p: 10})<-[r]-(t:T {foo: 2})
        |USING JOIN ON t
        |USING INDEX t:T(p)
        |WHERE 0 <= t.p <= 10
        |RETURN s, r, t
        """.stripMargin

    val cfg = plannerConfigWithRangeIndexForUsingHintTests()
    val plan = cfg.plan(query)

    // t:T(p) is enforced by hint
    // t:T(foo) would normally be preferred
    plan shouldEqual cfg.planBuilder()
      .produceResults("s", "r", "t")
      .nodeHashJoin("t")
      .|.expandAll("(s)<-[r]-(t)")
      .|.nodeIndexOperator("s:S(p = 10)", indexType = IndexType.RANGE)
      .filter("t.foo = 2")
      .nodeIndexOperator("t:T(0 <= p <= 10)", indexType = IndexType.RANGE)
      .build()
  }

  test("should allow one join and one scan hint on the same variable using range indexes") {
    val query =
      """MATCH (s:S {p: 10})<-[r]-(t:T {foo: 2})
        |USING JOIN ON t
        |USING SCAN t:T
        |RETURN s, r, t
        """.stripMargin

    val cfg = plannerConfigWithRangeIndexForUsingHintTests()
    val plan = cfg.plan(query)

    // t:T(foo) would normally be preferred
    plan shouldEqual cfg.planBuilder()
      .produceResults("s", "r", "t")
      .nodeHashJoin("t")
      .|.expandAll("(s)<-[r]-(t)")
      .|.nodeIndexOperator("s:S(p = 10)", indexType = IndexType.RANGE)
      .filter("t.foo = 2")
      .nodeByLabelScan("t", "T")
      .build()
  }

  test("should or-leaf-plan in reasonable time") {
    import scala.concurrent.ExecutionContext.Implicits.global

    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Coleslaw", 100)
      .addNodeIndex("Coleslaw", Seq("name"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, isUnique = true)
      .build()

    val futurePlan =
      Future(
        cfg.plan {
          """
            |MATCH (n:Coleslaw) USING INDEX n:Coleslaw(name)
            |WHERE (n.age < 10 AND ( n.name IN $p0 OR
            |        n.name IN $p1 OR
            |        n.name IN $p2 OR
            |        n.name IN $p3 OR
            |        n.name IN $p4 OR
            |        n.name IN $p5 OR
            |        n.name IN $p6 OR
            |        n.name IN $p7 OR
            |        n.name IN $p8 OR
            |        n.name IN $p9 OR
            |        n.name IN $p10 OR
            |        n.name IN $p11 OR
            |        n.name IN $p12 OR
            |        n.name IN $p13 OR
            |        n.name IN $p14 OR
            |        n.name IN $p15 OR
            |        n.name IN $p16 OR
            |        n.name IN $p17 OR
            |        n.name IN $p18 OR
            |        n.name IN $p19 OR
            |        n.name IN $p20 OR
            |        n.name IN $p21 OR
            |        n.name IN $p22 OR
            |        n.name IN $p23 OR
            |        n.name IN $p24 OR
            |        n.name IN $p25) AND n.legal)
            |RETURN n.name as name
        """.stripMargin
        })

    Await.result(futurePlan, 1.minutes)
  }

  test("should not plan index scan if predicate variable is an argument") {
    val query =
      """
        |MATCH (a: Label {prop: $param})
        |MATCH (b)
        |WHERE (a:Label {prop: $param})-[]-(b)
        |RETURN a
        |""".stripMargin

    val cfg = plannerConfigForBtreeIndexOnLabelPropTests()
    val plan = cfg.plan(query).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .semiApply()
      .|.expandInto("(a)-[anon_2]-(b)")
      .|.filter("cacheN[a.prop] = $param", "a:Label")
      .|.argument("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .nodeIndexOperator("a:Label(prop = ???)", paramExpr = Some(parameter("param", CTAny)), getValue = _ => GetValue)
      .build()
  }

  test("should prefer label scan to node index scan from existence constraint with same cardinality") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) RETURN n")

    plan shouldEqual planner.planBuilder()
                            .produceResults("n")
                            .nodeByLabelScan("n", "Label")
                            .build()
  }

  test("should prefer label scan to node index scan from existence constraint with same cardinality, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN n")

    plan shouldEqual planner.planBuilder()
                            .produceResults("n")
                            .filter("n.x = 1")
                            .nodeByLabelScan("n", "Label")
                            .build()
  }

  test("should prefer type scan to relationship index scan from existence constraint with same cardinality") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) RETURN r")

    plan shouldEqual planner.planBuilder()
                            .produceResults("r")
                            .relationshipTypeScan("(a)-[r:REL]->(b)")
                            .build()
  }

  test("should prefer node index scan from existence constraint to label scan with same cardinality, if indexed property is used") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) RETURN n.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("n.prop AS p")
                            .nodeIndexOperator("n:Label(prop)")
                            .build()
  }

  test("should prefer relationship index scan from existence constraint to type scan with same cardinality, if indexed property is used") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) RETURN r.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("r.prop AS p")
                            .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)")
                            .build()
  }

  test("should prefer node index scan from existence constraint to label scan with same cardinality, if indexed property is used, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN n.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("n.prop AS p")
                            .filter("n.x = 1")
                            .nodeIndexOperator("n:Label(prop)")
                            .build()
  }

  test("should prefer relationship index scan from existence constraint to type scan with same cardinality, if indexed property is used, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.x = 1 RETURN r.prop AS p")

    plan shouldEqual planner.planBuilder()
                            .produceResults("p")
                            .projection("r.prop AS p")
                            .filter("r.x = 1")
                            .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)")
                            .build()
  }

  test("should prefer node index scan from aggregation to node index scan from existence constraint with same cardinality") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(n.counted) AS c"))
                            .nodeIndexOperator("n:Label(counted)")
                            .build()
  }

  test("should prefer relationship index scan from aggregation to relationship index scan from existence constraint with same cardinality") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipIndex("REL", Seq("counted"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) RETURN count(r.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(r.counted) AS c"))
                            .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)")
                            .build()
  }

  test("should prefer node index scan from aggregation to node index scan from existence constraint with same cardinality, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(n.counted) AS c"))
                            .filter("n.x = 1")
                            .nodeIndexOperator("n:Label(counted)")
                            .build()
  }

  test("should prefer relationship index scan from aggregation to relationship index scan from existence constraint with same cardinality, when filtered") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipIndex("REL", Seq("counted"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.x = 1 RETURN count(r.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(r.counted) AS c"))
                            .filter("r.x = 1")
                            .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)")
                            .build()
  }

  test("should prefer node index scan for aggregated property, even if other property is referenced") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.prop <> 1 AND n.x = 1 RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(n.counted) AS c"))
                            .filter("not n.prop = 1", "n.x = 1")
                            .nodeIndexOperator("n:Label(counted)")
                            .build()
  }

  test("should prefer relationship index scan for aggregated property, even if other property is referenced") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]-()", 10)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipIndex("REL", Seq("counted"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val plan = planner.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop <> 1 AND r.x = 1 RETURN count(r.counted) AS c")

    plan shouldEqual planner.planBuilder()
                            .produceResults("c")
                            .aggregation(Seq(), Seq("count(r.counted) AS c"))
                            .filter("not r.prop = 1", "r.x = 1")
                            .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)")
                            .build()
  }

  test("should not plan node index scan with existence constraint if query has updates before the match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val query =
      """CREATE (a:Label)
        |WITH count(*) AS c
        |MATCH (n:Label)
        |WHERE n.prop IS NULL
        |SET n.prop = 123""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodeProperty("n", "prop", "123")
      .eager()
      .filter("n.prop IS NULL")
      .apply()
      .|.nodeByLabelScan("n", "Label", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
      .create(createNode("a", "Label"))
      .argument()
      .build()
  }

  test("should not plan node index scan with existence constraint if query has updates before the match in a subquery") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val query =
      """CREATE (a:Label)
        |WITH count(*) AS c
        |CALL {
        |  WITH c
        |  WITH c LIMIT 1
        |  MATCH (n:Label)
        |  WHERE n.prop IS NULL
        |  SET n.prop = c
        |  RETURN n
        |}
        |RETURN *""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply(fromSubquery = true)
      .|.setNodeProperty("n", "prop", "c")
      .|.eager()
      .|.filter("n.prop IS NULL")
      .|.apply()
      .|.|.nodeByLabelScan("n", "Label", "c")
      .|.limit(1)
      .|.argument("c")
      .aggregation(Seq(), Seq("count(*) AS c"))
      .create(createNode("a", "Label"))
      .argument()
      .build()
  }

  test("should not plan node index scan with existence constraint if transaction state has changes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .setTxStateHasChanges()
      .build()

    val query =
      """MATCH (n:Label)
        |WHERE n.prop IS NULL
        |SET n.prop = 123""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodeProperty("n", "prop", "123")
      .eager()
      .filter("n.prop IS NULL")
      .nodeByLabelScan("n", "Label")
      .build()
  }

  test("should plan node index scan with existence constraint if query has updates after the match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, withValues = true, indexType = IndexType.BTREE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val query =
      """MATCH (n:Label)
        |CREATE (a:Label {prop: n.prop * 2})""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .create(createNodeWithProperties("a", Seq("Label"), "{prop: cacheN[n.prop] * 2}"))
      .nodeIndexOperator("n:Label(prop)", getValue = Map("prop" -> GetValue))
      .build()
  }

  test("should plan node index scan with existence constraint for aggregation if transaction state has changes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.BTREE)
      .addNodeExistenceConstraint("Label", "prop")
      .setTxStateHasChanges()
      .build()

    val query =
      """MATCH (n:Label)
        |RETURN sum(n.prop) AS result""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .aggregation(Seq.empty, Seq("sum(n.prop) AS result"))
      .nodeIndexOperator("n:Label(prop)")
      .build()
  }

  test("should not plan relationship index scan with existence constraint if query has updates before the match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val query =
      """CREATE (a)-[r:REL]->(b)
        |WITH count(*) AS c
        |MATCH (a)-[r:REL]->(b)
        |WHERE r.prop IS NULL
        |SET r.prop = 123""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setRelationshipProperty("r", "prop", "123")
      .eager()
      .filter("r.prop IS NULL")
      .apply()
      .|.relationshipTypeScan("(a)-[r:REL]->(b)", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
      .create(
        nodes = Seq(createNode("a"), createNode("b")),
        relationships = Seq(createRelationship("r", "a", "REL", "b", SemanticDirection.OUTGOING)))
      .argument()
      .build()
  }

  test("should not plan relationship index scan with existence constraint if query has updates before the match in a subquery") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val query =
      """CREATE (a)-[r:REL]->(b)
        |WITH count(*) AS c
        |CALL {
        |  WITH c
        |  MATCH (a)-[r:REL]->(b)
        |  WHERE r.prop IS NULL
        |  SET r.prop = c
        |  RETURN r
        |}
        |RETURN *""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .apply(fromSubquery = true)
      .|.setRelationshipProperty("r", "prop", "c")
      .|.eager()
      .|.filter("r.prop IS NULL")
      .|.relationshipTypeScan("(a)-[r:REL]->(b)", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
      .create(
        nodes = Seq(createNode("a"), createNode("b")),
        relationships = Seq(createRelationship("r", "a", "REL", "b", SemanticDirection.OUTGOING)))
      .argument()
      .build()
  }

  test("should not plan relationship index scan with existence constraint if transaction state has changes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .setTxStateHasChanges()
      .build()

    val query =
      """MATCH (a)-[r:REL]->(b)
        |WHERE r.prop IS NULL
        |SET r.prop = 123""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setRelationshipProperty("r", "prop", "123")
      .eager()
      .filter("r.prop IS NULL")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("should plan relationship index scan with existence constraint if query has updates after the match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0, withValues = true)
      .addRelationshipExistenceConstraint("REL", "prop")
      .build()

    val query =
      """MATCH (a)-[r1:REL]->(b)
        |CREATE (c)-[r2:REL {prop: r1.prop * 2}]->(d)""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .create(
        nodes = Seq(createNode("c"), createNode("d")),
        relationships = Seq(createRelationship("r2", "c", "REL", "d", SemanticDirection.OUTGOING, Some("{prop: cacheR[r1.prop] * 2}"))))
      .eager()
      .relationshipIndexOperator("(a)-[r1:REL(prop)]->(b)", getValue = Map("prop" -> GetValue))
      .build()
  }

  test("should plan relationship index scan with existence constraint for aggregation if transaction state has changes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
      .addRelationshipExistenceConstraint("REL", "prop")
      .setTxStateHasChanges()
      .build()

    val query =
      """MATCH (a)-[r:REL]->(b)
        |RETURN sum(r.prop) AS result""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .aggregation(Seq.empty, Seq("sum(r.prop) AS result"))
      .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)")
      .build()
  }

  test("should plan index usage for node pattern with exists() predicate") {
    val cfg = plannerConfigForBtreeIndexOnLabelPropTests()
    val plan = cfg.plan("MATCH (a:Label WHERE exists(a.prop)) RETURN a, a.prop").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .nodeIndexOperator("a:Label(prop)", indexType = IndexType.BTREE)
      .build()
  }

  test("should plan index usage for a pattern comprehension with exists() predicate") {
    val cfg = plannerConfigForBtreeIndexOnLabelPropTests()
    val plan = cfg.plan("RETURN [ (a:Label)-[r]->(b) WHERE exists(a.prop) | [a, a.prop] ] AS result ").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .rollUpApply("result", "anon_0")
      .|.projection("[a, a.prop] AS anon_0")
      .|.expandAll("(a)-[r]->(b)")
      .|.nodeIndexOperator("a:Label(prop)")
      .argument()
      .build()
  }

  test("should plan index usage for a pattern comprehension with exists() predicate inside a node pattern") {
    val cfg = plannerConfigForBtreeIndexOnLabelPropTests()
    val plan = cfg.plan("RETURN [ (a:Label WHERE exists(a.prop))-[r]->(b) | [a, a.prop] ] AS result ").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .rollUpApply("result", "anon_0")
      .|.projection("[a, a.prop] AS anon_0")
      .|.expandAll("(a)-[r]->(b)")
      .|.nodeIndexOperator("a:Label(prop)")
      .argument()
      .build()
  }

  test("should not plan node text index usage with IS NOT NULL predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("A", 50)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

    val plan = planner.plan("MATCH (a:A) WHERE a.prop IS NOT NULL RETURN a, a.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("cacheNFromStore[a.prop] IS NOT NULL")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should not plan relationship text index usage with IS NOT NULL predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

    val plan = planner.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop IS NOT NULL RETURN r, r.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .filter("cacheRFromStore[r.prop] IS NOT NULL")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("should not plan node text index usage when feature flag is not set") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .enablePlanningTextIndexes(false)
      .build()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter(s"cacheNFromStore[a.prop] $op 'hello'")
        .nodeByLabelScan("a", "A")
        .build()
    }
  }

  test("should not plan relationship text index usage when feature flag is not set") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .enablePlanningTextIndexes(false)
      .build()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .filter(s"cacheRFromStore[r.prop] $op 'hello'")
        .relationshipTypeScan("(a)-[r:REL]->(b)")
        .build()
    }
  }

  test("should plan node text index usage only for supported predicates when feature flag is set") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

    for (op <- List("STARTS WITH", "ENDS WITH", "CONTAINS", "<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("a.prop AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", indexType = IndexType.TEXT)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", getValue = Map("prop" -> GetValue), indexType = IndexType.TEXT)
        .build()
    }
  }

  test("should prefer node text index usage over btree only for ENDS WITH and CONTAINS") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .build()

    for (op <- List("ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("a.prop AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", indexType = IndexType.TEXT)
        .build()
    }

    for (op <- List("STARTS WITH", "<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("a.prop AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", indexType = IndexType.BTREE)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", getValue = Map("prop" -> GetValue), indexType = IndexType.BTREE)
        .build()
    }
  }

  test("should not plan node text index usage when comparing with non-string") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

    val queryStr = (op: String, arg: String) => s"MATCH (a:A) WHERE a.prop $op $arg RETURN a, a.prop"

    // sanity check
    val indexPlan = cfg.plan(queryStr("=", "'string'")).stripProduceResults
    indexPlan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .nodeIndexOperator("a:A(prop = 'string')", getValue = Map("prop" -> GetValue), indexType = IndexType.TEXT)
      .build()

    for (arg <- List("3", "a.prop2", "[\"a\", \"b\", \"c\"]", "$param")) {
      for (op <- List("<", "<=", ">", ">=", "=")) {
        val query = queryStr(op, arg)
        val plan = cfg.plan(query).stripProduceResults
        withClue(query) {
          plan shouldEqual cfg.subPlanBuilder()
            .projection("cacheN[a.prop] AS `a.prop`")
            .filter(s"cacheNFromStore[a.prop] $op $arg")
            .nodeByLabelScan("a", "A", IndexOrderNone)
            .build()
        }
      }
    }
  }

  test("should plan relationship text index usage only for supported predicates when feature flag is set") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

    for (op <- List("STARTS WITH", "ENDS WITH", "CONTAINS", "<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(s"(a)-[r:REL(prop $op 'hello')]->(b)", indexType = IndexType.TEXT)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(s"(a)-[r:REL(prop $op 'hello')]->(b)", getValue = Map("prop" -> GetValue), indexType = IndexType.TEXT)
        .build()
    }
  }

  test("should prefer relationship text index usage over btree only for ENDS WITH and CONTAINS") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .build()

    for (op <- List("ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(s"(a)-[r:REL(prop $op 'hello')]->(b)", indexType = IndexType.TEXT)
        .build()
    }

    for (op <- List("STARTS WITH", "<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(s"(a)-[r:REL(prop $op 'hello')]->(b)", indexType = IndexType.BTREE)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(s"(a)-[r:REL(prop $op 'hello')]->(b)", getValue = Map("prop" -> GetValue), indexType = IndexType.BTREE)
        .build()
    }
  }

  test("should not plan relationship text index usage when comparing with non-string") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

    val queryStr = (op: String, arg: String) => s"MATCH (a)-[r:R]->(b) WHERE r.prop $op $arg RETURN r, r.prop"

    // sanity check
    val indexPlan = cfg.plan(queryStr("=", "'string'")).stripProduceResults
    indexPlan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .relationshipIndexOperator("(a)-[r:R(prop = 'string')]->(b)", getValue = Map("prop" -> GetValue), indexType = IndexType.TEXT)
      .build()

    for (arg <- List("3", "a.prop2", "[\"a\", \"b\", \"c\"]", "$param")) {
      for (op <- List("<", "<=", ">", ">=", "=")) {
        val query = queryStr(op, arg)
        val plan = cfg.plan(query).stripProduceResults
        withClue(query) {
          plan shouldEqual cfg.subPlanBuilder()
            .projection("cacheR[r.prop] AS `r.prop`")
            .filter(s"cacheRFromStore[r.prop] $op $arg")
            .expandAll("(a)-[r:R]->(b)")
            .allNodeScan("a")
            .build()
        }
      }
    }
  }

  test("index hint with node pattern predicate") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .build()

    val queries = Seq(
      """MATCH (a:A)
        |USING INDEX a:A(prop)
        |WHERE a.prop > 10
        |RETURN a, a.prop
        |""".stripMargin,
      """MATCH (a:A WHERE a.prop > 10)
        |USING INDEX a:A(prop)
        |RETURN a, a.prop
        |""".stripMargin,
    )

    for (query <- queries) withClue(query) {
      val plan = cfg.plan(query)
      plan shouldEqual cfg.planBuilder()
        .produceResults("a", "`a.prop`")
        .projection("a.prop AS `a.prop`")
        .nodeIndexOperator("a:A(prop > 10)")
        .build()
    }
  }

  test("index hint with relationship pattern predicate") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1)
      .addSemanticFeature(SemanticFeature.RelationshipPatternPredicates)
      .build()

    val queries = Seq(
      """MATCH ()-[r:R]->()
        |USING INDEX r:R(prop)
        |WHERE r.prop > 10
        |RETURN r, r.prop
        |""".stripMargin,
      """MATCH ()-[r:R WHERE r.prop > 10]->()
        |USING INDEX r:R(prop)
        |RETURN r, r.prop
        |""".stripMargin,
    )

    for (query <- queries) withClue(query) {
      val plan = cfg.plan(query)
      plan shouldEqual cfg.planBuilder()
        .produceResults("r", "`r.prop`")
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator("(anon_0)-[r:R(prop > 10)]->(anon_1)")
        .build()
    }
  }

  private def plannerConfigForNodePointIndex: StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .enablePlanningPointIndexes()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .build()

  test("should plan node point index usage for equality") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop = point({x:1.0, y:2.0}) RETURN a, a.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .nodeIndexOperator(s"a:A(prop = ???)", paramExpr = Some(point(1.0, 2.0)), getValue = Map("prop" -> GetValue), indexType = IndexType.POINT)
      .build()
  }

  test("should plan node point index usage for point.distance") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(s"MATCH (a:A) WHERE point.distance(a.prop, point({x:1, y:2})) < 1.0 RETURN a, a.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("point.distance(cacheNFromStore[a.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceNodeIndexSeek("a", "A", "prop", "{x:1, y:2}", 1.0, indexType = IndexType.POINT)
      .build()
  }

  test("should plan node point index usage for point.withinBBox") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(s"MATCH (a:A) WHERE point.withinBBox(a.prop, point({x:1, y:2}), point({x:3, y:4})) RETURN a, a.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .pointBoundingBoxNodeIndexSeek("a", "A", "prop", "{x:1, y:2}", "{x:3, y:4}", indexType = IndexType.POINT)
      .build()
  }

  test("should not plan node point index usage for inequalities") {
    val cfg = plannerConfigForNodePointIndex
    for (op <- Seq("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op point({x:1, y:2}) RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter(s"cacheNFromStore[a.prop] $op point({x:1, y:2})")
        .nodeByLabelScan("a", "A")
        .build()
    }
  }

  test("should not plan node point index usage with IS NOT NULL predicate") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan("MATCH (a:A) WHERE a.prop IS NOT NULL RETURN a, a.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("cacheNFromStore[a.prop] IS NOT NULL")
      .nodeByLabelScan("a", "A")
      .build()
  }

  private def plannerConfigForRelPointIndex: StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .enablePlanningPointIndexes()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .build()

  test("should plan relationship point index usage for equality") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop = point({x:1.0, y:2.0}) RETURN r, r.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .relationshipIndexOperator("(a)-[r:REL(prop = ???)]->(b)", paramExpr = Some(point(1.0, 2.0)), getValue = Map("prop" -> GetValue), indexType = IndexType.POINT)
      .build()
  }

  test("should plan relationship point index usage for point.distance") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE point.distance(r.prop, point({x:1, y:2})) < 1.0 RETURN r, r.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .filter("point.distance(cacheRFromStore[r.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceRelationshipIndexSeek("r", "a", "b", "REL", "prop", "{x:1, y:2}", 1.0, indexType = IndexType.POINT)
      .build()
  }

  test("should plan relationship point index usage for point.withinBBox") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE point.withinBBox(r.prop, point({x:1, y:2}), point({x:3, y:4})) RETURN r, r.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("r.prop AS `r.prop`")
      .pointBoundingBoxRelationshipIndexSeek("r", "a", "b", "REL", "prop", "{x:1, y:2}", "{x:3, y:4}", indexType = IndexType.POINT)
      .build()
  }

  test("should not plan relationship point index usage for inequalities") {
    val cfg = plannerConfigForRelPointIndex
    for (op <- Seq("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op point({x:1, y:2}) RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .filter(s"cacheRFromStore[r.prop] $op point({x:1, y:2})")
        .relationshipTypeScan("(a)-[r:REL]->(b)")
        .build()
    }
  }

  test("should not plan relationship point index usage for IS NOT NULL") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop IS NOT NULL RETURN r, r.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .filter("cacheRFromStore[r.prop] IS NOT NULL")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("should prefer node point index usage over btree for compatible predicates") {
    val cfg = plannerBuilder()
      .enablePlanningPointIndexes()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .build()

    for (predicate <- Seq(
      "a.prop = point({x:1, y:2})",
      "point.withinBBox(a.prop, point({x:1, y:2}), point({x:3, y:4}))",
      "point.distance(a.prop, point({x:1, y:2})) < 1.0",
    )) {
      val query = s"MATCH (a:A) WHERE $predicate RETURN a, a.prop"
      val plan = cfg.plan(query)
      withClue(plan.asLogicalPlanBuilderString() + "\n\n") {
        plan.leftmostLeaf should beLike {
          case leaf: NodeIndexLeafPlan if leaf.indexType == IndexType.POINT => ()
        }
      }
    }
  }

  test("should prefer relationship point index usage over btree for compatible predicates") {
    val cfg = plannerBuilder()
      .enablePlanningPointIndexes()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .addRelationshipIndex("REL", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.BTREE)
      .build()

    for (predicate <- Seq(
      "r.prop = point({x:1, y:2})",
      "point.withinBBox(r.prop, point({x:1, y:2}), point({x:3, y:4}))",
      "point.distance(r.prop, point({x:1, y:2})) < 1.0",
    )) {
      val query = s"MATCH ()-[r:REL]->() WHERE $predicate RETURN r, r.prop"
      val plan = cfg.plan(query)
      withClue(plan.asLogicalPlanBuilderString() + "\n\n") {
        plan.leftmostLeaf should beLike {
          case leaf: RelationshipIndexLeafPlan if leaf.indexType == IndexType.POINT => ()
        }
      }
    }
  }
}
