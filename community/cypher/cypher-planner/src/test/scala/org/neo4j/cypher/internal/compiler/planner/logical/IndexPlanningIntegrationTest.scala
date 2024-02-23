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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.AttributeComparisonStrategy
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities.text_2_0
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.constraints.SchemaValueType

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class IndexPlanningIntegrationTest
    extends CypherFunSuite
    with LogicalPlanningIntegrationTestSupport
    with AstConstructionTestSupport
    with LogicalPlanningAttributesTestSupport {

  private def plannerBaseConfigForIndexOnLabelPropTests(): StatisticsBackedLogicalPlanningConfigurationBuilder =
    plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Label", 100)
      .setRelationshipCardinality("()-[]->(:Label)", 50)
      .setRelationshipCardinality("(:Label)-[]->()", 50)
      .setRelationshipCardinality("()-[]->()", 500)

  private def plannerConfigForRangeIndexOnLabelPropTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForIndexOnLabelPropTests()
      .addNodeIndex("Label", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
      .addFunction(
        functionSignature("datetime")
          .withInputField("value", CTString)
          .withOutputType(CTDateTime)
          .build()
      )
      .build()

  private def plannerConfigForRangeIndexOnRelationshipTypePropTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForIndexOnLabelPropTests()
      .setRelationshipCardinality("()-[:Type]->()", 20)
      .addRelationshipIndex(
        "Type",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 0.1,
        indexType = IndexType.RANGE
      )
      .build()

  test("should plan ranged index usage if predicate depends on simple variable from horizon") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan =
        cfg.plan(s"WITH 'foo' AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .apply()
        .|.nodeIndexOperator(
          s"b:Label(prop $op ???)",
          paramExpr = Some(v"foo"),
          argumentIds = Set("foo"),
          indexType = IndexType.RANGE
        )
        .projection("'foo' AS foo")
        .argument()
        .build()
    }
  }

  test("should plan range index scan for partial existence predicate if predicate is for points") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (
      predicate <- List(
        "point.withinBBox(a.prop, point({x:1, y:1}), point({x:2, y:2}))",
        "point.distance(a.prop, point({x:1, y:1})) < 10"
      )
    ) {
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
      .build()

    // seekable first predicate
    for (op1 <- List("=")) {
      for (op2 <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
        val query = s"MATCH (a:Label) WHERE a.prop1 $op1 1 AND a.prop2 $op2 2 RETURN a"

        withClue(s"Failed planning range index for: '$query'") {
          val plan = cfg.plan(query)

          plan shouldEqual cfg.planBuilder()
            .produceResults("a")
            .nodeIndexOperator(
              s"a:Label(prop1 $op1 1, prop2 $op2 2)",
              indexType = IndexType.RANGE,
              supportPartitionedScan = false
            )
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
            .nodeIndexOperator(
              s"a:Label(prop1 $op1 1, prop2)",
              indexType = IndexType.RANGE,
              supportPartitionedScan = false
            )
            .build()
        }
      }
    }
  }

  test("should plan composite range relationship index") {
    val cfg = plannerBaseConfigForIndexOnLabelPropTests()
      .addRelationshipIndex("Type", Seq("prop1", "prop2"), 0.1, 0.01, indexType = IndexType.RANGE)
      .setRelationshipCardinality("()-[:Type]->()", 20)
      .build()

    // seekable first predicate
    for (op1 <- List("=")) {
      for (op2 <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
        val query = s"MATCH (a)-[r:Type]->(b) WHERE r.prop1 $op1 1 AND r.prop2 $op2 2 RETURN r"

        withClue(s"Failed planning range index for: '$query'") {
          val plan = cfg.plan(query)

          plan shouldEqual cfg.planBuilder()
            .produceResults("r")
            .relationshipIndexOperator(
              s"(a)-[r:Type(prop1 $op1 1, prop2 $op2 2)]->(b)",
              indexType = IndexType.RANGE,
              supportPartitionedScan = false
            )
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
            .relationshipIndexOperator(
              s"(a)-[r:Type(prop1 $op1 1, prop2)]->(b)",
              indexType = IndexType.RANGE,
              supportPartitionedScan = false
            )
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
           |RETURN a""".stripMargin
      ).stripProduceResults

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
      val plan =
        cfg.plan(s"WITH 'foo' AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .apply()
        .|.nodeIndexOperator(
          s"b:Label(prop $op ???)",
          paramExpr = Some(v"foo"),
          argumentIds = Set("foo"),
          indexType = IndexType.RANGE
        )
        .projection("'foo' AS foo")
        .argument()
        .build()
    }
  }

  test("should plan range index usage if predicate depends on property of variable from horizon") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan = cfg.plan(
        s"WITH {prop: 'foo'} AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo.prop RETURN a"
      ).stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[r]-(a)")
        .apply()
        .|.nodeIndexOperator(
          s"b:Label(prop $op ???)",
          paramExpr = Some(prop("foo", "prop")),
          argumentIds = Set("foo"),
          indexType = IndexType.RANGE
        )
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
      .setLabelCardinality("Place", 40)
      .setLabelCardinality("Preference", 50)
      .setRelationshipCardinality("(:Place)-[]->()", 20)
      .setRelationshipCardinality("(:Place)-[]->(:Preference)", 20)
      .setRelationshipCardinality("()-[]->(:Preference)", 20)
      .setRelationshipCardinality("()-[]->()", 200)

  private def plannerConfigWithPointIndexForDistancePredicateTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForDistancePredicateTests()
      .addNodeIndex(
        "Place",
        Seq("location"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 0.1,
        indexType = IndexType.POINT
      )
      .build()

  test("should plan index scan if distance predicate depends on variable from same QueryGraph") {
    val query =
      """MATCH (p:Place)-[r]->(x:Preference)
        |WHERE point.distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= x.maxDistance
        |RETURN p.location as point
        """.stripMargin

    val cfg = plannerConfigWithPointIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter(
        "x.maxDistance >= point.distance(cacheNFromStore[p.location], point({x: 0, y: 0, crs: 'cartesian'}))",
        "x:Preference"
      )
      .expandAll("(p)-[r]->(x)")
      .nodeIndexOperator("p:Place(location)", indexType = IndexType.POINT, supportPartitionedScan = false)
      .build()
  }

  test("should plan index usage if distance predicate depends on variable from the horizon") {
    val query = """WITH 10 AS maxDistance
                  |MATCH (p:Place)
                  |WHERE point.distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance
                  |RETURN p.location as point
        """.stripMargin

    val cfg = plannerConfigWithPointIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter("point.distance(cacheNFromStore[p.location], point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance")
      .apply()
      .|.pointDistanceNodeIndexSeekExpr(
        "p",
        "Place",
        "location",
        "{x: 0, y: 0, crs: 'cartesian'}",
        distanceExpr = v"maxDistance",
        argumentIds = Set("maxDistance"),
        inclusive = true,
        indexType = IndexType.POINT
      )
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

    val cfg = plannerConfigWithPointIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter("x.maxDistance >= point.distance(cacheNFromStore[p.location], point({x: 0, y: 0, crs: 'cartesian'}))")
      .apply()
      .|.pointDistanceNodeIndexSeekExpr(
        "p",
        "Place",
        "location",
        "{x: 0, y: 0, crs: 'cartesian'}",
        distanceExpr = prop("x", "maxDistance"),
        argumentIds = Set("x"),
        inclusive = true,
        indexType = IndexType.POINT
      )
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
      .setRelationshipCardinality("()-[]->()", 10000)

  private def plannerConfigWithRangeIndexForUsingHintTests(): StatisticsBackedLogicalPlanningConfiguration =
    plannerBaseConfigForUsingHintTests()
      .addNodeIndex("S", Seq("p"), existsSelectivity = 1.0, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
      .addNodeIndex(
        "T",
        Seq("p"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 0.1,
        indexType = IndexType.RANGE
      ) // This index is enforced by hint
      .addNodeIndex(
        "T",
        Seq("foo"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 0.1,
        indexType = IndexType.RANGE
      ) // This index would normally be preferred
      .build()

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
        }
      )

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

    val cfg = plannerConfigForRangeIndexOnLabelPropTests()
    val plan = cfg.plan(query).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .semiApply()
      .|.expandInto("(a)-[anon_0]-(b)")
      .|.filter("cacheN[a.prop] = $param", "a:Label")
      .|.argument("a", "b")
      .cartesianProduct()
      .|.allNodeScan("b")
      .nodeIndexOperator(
        "a:Label(prop = ???)",
        paramExpr = Some(parameter("param", CTAny)),
        getValue = _ => GetValue,
        indexType = IndexType.RANGE
      )
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

  test(
    "should prefer node index scan from existence constraint to label scan with same cardinality, if indexed property is used"
  ) {

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
      .nodeIndexOperator("n:Label(prop)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should prefer relationship index scan from existence constraint to type scan with same cardinality, if indexed property is used"
  ) {

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
      .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should prefer node index scan from existence constraint to label scan with same cardinality, if indexed property is used, when filtered"
  ) {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN n.prop AS p")

    plan shouldEqual planner.planBuilder()
      .produceResults("p")
      .projection("n.prop AS p")
      .filter("n.x = 1")
      .nodeIndexOperator("n:Label(prop)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should prefer relationship index scan from existence constraint to type scan with same cardinality, if indexed property is used, when filtered"
  ) {

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
      .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should prefer node index scan from aggregation to node index scan from existence constraint with same cardinality"
  ) {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
      .produceResults("c")
      .aggregation(Seq(), Seq("count(n.counted) AS c"))
      .nodeIndexOperator("n:Label(counted)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should prefer relationship index scan from aggregation to relationship index scan from existence constraint with same cardinality"
  ) {

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
      .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should prefer node index scan from aggregation to node index scan from existence constraint with same cardinality, when filtered"
  ) {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.x = 1 RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
      .produceResults("c")
      .aggregation(Seq(), Seq("count(n.counted) AS c"))
      .filter("n.x = 1")
      .nodeIndexOperator("n:Label(counted)", indexType = IndexType.RANGE)
      .build()
  }

  test(
    "should prefer relationship index scan from aggregation to relationship index scan from existence constraint with same cardinality, when filtered"
  ) {

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
      .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test("should prefer node index scan for aggregated property, even if other property is referenced") {

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeIndex("Label", Seq("counted"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val plan = planner.plan(s"MATCH (n:Label) WHERE n.prop <> 1 AND n.x = 1 RETURN count(n.counted) AS c")

    plan shouldEqual planner.planBuilder()
      .produceResults("c")
      .aggregation(Seq(), Seq("count(n.counted) AS c"))
      .filter("not n.prop = 1", "n.x = 1")
      .nodeIndexOperator("n:Label(counted)", indexType = IndexType.RANGE)
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
      .relationshipIndexOperator("(a)-[r:REL(counted)]->(b)", indexType = IndexType.RANGE)
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
      .eager()
      .create(createNode("a", "Label"))
      .argument()
      .build()
  }

  test(
    "should not plan node index scan with existence constraint if query has updates before the match in a subquery"
  ) {
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
      .eager(ListSet(EagernessReason.Unknown))
      .apply()
      .|.setNodeProperty("n", "prop", "c")
      .|.eager()
      .|.filter("n.prop IS NULL")
      .|.apply()
      .|.|.nodeByLabelScan("n", "Label", "c")
      .|.limit(1)
      .|.argument("c")
      .aggregation(Seq(), Seq("count(*) AS c"))
      .eager()
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
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, withValues = true, indexType = IndexType.RANGE)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val query =
      """MATCH (n:Label)
        |CREATE (a:Label {prop: n.prop * 2})""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .create(createNodeWithProperties("a", Seq("Label"), "{prop: cacheN[n.prop] * 2}"))
      .nodeIndexOperator("n:Label(prop)", getValue = Map("prop" -> GetValue), indexType = IndexType.RANGE)
      .build()
  }

  test("should plan node index scan with existence constraint for aggregation if transaction state has changes") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.RANGE)
      .addNodeExistenceConstraint("Label", "prop")
      .setTxStateHasChanges()
      .build()

    val query =
      """MATCH (n:Label)
        |RETURN sum(n.prop) AS result""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .aggregation(Seq.empty, Seq("sum(n.prop) AS result"))
      .nodeIndexOperator("n:Label(prop)", indexType = IndexType.RANGE)
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
      .eager()
      .create(
        createNode("a"),
        createNode("b"),
        createRelationship("r", "a", "REL", "b", SemanticDirection.OUTGOING)
      )
      .argument()
      .build()
  }

  test(
    "should not plan relationship index scan with existence constraint if query has updates before the match in a subquery"
  ) {
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
      .eager(ListSet(EagernessReason.Unknown))
      .apply()
      .|.setRelationshipProperty("r", "prop", "c")
      .|.eager()
      .|.filter("r.prop IS NULL")
      .|.relationshipTypeScan("(a)-[r:REL]->(b)", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
      .eager()
      .create(
        createNode("a"),
        createNode("b"),
        createRelationship("r", "a", "REL", "b", SemanticDirection.OUTGOING)
      )
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
        createNode("c"),
        createNode("d"),
        createRelationship(
          "r2",
          "c",
          "REL",
          "d",
          SemanticDirection.OUTGOING,
          Some("{prop: cacheR[r1.prop] * 2}")
        )
      )
      .eager()
      .relationshipIndexOperator(
        "(a)-[r1:REL(prop)]->(b)",
        getValue = Map("prop" -> GetValue),
        indexType = IndexType.RANGE
      )
      .build()
  }

  test(
    "should plan relationship index scan with existence constraint for aggregation if transaction state has changes"
  ) {
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
      .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", indexType = IndexType.RANGE)
      .build()
  }

  test("should plan index usage for node pattern with IS NOT NULL predicate") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()
    val plan = cfg.plan("MATCH (a:Label WHERE a.prop IS NOT NULL) RETURN a, a.prop").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .nodeIndexOperator("a:Label(prop)", indexType = IndexType.RANGE)
      .build()
  }

  test("should plan index usage for a pattern comprehension with IS NOT NULL predicate") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()
    val plan =
      cfg.plan("RETURN [ (a:Label)-[r]->(b) WHERE a.prop IS NOT NULL | [a, a.prop] ] AS result ").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .rollUpApply("result", "anon_0")
      .|.projection("[a, a.prop] AS anon_0")
      .|.expandAll("(a)-[r]->(b)")
      .|.nodeIndexOperator("a:Label(prop)", indexType = IndexType.RANGE)
      .argument()
      .build()
  }

  test("should plan index usage for a pattern comprehension with IS NOT NULL predicate inside a node pattern") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()
    val plan =
      cfg.plan("RETURN [ (a:Label WHERE a.prop IS NOT NULL)-[r]->(b) | [a, a.prop] ] AS result ").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .rollUpApply("result", "anon_0")
      .|.projection("[a, a.prop] AS anon_0")
      .|.expandAll("(a)-[r]->(b)")
      .|.nodeIndexOperator("a:Label(prop)", indexType = IndexType.RANGE)
      .argument()
      .build()
  }

  private def plannerConfigForNodeTextIndex: StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

  test("should not plan node text index usage with IS NOT NULL predicate") {
    val planner = plannerConfigForNodeTextIndex

    val plan = planner.plan("MATCH (a:A) WHERE a.prop IS NOT NULL RETURN a, a.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("cacheNFromStore[a.prop] IS NOT NULL")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should plan node text index scan with IS :: STRING NOT NULL predicate") {
    val planner = plannerConfigForNodeTextIndex

    val plan = planner.plan("MATCH (a:A) WHERE a.prop IS :: STRING NOT NULL RETURN a, a.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .nodeIndexOperator("a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false)
      .build()
  }

  test("should plan type filter on top of node range index scan with IS :: STRING NOT NULL predicate") {
    val planner = plannerConfigForRangeIndexOnLabelPropTests()

    val plan = planner.plan("MATCH (a:Label) WHERE a.prop IS :: STRING NOT NULL RETURN a, a.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("cacheNFromStore[a.prop] IS :: STRING NOT NULL")
      .nodeIndexOperator("a:Label(prop)", indexType = IndexType.RANGE)
      .build()
  }

  test("should not plan relationship text index usage with IS NOT NULL predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .build()

    val plan = planner.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop IS NOT NULL RETURN r, r.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .filter("cacheRFromStore[r.prop] IS NOT NULL")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("should plan node text index seek for supported predicates") {
    val cfg = plannerConfigForNodeTextIndex

    for (op <- List("STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("a.prop AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator(
          s"a:A(prop $op 'hello')",
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
        .build()
    }
    for (op <- List("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter(s"cacheNFromStore[a.prop] $op 'hello'")
        .nodeIndexOperator("a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }
  }

  test("should plan node text index scan for IS NOT NULL predicate when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = 0.1,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT,
        maybeIndexCapability = Some(text_2_0)
      )
      .addNodePropertyTypeConstraint("A", "prop", SchemaValueType.STRING)
      .build()

    val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop IS NOT NULL RETURN *").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .nodeIndexOperator(s"a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false)
      .build()
  }

  test("should plan node text index scan for equality predicate when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = 0.1,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT,
        maybeIndexCapability = Some(text_2_0)
      )
      .addNodePropertyTypeConstraint("A", "prop", SchemaValueType.STRING)
      .build()

    val plan = cfg.plan("MATCH (a:A) WHERE a.prop = $param RETURN *").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .nodeIndexOperator(
        "a:A(prop = ???)",
        paramExpr = List(parameter("param", CTAny)),
        indexType = IndexType.TEXT,
        supportPartitionedScan = false
      )
      .build()
  }

  test("should plan node point index scan for IS NOT NULL predicate when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = 0.1,
        uniqueSelectivity = 0.1,
        indexType = IndexType.POINT
      )
      .addNodePropertyTypeConstraint("A", "prop", SchemaValueType.POINT)
      .build()

    val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop IS NOT NULL RETURN *").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .nodeIndexOperator(s"a:A(prop)", indexType = IndexType.POINT, supportPartitionedScan = false)
      .build()
  }

  test("should plan node text index scan for a.prop = b.prop predicate when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setLabelCardinality("A", 500)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = 0.1,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT,
        maybeIndexCapability = Some(text_2_0)
      )
      .addNodePropertyTypeConstraint("A", "prop", SchemaValueType.STRING)
      .build()

    val plan = cfg.plan(s"MATCH (a:A)-[r]->(b) WHERE a.prop = b.prop RETURN *").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("a.prop = b.prop")
      .expandAll("(a)-[r]->(b)")
      .nodeIndexOperator(s"a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false)
      .build()
  }

  test("should plan relationship text index scan for r.prop IS NOT NULL when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("(:A)-[:REL]->()", 500)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .setLabelCardinality("A", 500)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.1,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT,
        maybeIndexCapability = Some(text_2_0)
      )
      .addRelationshipPropertyTypeConstraint("REL", "prop", SchemaValueType.STRING)
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop IS NOT NULL RETURN *").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .relationshipIndexOperator(s"(a)-[r:REL(prop)]->(b)", indexType = IndexType.TEXT, supportPartitionedScan = false)
      .build()
  }

  test("should plan relationship text index scan for r.prop = a.prop predicate when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("(:A)-[:REL]->()", 500)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .setLabelCardinality("A", 500)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.1,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT,
        maybeIndexCapability = Some(text_2_0)
      )
      .addRelationshipPropertyTypeConstraint("REL", "prop", SchemaValueType.STRING)
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop = a.prop RETURN *").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("r.prop = a.prop")
      .relationshipIndexOperator(s"(a)-[r:REL(prop)]->(b)", indexType = IndexType.TEXT, supportPartitionedScan = false)
      .build()
  }

  test("should plan relationship point index scan for r.prop = a.prop predicate when type constraint exists") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("(:A)-[:REL]->()", 500)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .setLabelCardinality("A", 500)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.1,
        uniqueSelectivity = 0.1,
        indexType = IndexType.POINT
      )
      .addRelationshipPropertyTypeConstraint("REL", "prop", SchemaValueType.POINT)
      .build()

    val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop = a.prop RETURN *").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .filter("r.prop = a.prop")
      .relationshipIndexOperator(s"(a)-[r:REL(prop)]->(b)", indexType = IndexType.POINT, supportPartitionedScan = false)
      .build()
  }

  test(
    "should plan node text index seek only for supported predicates with a text index that does not support range predicates"
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT,
        maybeIndexCapability = Some(text_2_0)
      )
      .build()

    for (op <- List("STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("a.prop AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }

    for (op <- List("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter(s"cacheNFromStore[a.prop] $op 'hello'")
        .nodeIndexOperator("a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator(
          s"a:A(prop $op 'hello')",
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
        .build()
    }
  }

  test("should prefer node text index usage over range only for ENDS WITH and CONTAINS") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
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
        .nodeIndexOperator(s"a:A(prop $op 'hello')", indexType = IndexType.RANGE)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", getValue = Map("prop" -> GetValue), indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should not plan node text index usage when comparing with non-string") {
    val cfg = plannerConfigForNodeTextIndex

    val queryStr = (op: String, arg: String) => s"MATCH (a:A) WHERE a.prop $op $arg RETURN a, a.prop"

    // sanity check
    val indexPlan = cfg.plan(queryStr("=", "'string'")).stripProduceResults
    indexPlan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .nodeIndexOperator(
        "a:A(prop = 'string')",
        getValue = Map("prop" -> GetValue),
        indexType = IndexType.TEXT,
        supportPartitionedScan = false
      )
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

  test("should plan relationship text index seek only for supported predicates") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .build()

    for (op <- List("STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(
          s"(a)-[r:REL(prop $op 'hello')]->(b)",
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          s"(a)-[r:REL(prop $op 'hello')]->(b)",
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
        .build()
    }
    for (op <- List("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .filter(s"cacheRFromStore[r.prop] $op 'hello'")
        .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }
  }

  test(
    "should plan relationship text index seek only for supported predicates with a text index that does not support range predicates"
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT,
        maybeIndexCapability = Some(text_2_0)
      )
      .build()

    for (op <- List("STARTS WITH", "ENDS WITH", "CONTAINS")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator(
          s"(a)-[r:REL(prop $op 'hello')]->(b)",
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
        .build()
    }

    for (op <- List("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .filter(s"cacheRFromStore[r.prop] $op 'hello'")
        .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          s"(a)-[r:REL(prop $op 'hello')]->(b)",
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
        .build()
    }
  }

  test("should prefer relationship text index usage over range only for ENDS WITH and CONTAINS") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 200)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.RANGE
      )
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
        .relationshipIndexOperator(s"(a)-[r:REL(prop $op 'hello')]->(b)", indexType = IndexType.RANGE)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          s"(a)-[r:REL(prop $op 'hello')]->(b)",
          getValue = Map("prop" -> GetValue),
          indexType = IndexType.RANGE
        )
        .build()
    }
  }

  test("should not plan relationship text index usage when comparing with non-string") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .build()

    val queryStr = (op: String, arg: String) => s"MATCH (a)-[r:R]->(b) WHERE r.prop $op $arg RETURN r, r.prop"

    // sanity check
    val indexPlan = cfg.plan(queryStr("=", "'string'")).stripProduceResults
    indexPlan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .relationshipIndexOperator(
        "(a)-[r:R(prop = 'string')]->(b)",
        getValue = Map("prop" -> GetValue),
        indexType = IndexType.TEXT,
        supportPartitionedScan = false
      )
      .build()

    for (arg <- List("3", "a.prop2", "[\"a\", \"b\", \"c\"]", "$param")) {
      for (op <- List("<", "<=", ">", ">=", "=")) {
        val query = queryStr(op, arg)
        val plan = cfg.plan(query).stripProduceResults
        withClue(query) {
          plan shouldEqual cfg.subPlanBuilder()
            .projection("cacheR[r.prop] AS `r.prop`")
            .filter(s"cacheRFromStore[r.prop] $op $arg")
            .relationshipTypeScan("(a)-[r:R]->(b)")
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
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
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
        |""".stripMargin
    )

    for (query <- queries) withClue(query) {
      val plan = cfg.plan(query)
      plan shouldEqual cfg.planBuilder()
        .produceResults("a", "`a.prop`")
        .projection("a.prop AS `a.prop`")
        .nodeIndexOperator("a:A(prop > 10)", indexType = IndexType.RANGE)
        .build()
    }
  }

  test("index hint with relationship pattern predicate") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setAllRelationshipsCardinality(1000)
      .setRelationshipCardinality("()-[:R]->()", 500)
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1)
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
        |""".stripMargin
    )

    for (query <- queries) withClue(query) {
      val plan = cfg.plan(query)
      plan shouldEqual cfg.planBuilder()
        .produceResults("r", "`r.prop`")
        .projection("r.prop AS `r.prop`")
        .relationshipIndexOperator("(anon_0)-[r:R(prop > 10)]->(anon_1)", indexType = IndexType.RANGE)
        .build()
    }
  }

  private def plannerConfigForNodePointIndex: StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .build()

  test("should plan node point index usage for equality") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop = point({x:1.0, y:2.0}) RETURN a, a.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .nodeIndexOperator(
        s"a:A(prop = ???)",
        paramExpr = Some(point(1.0, 2.0)),
        getValue = Map("prop" -> GetValue),
        indexType = IndexType.POINT,
        supportPartitionedScan = false
      )
      .build()
  }

  test("should plan node point index usage for point.distance") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(
      s"MATCH (a:A) WHERE point.distance(a.prop, point({x:1, y:2})) < 1.0 RETURN a, a.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("point.distance(cacheNFromStore[a.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceNodeIndexSeek("a", "A", "prop", "{x:1, y:2}", 1.0, indexType = IndexType.POINT)
      .build()
  }

  test("should plan node point index usage for point.withinBBox") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(
      s"MATCH (a:A) WHERE point.withinBBox(a.prop, point({x:1, y:2}), point({x:3, y:4})) RETURN a, a.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .pointBoundingBoxNodeIndexSeek("a", "A", "prop", "{x:1, y:2}", "{x:3, y:4}", indexType = IndexType.POINT)
      .build()
  }

  test("should plan node point index usage for inequalities with point literals") {
    val cfg = plannerConfigForNodePointIndex
    for (op <- Seq("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op point({x:1, y:2}) RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter(s"cacheNFromStore[a.prop] $op point({x:1, y:2})")
        .nodeIndexOperator("a:A(prop)", indexType = IndexType.POINT, supportPartitionedScan = false)
        .build()
    }
  }

  test("should not plan node point index usage for inequalities with non-point types") {
    val cfg = plannerConfigForNodePointIndex
    for (op <- Seq("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 123 RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .filter(s"a.prop $op 123")
        .nodeByLabelScan("a", "A")
        .build()
    }
  }

  test("should plan node point index scan with IS :: POINT NOT NULL predicate") {
    val planner = plannerConfigForNodePointIndex

    val plan = planner.plan("MATCH (a:A) WHERE a.prop IS :: POINT NOT NULL RETURN a, a.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .nodeIndexOperator("a:A(prop)", indexType = IndexType.POINT, supportPartitionedScan = false)
      .build()
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
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.POINT
      )
      .build()

  test("should plan relationship point index usage for equality") {
    val cfg = plannerConfigForRelPointIndex
    val plan =
      cfg.plan("MATCH (a)-[r:REL]->(b) WHERE r.prop = point({x:1.0, y:2.0}) RETURN r, r.prop").stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .relationshipIndexOperator(
        "(a)-[r:REL(prop = ???)]->(b)",
        paramExpr = Some(point(1.0, 2.0)),
        getValue = Map("prop" -> GetValue),
        indexType = IndexType.POINT,
        supportPartitionedScan = false
      )
      .build()
  }

  test("should plan relationship point index usage for point.distance") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan(
      s"MATCH (a)-[r:REL]->(b) WHERE point.distance(r.prop, point({x:1, y:2})) < 1.0 RETURN r, r.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .filter("point.distance(cacheRFromStore[r.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceRelationshipIndexSeek("r", "a", "b", "REL", "prop", "{x:1, y:2}", 1.0, indexType = IndexType.POINT)
      .build()
  }

  test("should plan relationship point index usage for point.distance (case insensitive)") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan(
      s"MATCH (a)-[r:REL]->(b) WHERE pOinT.dIsTaNcE(r.prop, point({x:1, y:2})) < 1.0 RETURN r, r.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .filter("pOinT.dIsTaNcE(cacheRFromStore[r.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceRelationshipIndexSeek("r", "a", "b", "REL", "prop", "{x:1, y:2}", 1.0, indexType = IndexType.POINT)
      .build()
  }

  test("should plan relationship point index usage for point.withinBBox") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan(
      s"MATCH (a)-[r:REL]->(b) WHERE point.withinBBox(r.prop, point({x:1, y:2}), point({x:3, y:4})) RETURN r, r.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("r.prop AS `r.prop`")
      .pointBoundingBoxRelationshipIndexSeek(
        "r",
        "a",
        "b",
        "REL",
        "prop",
        "{x:1, y:2}",
        "{x:3, y:4}",
        indexType = IndexType.POINT
      )
      .build()
  }

  test("should plan node point index usage for point.withinBBox (case insensitive)") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(
      s"MATCH (a:A) WHERE PoInT.wItHinBbOx(a.prop, point({x:1, y:2}), point({x:3, y:4})) RETURN a, a.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("a.prop AS `a.prop`")
      .pointBoundingBoxNodeIndexSeek("a", "A", "prop", "{x:1, y:2}", "{x:3, y:4}", indexType = IndexType.POINT)
      .build()
  }

  test("should plan relationship point index scan for inequalities") {
    val cfg = plannerConfigForRelPointIndex
    for (op <- Seq("<", "<=", ">", ">=")) {
      val plan =
        cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op point({x:1, y:2}) RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .filter(s"cacheRFromStore[r.prop] $op point({x:1, y:2})")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]->(b)",
          indexType = IndexType.POINT,
          supportPartitionedScan = false
        )
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

  test("should prefer node point index usage over range for compatible predicates") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.RANGE)
      .build()

    for (
      predicate <- Seq(
        "a.prop = point({x:1, y:2})",
        "point.withinBBox(a.prop, point({x:1, y:2}), point({x:3, y:4}))",
        "point.distance(a.prop, point({x:1, y:2})) < 1.0"
      )
    ) {
      val query = s"MATCH (a:A) WHERE $predicate RETURN a, a.prop"
      val plan = cfg.plan(query)
      withClue(plan.asLogicalPlanBuilderString() + "\n\n") {
        plan.leftmostLeaf should beLike {
          case leaf: NodeIndexLeafPlan if leaf.indexType == IndexType.POINT => ()
        }
      }
    }
  }

  test("should prefer relationship point index usage over range for compatible predicates") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.POINT
      )
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.RANGE
      )
      .build()

    for (
      predicate <- Seq(
        "r.prop = point({x:1, y:2})",
        "point.withinBBox(r.prop, point({x:1, y:2}), point({x:3, y:4}))",
        "point.distance(r.prop, point({x:1, y:2})) < 1.0"
      )
    ) {
      val query = s"MATCH ()-[r:REL]->() WHERE $predicate RETURN r, r.prop"
      val plan = cfg.plan(query)
      withClue(plan.asLogicalPlanBuilderString() + "\n\n") {
        plan.leftmostLeaf should beLike {
          case leaf: RelationshipIndexLeafPlan if leaf.indexType == IndexType.POINT => ()
        }
      }
    }
  }

  test("should calculate node index scan cardinality correctly with multiple predicates") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 1000)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.1, indexType = IndexType.RANGE)
      .build()

    val query =
      """MATCH (a:A)
        |WHERE a.prop CONTAINS 'hello' AND a.prop CONTAINS 'world'
        |RETURN a
        |""".stripMargin

    val planState = cfg.planState(query)
    val expected = cfg.planBuilder()
      .produceResults("a")
      .filterExpression(andsReorderable(
        "cacheNFromStore[a.prop] CONTAINS 'hello'",
        "cacheNFromStore[a.prop] CONTAINS 'world'"
      ))
      .nodeIndexOperator("a:A(prop)").withCardinality(500)

    planState should
      haveSamePlanAndCardinalitiesAsBuilder(expected, AttributeComparisonStrategy.ComparingProvidedAttributesOnly)
  }

  test("should calculate relationship index scan cardinality correctly with multiple predicates") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 1000)
      .addRelationshipIndex("REL", Seq("prop"), 0.5, 0.1, indexType = IndexType.RANGE)
      .build()

    val query =
      """MATCH (a)-[r:REL]->(b)
        |WHERE r.prop CONTAINS 'hello' AND r.prop CONTAINS 'world'
        |RETURN r
        |""".stripMargin

    val planState = cfg.planState(query)
    val expected = cfg.planBuilder()
      .produceResults("r")
      .filterExpression(andsReorderable(
        "cacheRFromStore[r.prop] CONTAINS 'hello'",
        "cacheRFromStore[r.prop] CONTAINS 'world'"
      ))
      .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)").withCardinality(500)

    planState should
      haveSamePlanAndCardinalitiesAsBuilder(expected, AttributeComparisonStrategy.ComparingProvidedAttributesOnly)
  }

  test("should plan index seek for COUNT subquery with multiple predicates") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 1000)
      .setRelationshipCardinality("(:A)-[]->()", 500)
      .setRelationshipCardinality("()-[]->()", 500)
      .addNodeIndex("A", Seq("prop"), 0.5, 0.1, indexType = IndexType.RANGE)
      .build()

    val q = "RETURN COUNT { (n:A)-[r]->(b) WHERE n.prop > 3 AND n.prop < 10 } AS result"
    val plan = cfg.plan(q).stripProduceResults
    plan shouldBe cfg.subPlanBuilder()
      .aggregation(Seq.empty, Seq("count(*) AS result"))
      .expandAll("(n)-[r]->(b)")
      .nodeIndexOperator("n:A(3 < prop < 10)", indexType = IndexType.RANGE)
      .build()

  }

  test("should plan a unique index seek with a single estimated row") {
    val planner = plannerBuilder()
      .enableMinimumGraphStatistics()
      .setAllNodesCardinality(1)
      .setLabelCardinality("User", 1)
      .addNodeIndex("User", Seq("id"), 1.0, 1.0, isUnique = true)
      .build()

    val query = "MATCH (u:User {id: 123}) RETURN u"

    val expected = planner.planBuilder()
      .produceResults("u").withCardinality(1)
      .nodeIndexOperator("u:User(id = 123)", unique = true).withCardinality(1)

    val actual = planner.planState(query)
    actual should haveSamePlanAndEffectiveCardinalitiesAsBuilder(expected)
  }

  private def scannablePredicates(prop: String): Seq[String] = {
    val binaryOpPredicates =
      Seq("=", ">", ">=", "<", "<=", "IN")
        .map(op => s"$prop $op $$param")

    binaryOpPredicates ++ scannableTextPredicates(prop) ++ scannablePointPredicates(prop)
  }

  private def scannableTextPredicates(prop: String): Seq[String] = {
    Seq("STARTS WITH", "ENDS WITH", "CONTAINS")
      .map(op => s"$prop $op $$param")
  }

  private def scannablePointPredicates(prop: String): Seq[String] = {
    Seq(
      s"point.withinBBox($prop, $$p1, $$p2)",
      s"point.distance($prop, $$p1) < $$p2"
    )
  }

  test("should plan a node index scan for negated predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), 0.1, 0.1)
      .build()

    for (pred <- scannablePredicates("a.prop")) {
      val q = s"MATCH (a:A) WHERE NOT $pred RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .nodeIndexOperator("a:A(prop)")
        .build()
    }
  }

  test("should plan a composite node index scan for negated predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop", "otherProp"), 0.1, 0.1)
      .build()

    for (pred <- scannablePredicates("a.prop")) {
      val q = s"MATCH (a:A) WHERE NOT $pred AND a.otherProp IS NOT NULL RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .nodeIndexOperator("a:A(prop, otherProp)")
        .build()
    }
  }

  test("should plan a relationship index scan for negated predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1)
      .build()

    for (pred <- scannablePredicates("r.prop")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT $pred RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)")
        .build()
    }
  }

  test("should plan a composite relationship index scan for negated predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop", "otherProp"), 0.1, 0.1)
      .build()

    for (pred <- scannablePredicates("r.prop")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT $pred AND r.otherProp IS NOT NULL RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .relationshipIndexOperator("(a)-[r:REL(prop, otherProp)]->(b)")
        .build()
    }
  }

  test("should plan node text index scan with negated predicate") {
    val planner = plannerConfigForNodeTextIndex

    for (pred <- scannableTextPredicates("a.prop")) {
      val q = s"MATCH (a:A) WHERE NOT $pred RETURN a"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .nodeIndexOperator("a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }
  }

  test("should plan relationship text index scan with negated predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .setRelationshipCardinality("()-[:REL]->()", 1000)
      .build()

    for (pred <- scannableTextPredicates("r.prop")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT $pred RETURN r"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }
  }

  test("should plan node point index scan with negated predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.POINT)
      .setLabelCardinality("A", 1000)
      .build()

    for (pred <- scannablePointPredicates("a.prop")) {
      val q = s"MATCH (a:A) WHERE NOT $pred RETURN a"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .nodeIndexOperator("a:A(prop)", indexType = IndexType.POINT, supportPartitionedScan = false)
        .build()
    }
  }

  test("should plan relationship point index scan with negated predicate") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .addRelationshipIndex(
        "REL",
        Seq("prop"),
        existsSelectivity = 0.5,
        uniqueSelectivity = 0.1,
        indexType = IndexType.POINT
      )
      .setRelationshipCardinality("()-[:REL]->()", 1000)
      .build()

    for (pred <- scannablePointPredicates("r.prop")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT $pred RETURN r"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT $pred")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop)]->(b)",
          indexType = IndexType.POINT,
          supportPartitionedScan = false
        )
        .build()
    }
  }

  test("should plan index scan if text predicate depends on variable from same QueryGraph") {
    val planner = plannerConfigForNodeTextIndex

    val q = "MATCH (a:A) WHERE a.prop CONTAINS a.otherProp RETURN a"

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("a.prop CONTAINS a.otherProp")
      .nodeIndexOperator("a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false)
      .build()
  }

  test("should estimate cardinality for less-than with a string literal using TEXT index") {
    val existsSelectivity = 0.5
    val labelCardinality = 1000

    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = existsSelectivity,
        uniqueSelectivity = 0.1,
        indexType = IndexType.TEXT
      )
      .setLabelCardinality("A", labelCardinality)
      .build()

    val q = "MATCH (a:A) WHERE a.prop < 'hello' RETURN a"
    val planState = planner.planState(q)
    val expected = planner.planBuilder()
      .produceResults("a")
      .filter("a.prop < 'hello'")
      .nodeIndexOperator("a:A(prop)", indexType = IndexType.TEXT, supportPartitionedScan = false).withCardinality(
        labelCardinality * existsSelectivity
      )

    planState should haveSamePlanAndCardinalitiesAsBuilder(
      expected,
      AttributeComparisonStrategy.ComparingProvidedAttributesOnly
    )
  }

  test("should estimate cardinality for less-than with a point literal using POINT index") {
    val existsSelectivity = 0.5
    val labelCardinality = 1000

    val planner = plannerBuilder()
      .setAllNodesCardinality(2000)
      .addNodeIndex(
        "A",
        Seq("prop"),
        existsSelectivity = existsSelectivity,
        uniqueSelectivity = 0.1,
        indexType = IndexType.POINT
      )
      .setLabelCardinality("A", labelCardinality)
      .build()

    val q = "MATCH (a:A) WHERE a.prop < point({x:1, y:2}) RETURN a"
    val planState = planner.planState(q)
    val expected = planner.planBuilder()
      .produceResults("a")
      .filter("a.prop < point({x:1, y:2})")
      .nodeIndexOperator("a:A(prop)", indexType = IndexType.POINT, supportPartitionedScan = false).withCardinality(
        labelCardinality * existsSelectivity
      )

    planState should haveSamePlanAndCardinalitiesAsBuilder(
      expected,
      AttributeComparisonStrategy.ComparingProvidedAttributesOnly
    )
  }

  test("should stringify index seek with datetime function properly") {
    val planner = plannerConfigForRangeIndexOnLabelPropTests()

    implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

    planner.plan(
      """MATCH (a:Label)
        |  WHERE datetime('2021-01-01') < a.prop
        |RETURN a""".stripMargin
    ).asLogicalPlanBuilderString() should equal(
      """.produceResults("a")
        |.nodeIndexOperator("a:Label(prop > datetime('2021-01-01'))", indexOrder = IndexOrderNone, argumentIds = Set(), getValue = Map("prop" -> DoNotGetValue), unique = false, indexType = IndexType.RANGE, supportPartitionedScan = true)
        |.build()""".stripMargin
    )
  }

  test("should use the widest applicable composite index to estimate cardinality") {
    val aNodes = 900

    val existsSelectivity = 0.5
    val uniqueSelectivity = 0.2

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aNodes)
      .addNodeIndex("A", Seq("a", "b", "c", "d"), 0.2, 0.01)
      .addNodeIndex("A", Seq("a", "b", "c"), existsSelectivity, uniqueSelectivity)
      .addNodeIndex("A", Seq("a", "b"), 0.9, 0.5)
      .addNodeIndex("A", Seq("a"), 1.0, 1)
      .build()

    val query = "MATCH (n:A {a: 1, b: 2, c: 3}) RETURN n"
    val planState = planner.planState(query)

    val expectedCardinality = aNodes * existsSelectivity * uniqueSelectivity

    val expectedPlanBuilder = planner.planBuilder()
      .produceResults("n").withCardinality(expectedCardinality)
      .nodeIndexOperator("n:A(a = 1, b = 2, c = 3)", supportPartitionedScan = false).withCardinality(
        expectedCardinality
      )

    planState should haveSamePlanAndCardinalitiesAsBuilder(expectedPlanBuilder)
  }

  test("should use multiple disjoint composite indexes to estimate cardinality") {
    val aNodes = 900

    val abExistsSelectivity = 1.0
    val abUniqueSelectivity = 0.5

    val cdExistsSelectivity = 0.6
    val cdUniqueSelectivity = 0.1

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aNodes)
      .addNodeIndex("A", Seq("a", "b"), abExistsSelectivity, abUniqueSelectivity)
      .addNodeIndex("A", Seq("c", "d"), cdExistsSelectivity, cdUniqueSelectivity)
      .build()

    val query = "MATCH (n:A {a: 1, b: 2, c: 3, d: 4}) RETURN n"
    val planState = planner.planState(query)

    val cdCardinality = aNodes * cdExistsSelectivity * cdUniqueSelectivity
    val abCardinality = cdCardinality * abExistsSelectivity * abUniqueSelectivity

    val expectedPlanBuilder = planner.planBuilder()
      .produceResults("n").withCardinality(abCardinality)
      .filterExpression(andsReorderable("n.a = 1", "n.b = 2")).withCardinality(abCardinality)
      .nodeIndexOperator("n:A(c = 3, d = 4)", supportPartitionedScan = false).withCardinality(cdCardinality)

    planState should haveSamePlanAndCardinalitiesAsBuilder(expectedPlanBuilder)
  }

  test(
    "should use the widest applicable composite index to estimate cardinality, even if better predicate coverage is possible from narrower indexes"
  ) {
    val aNodes = 900

    val abExistsSelectivity = 1.0
    val abUniqueSelectivity = 0.5

    val cdExistsSelectivity = 0.6
    val cdUniqueSelectivity = 0.1

    val abcExistsSelectivity = 0.7
    val abcUniqueSelectivity = 0.3

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aNodes)
      .addNodeIndex("A", Seq("a", "b", "c"), abcExistsSelectivity, abcUniqueSelectivity)
      .addNodeIndex("A", Seq("a", "b"), abExistsSelectivity, abUniqueSelectivity)
      .addNodeIndex("A", Seq("c", "d"), cdExistsSelectivity, cdUniqueSelectivity)
      .build()

    val query = "MATCH (n:A {a: 1, b: 2, c: 3, d: 4}) RETURN n"
    val planState = planner.planState(query)

    val cdCardinality = aNodes * cdExistsSelectivity * cdUniqueSelectivity

    val abcCardinality =
      aNodes * abcExistsSelectivity * abcUniqueSelectivity *
        (PlannerDefaults.DEFAULT_EQUALITY_SELECTIVITY * PlannerDefaults.DEFAULT_PROPERTY_SELECTIVITY).factor // `d = 4` predicate is not covered by :A(a,b,c) index

    val expectedPlanBuilder = planner.planBuilder()
      .produceResults("n").withCardinality(abcCardinality)
      .filterExpression(andsReorderable("n.a = 1", "n.b = 2")).withCardinality(abcCardinality)
      .nodeIndexOperator("n:A(c = 3, d = 4)", supportPartitionedScan = false).withCardinality(cdCardinality)

    planState should haveSamePlanAndCardinalitiesAsBuilder(expectedPlanBuilder)
  }

  test("should use the widest applicable composite index to estimate cardinality of a nested index join") {
    val aNodes = 900

    val abcExistsSelectivity = 0.5
    val abcUniqueSelectivity = 0.3

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", aNodes)
      .addNodeIndex("A", Seq("a", "b", "c"), abcExistsSelectivity, abcUniqueSelectivity)
      .addNodeIndex("A", Seq("a", "b"), existsSelectivity = 0.9, uniqueSelectivity = 0.5)
      .addNodeIndex("A", Seq("id"), existsSelectivity = 1, uniqueSelectivity = 1.0 / aNodes, isUnique = true)
      .build()

    val query = "MATCH (x:A {id: 123}), (n:A {a: x.prop, b: 2, c: 3}) RETURN n"
    val planState = planner.planState(query)

    val expectedCardinality = aNodes * abcExistsSelectivity * abcUniqueSelectivity

    val expectedPlanBuilder = planner.planBuilder()
      .produceResults("n").withCardinality(expectedCardinality)
      .apply().withCardinality(expectedCardinality)
      .|.nodeIndexOperator(
        "n:A(a = ???, b = 2, c = 3)",
        argumentIds = Set("x"),
        paramExpr = Some(prop("x", "prop")),
        supportPartitionedScan = false
      ).withCardinality(expectedCardinality)
      .nodeIndexOperator("x:A(id = 123)", unique = true, supportPartitionedScan = false).withCardinality(1.0)

    planState should haveSamePlanAndCardinalitiesAsBuilder(expectedPlanBuilder)
  }

  test("should use the widest applicable composite relationship index to estimate cardinality of a nested index join") {
    val relCount = 900

    val abcExistsSelectivity = 0.5
    val abcUniqueSelectivity = 0.3

    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", relCount)
      .setRelationshipCardinality("()-[:KNOWS]->()", relCount)
      .addRelationshipIndex("REL", Seq("a", "b", "c"), abcExistsSelectivity, abcUniqueSelectivity)
      .addRelationshipIndex("REL", Seq("a", "b"), existsSelectivity = 0.9, uniqueSelectivity = 0.5)
      .addRelationshipIndex(
        "KNOWS",
        Seq("id"),
        existsSelectivity = 1,
        uniqueSelectivity = 1.0 / relCount,
        isUnique = true
      )
      .build()

    val query =
      """
        |MATCH
        |  (a)-[r:KNOWS {id: 123}]->(b),
        |  (x)-[q:REL {a: r.prop, b: 2, c: 3}]->(y)
        |RETURN q
        |""".stripMargin

    val planState = planner.planState(query)

    val expectedCardinality = relCount * abcExistsSelectivity * abcUniqueSelectivity

    val expectedPlanBuilder = planner.planBuilder()
      .produceResults("q").withCardinality(expectedCardinality)
      .apply().withCardinality(expectedCardinality)
      .|.relationshipIndexOperator(
        "(x)-[q:REL(a = ???, b = 2, c = 3)]->(y)",
        argumentIds = Set("r", "a", "b"),
        paramExpr = Some(prop("r", "prop")),
        supportPartitionedScan = false
      ).withCardinality(expectedCardinality)
      .relationshipIndexOperator(
        "(a)-[r:KNOWS(id = 123)]->(b)",
        unique = true,
        supportPartitionedScan = false
      ).withCardinality(1.0)

    planState should haveSamePlanAndCardinalitiesAsBuilder(expectedPlanBuilder)
  }
}
