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
import org.neo4j.cypher.internal.compiler.CypherPlannerTestSuite
import org.neo4j.cypher.internal.compiler.planner.AttributeComparisonStrategy
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningAttributesTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder.IndexCapabilities.text_2_0
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.ir.EagernessReason.Conflict
import org.neo4j.cypher.internal.ir.EagernessReason.PropertyReadSetConflict
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.runtime.ast.PropertiesUsingCachedProperties
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.constraints.SchemaValueType

import scala.concurrent.Await
import scala.concurrent.Future
import scala.concurrent.duration.DurationInt

class IndexPlanningIntegrationTest
    extends CypherPlannerTestSuite
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
      .registerTemporalFunctions()
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

  test("should consider starting in a union of unique index seeks") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(1000)
        .setLabelCardinality("A", 200)
        .setLabelCardinality("B", 300)
        .setLabelCardinality("C", 250)
        .setRelationshipCardinality("()-[:REL]->()", 2000)
        // It is quite expensive to start in :C because only the labels on the other side will reduce the cardinality significantly
        .setRelationshipCardinality("()-[:REL]->(:C)", 1000)
        .setRelationshipCardinality("(:A)-[:REL]->()", 10)
        .setRelationshipCardinality("(:A)-[:REL]->(:C)", 10)
        .setRelationshipCardinality("(:B)-[:REL]->()", 10)
        .setRelationshipCardinality("(:B)-[:REL]->(:C)", 10)
        .addNodeIndex("A", Seq("id"), 0.1, 0.01, isUnique = true)
        .addNodeIndex("B", Seq("id"), 0.1, 0.01, isUnique = true)
        .addNodeIndex("C", Seq("id"), 0.1, 0.01, isUnique = true)
        .build()

    planner.plan(
      """MATCH (a:A|B {id: $param})-[r:REL]->(c:C {id: $param2})
        |RETURN r
        |""".stripMargin
    ).stripProduceResults should equal(
      planner.subPlanBuilder()
        .filter("c.id = $param2", "c:C")
        .expandAll("(a)-[r:REL]->(c)")
        .distinct("a AS a")
        .union()
        .|.nodeIndexOperator("a:B(id = ???)", paramExpr = Seq(parameter("param", CTAny)), unique = true)
        .nodeIndexOperator("a:A(id = ???)", paramExpr = Seq(parameter("param", CTAny)), unique = true)
        .build()
    )
  }

  test("should be able to plan a label disjunction if one of the labels causes increase in cardinality estimates") {
    val planner =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setLabelCardinality("A", 80)
        .setLabelCardinality("B", 40)
        .addNodeIndex("A", Seq("prop"), 1, 0.01)
        .build()

    planner.plan(
      """MATCH (n:A|B)
        |WHERE n.prop IS NOT NULL
        |RETURN n""".stripMargin
    ).stripProduceResults should equal(
      planner.subPlanBuilder()
        .distinct("n AS n")
        .union()
        .|.filter("cacheNFromStore[n.prop] IS NOT NULL")
        .|.nodeByLabelScan("n", "B", IndexOrderAscending)
        .nodeIndexOperator("n:A(prop)", getValue = _ => GetValue)
        .build()
    )
  }

  test("should plan ranged index usage if predicate depends on simple variable from horizon") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    for (op <- List("=", "<", "<=", ">", ">=", "STARTS WITH")) {
      val plan =
        cfg.plan(s"WITH 'foo' AS foo MATCH (a)-[r]->(b:Label) WHERE b.prop $op foo RETURN a").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .expandAll("(b)<-[]-(a)")
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

    for (pred <- scannablePointPredicates("a.prop")) {
      val plan = cfg.plan(s"MATCH (a:Label) WHERE ${pred.cypher} RETURN a").stripProduceResults

      plan shouldEqual cfg.subPlanBuilder()
        .filter(pred.planWithCachedProp)
        .nodeIndexOperator("a:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }
  }

  test("should plan range index node scan for existence predicate") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()

    val plan = cfg.plan(s"MATCH (a:Label) WHERE a.prop IS NOT NULL RETURN a").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .nodeIndexOperator("a:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
      .build()
  }

  test("should plan range index relationship scan for existence predicate") {
    val cfg = plannerConfigForRangeIndexOnRelationshipTypePropTests()

    val plan = cfg.plan(s"MATCH (a)-[r:Type]->(b) WHERE r.prop IS NOT NULL RETURN r").stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .relationshipIndexOperator("()-[r:Type(prop)]->()", _ => GetValue, indexType = IndexType.RANGE)
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
            .produceResults(column("a", "cacheN[a.prop1]", "cacheN[a.prop2]"))
            .nodeIndexOperator(
              s"a:Label(prop1 $op1 1, prop2 $op2 2)",
              _ => GetValue,
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
            .produceResults(column("a", "cacheN[a.prop2]", "cacheN[a.prop1]"))
            .filter(s"cacheN[a.prop2] $op2 2")
            .nodeIndexOperator(
              s"a:Label(prop1 $op1 1, prop2)",
              _ => GetValue,
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
            .produceResults(column("r", "cacheR[r.prop1]", "cacheR[r.prop2]"))
            .relationshipIndexOperator(
              s"()-[r:Type(prop1 $op1 1, prop2 $op2 2)]->()",
              _ => GetValue,
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
            .produceResults(column("r", "cacheR[r.prop2]", "cacheR[r.prop1]"))
            .filter(s"cacheR[r.prop2] $op2 2")
            .relationshipIndexOperator(
              s"()-[r:Type(prop1 $op1 1, prop2)]->()",
              _ => GetValue,
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
        .|.nodeIndexOperator(
          s"a:Label(prop $op point)",
          _ => GetValue,
          argumentIds = Set("point"),
          indexType = IndexType.RANGE
        )
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
        .filter(s"cacheN[b.prop] $op a.prop")
        .expandAll("(b)<-[]-(a)")
        .nodeIndexOperator("b:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
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
        .expandAll("(b)<-[]-(a)")
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
        .expandAll("(b)<-[]-(a)")
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
        .expandAll("(b)<-[]-(a)")
        .filter(s"cacheN[b.prop] $op 'test'")
        .nodeIndexOperator("b:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
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
        "x.maxDistance >= point.distance(cacheN[p.location], point({x: 0, y: 0, crs: 'cartesian'}))",
        "x:Preference"
      )
      .expandAll("(p)-[]->(x)")
      .nodeIndexOperator(
        "p:Place(location)",
        _ => GetValue,
        indexType = IndexType.POINT,
        supportPartitionedScan = false
      )
      .build()
  }

  test("should plan index usage if distance predicate depends on variable from the horizon") {
    val query =
      """WITH 10 AS maxDistance
        |MATCH (p:Place)
        |WHERE point.distance(p.location, point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance
        |RETURN p.location as point
        """.stripMargin

    val cfg = plannerConfigWithPointIndexForDistancePredicateTests()
    val plan = cfg.plan(query).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[p.location] AS point")
      .filter("point.distance(cacheN[p.location], point({x: 0, y: 0, crs: 'cartesian'})) <= maxDistance")
      .apply()
      .|.pointDistanceNodeIndexSeek(
        "p",
        "Place",
        "location",
        "{x: 0, y: 0, crs: 'cartesian'}",
        distance = v"maxDistance",
        argumentIds = Set("maxDistance"),
        inclusive = true,
        getValue = GetValue,
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
      .filter("x.maxDistance >= point.distance(cacheN[p.location], point({x: 0, y: 0, crs: 'cartesian'}))")
      .apply()
      .|.pointDistanceNodeIndexSeek(
        "p",
        "Place",
        "location",
        "{x: 0, y: 0, crs: 'cartesian'}",
        distance = prop("x", "maxDistance"),
        argumentIds = Set("x"),
        inclusive = true,
        getValue = GetValue,
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
      .produceResults(column("s", "cacheN[s.p]"), column("r"), column("t", "cacheN[t.p]"))
      .nodeHashJoin("t")
      .|.expandAll("(s)<-[r]-(t)")
      .|.nodeIndexOperator("s:S(p = 10)", _ => GetValue, indexType = IndexType.RANGE)
      .filter("t.foo = 2")
      .nodeIndexOperator("t:T(0 <= p <= 10)", _ => GetValue, indexType = IndexType.RANGE)
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
      .produceResults(column("s", "cacheN[s.p]"), column("r"), column("t"))
      .nodeHashJoin("t")
      .|.expandAll("(s)<-[r]-(t)")
      .|.nodeIndexOperator("s:S(p = 10)", _ => GetValue, indexType = IndexType.RANGE)
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
      .|.expandInto("(a)-[]-(b)")
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
      .relationshipTypeScan("()-[r:REL]->()")
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
      .projection("cacheN[n.prop] AS p")
      .nodeIndexOperator("n:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
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
      .projection("cacheR[r.prop] AS p")
      .relationshipIndexOperator("()-[r:REL(prop)]->()", _ => GetValue, indexType = IndexType.RANGE)
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
      .projection("cacheN[n.prop] AS p")
      .filter("n.x = 1")
      .nodeIndexOperator("n:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
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
      .projection("cacheR[r.prop] AS p")
      .filter("r.x = 1")
      .relationshipIndexOperator("()-[r:REL(prop)]->()", _ => GetValue, indexType = IndexType.RANGE)
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
      .aggregation(Seq(), Seq("count(cacheN[n.counted]) AS c"))
      .nodeIndexOperator("n:Label(counted)", _ => GetValue, indexType = IndexType.RANGE)
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
      .aggregation(Seq(), Seq("count(cacheR[r.counted]) AS c"))
      .relationshipIndexOperator("()-[r:REL(counted)]->()", _ => GetValue, indexType = IndexType.RANGE)
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
      .aggregation(Seq(), Seq("count(cacheN[n.counted]) AS c"))
      .filter("n.x = 1")
      .nodeIndexOperator("n:Label(counted)", _ => GetValue, indexType = IndexType.RANGE)
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
      .aggregation(Seq(), Seq("count(cacheR[r.counted]) AS c"))
      .filter("r.x = 1")
      .relationshipIndexOperator("()-[r:REL(counted)]->()", _ => GetValue, indexType = IndexType.RANGE)
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
      .aggregation(Seq(), Seq("count(cacheN[n.counted]) AS c"))
      .filter("not n.prop = 1", "n.x = 1")
      .nodeIndexOperator("n:Label(counted)", _ => GetValue, indexType = IndexType.RANGE)
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
      .aggregation(Seq(), Seq("count(cacheR[r.counted]) AS c"))
      .filter("not r.prop = 1", "r.x = 1")
      .relationshipIndexOperator("()-[r:REL(counted)]->()", _ => GetValue, indexType = IndexType.RANGE)
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
      .filter("n.prop IS NULL")
      .apply()
      .|.nodeByLabelScan("n", "Label", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
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
      .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(0)))))
      .apply()
      .|.setNodeProperty("n", "prop", "c")
      .|.eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(5)))))
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
      .filter("n.prop IS NULL")
      .nodeByLabelScan("n", "Label")
      .build()
  }

  test("should plan node index scan with existence constraint if query has updates after the match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0, indexType = IndexType.RANGE)
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
      .aggregation(Seq.empty, Seq("sum(cacheN[n.prop]) AS result"))
      .nodeIndexOperator("n:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
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
      .filter("r.prop IS NULL")
      .apply()
      .|.relationshipTypeScan("()-[r:REL]->()", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
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
      .eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(0)))))
      .apply()
      .|.setRelationshipProperty("r", "prop", "c")
      .|.eager(ListSet(PropertyReadSetConflict(propName("prop")).withConflict(Conflict(Id(3), Id(5)))))
      .|.filter("r.prop IS NULL")
      .|.relationshipTypeScan("()-[r:REL]->()", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
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
      .filter("r.prop IS NULL")
      .relationshipTypeScan("()-[r:REL]->()")
      .build()
  }

  test("should plan relationship index scan with existence constraint if query has updates after the match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 100)
      .addRelationshipIndex("REL", Seq("prop"), 1.0, 1.0)
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
      .relationshipIndexOperator(
        "()-[r1:REL(prop)]->()",
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
      .aggregation(Seq.empty, Seq("sum(cacheR[r.prop]) AS result"))
      .relationshipIndexOperator("()-[r:REL(prop)]->()", _ => GetValue, indexType = IndexType.RANGE)
      .build()
  }

  test("should plan index usage for node pattern with IS NOT NULL predicate") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()
    val plan = cfg.plan("MATCH (a:Label WHERE a.prop IS NOT NULL) RETURN a, a.prop").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .nodeIndexOperator("a:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
      .build()
  }

  test("should plan index usage for a pattern comprehension with IS NOT NULL predicate") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()
    val plan =
      cfg.plan("RETURN [ (a:Label)-[r]->(b) WHERE a.prop IS NOT NULL | [a, a.prop] ] AS result ").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .rollUpApply("result", "anon_0")
      .|.projection("[a, cacheN[a.prop]] AS anon_0")
      .|.expandAll("(a)-[]->()")
      .|.nodeIndexOperator("a:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
      .argument()
      .build()
  }

  test("should plan index usage for a pattern comprehension with IS NOT NULL predicate inside a node pattern") {
    val cfg = plannerConfigForRangeIndexOnLabelPropTests()
    val plan =
      cfg.plan("RETURN [ (a:Label WHERE a.prop IS NOT NULL)-[r]->(b) | [a, a.prop] ] AS result ").stripProduceResults

    plan shouldBe cfg.subPlanBuilder()
      .rollUpApply("result", "anon_0")
      .|.projection("[a, cacheN[a.prop]] AS anon_0")
      .|.expandAll("(a)-[]->()")
      .|.nodeIndexOperator("a:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
      .argument()
      .build()
  }

  private def plannerConfigForNodeTextIndex: StatisticsBackedLogicalPlanningConfiguration =
    plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0.5, uniqueSelectivity = 0.1, indexType = IndexType.TEXT)
      .build()

  test("should plan node exists instead of NodeIndexContainsScan for a.prop CONTAINS a.prop (issue-13489)") {
    // we need to use selectivity of 0 to force the planner to select NodeIndexContainsScan.
    // in general the planner will choose the NodeIndexScan since it is has a lower heuristic cost.
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0, uniqueSelectivity = 0, indexType = IndexType.TEXT)
      .build()

    val plan = planner.plan("MATCH (a:A) WHERE a.prop CONTAINS a.prop RETURN a.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("cacheNFromStore[a.prop] CONTAINS cacheNFromStore[a.prop]")
      .nodeIndexOperator(
        "a:A(prop)",
        getValue = Map("prop" -> DoNotGetValue),
        indexType = IndexType.TEXT,
        supportPartitionedScan = false
      )
      .build()
  }

  test("should plan node exists instead of NodeIndexEndsWithScan for a.prop ENDS WITH a.prop (issue-13489)") {
    // we need to use selectivity of 0 to force the planner to select NodeIndexEndsWithScan.
    // in general the planner will choose the NodeIndexScan since it is has a lower heuristic cost.
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), existsSelectivity = 0, uniqueSelectivity = 0, indexType = IndexType.TEXT)
      .build()

    val plan = planner.plan("MATCH (a:A) WHERE a.prop ENDS WITH a.prop RETURN a.prop").stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .filter("cacheNFromStore[a.prop] ENDS WITH cacheNFromStore[a.prop]")
      .nodeIndexOperator(
        "a:A(prop)",
        getValue = Map("prop" -> DoNotGetValue),
        indexType = IndexType.TEXT,
        supportPartitionedScan = false
      )
      .build()
  }

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
      .filter("cacheN[a.prop] IS :: STRING NOT NULL")
      .nodeIndexOperator("a:Label(prop)", _ => GetValue, indexType = IndexType.RANGE)
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
      .relationshipTypeScan("()-[r:REL]->()")
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
        _ => GetValue,
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
      .nodeIndexOperator(s"a:A(prop)", _ => GetValue, indexType = IndexType.POINT, supportPartitionedScan = false)
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
      .setRelationshipCardinality("()-[:REL]->(:A)", 0)
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
      .setRelationshipCardinality("()-[:REL]->(:A)", 0)
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
      .setRelationshipCardinality("()-[:REL]->(:A)", 0)
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
      .filter("cacheR[r.prop] = a.prop")
      .relationshipIndexOperator(
        s"(a)-[r:REL(prop)]->(b)",
        _ => GetValue,
        indexType = IndexType.POINT,
        supportPartitionedScan = false
      )
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
          _ => GetValue,
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
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op 'hello' RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator(s"a:A(prop $op 'hello')", _ => GetValue, indexType = IndexType.RANGE)
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
          s"()-[r:REL(prop $op 'hello')]->()",
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
          s"()-[r:REL(prop $op 'hello')]->()",
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
        .relationshipIndexOperator("()-[r:REL(prop)]->()", indexType = IndexType.TEXT, supportPartitionedScan = false)
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
          s"()-[r:REL(prop $op 'hello')]->()",
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
        .relationshipIndexOperator("()-[r:REL(prop)]->()", indexType = IndexType.TEXT, supportPartitionedScan = false)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          s"()-[r:REL(prop $op 'hello')]->()",
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
        .relationshipIndexOperator(s"()-[r:REL(prop $op 'hello')]->()", indexType = IndexType.TEXT)
        .build()
    }

    for (op <- List("STARTS WITH", "<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(s"()-[r:REL(prop $op 'hello')]->()", _ => GetValue, indexType = IndexType.RANGE)
        .build()
    }

    for (op <- List("=")) {
      val plan = cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op 'hello' RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator(
          s"()-[r:REL(prop $op 'hello')]->()",
          _ => GetValue,
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
        "()-[r:R(prop = 'string')]->()",
        getValue = Map("prop" -> GetValue),
        indexType = IndexType.TEXT,
        supportPartitionedScan = false
      )
      .build()

    for (arg <- List("3", "[\"a\", \"b\", \"c\"]", "$param")) {
      for (op <- List("<", "<=", ">", ">=", "=")) {
        val query = queryStr(op, arg)
        val plan = cfg.plan(query).stripProduceResults
        withClue(query) {
          plan shouldEqual cfg.subPlanBuilder()
            .projection("cacheR[r.prop] AS `r.prop`")
            .filter(s"cacheRFromStore[r.prop] $op $arg")
            .relationshipTypeScan("()-[r:R]->()")
            .build()
        }
      }
    }

    // expressions with dependencies on source node
    for (arg <- List("a.prop2", "a")) {
      for (op <- List("<", "<=", ">", ">=", "=")) {
        val query = queryStr(op, arg)
        val plan = cfg.plan(query).stripProduceResults
        withClue(query) {
          plan shouldEqual cfg.subPlanBuilder()
            .projection("cacheR[r.prop] AS `r.prop`")
            .filter(s"cacheRFromStore[r.prop] $op $arg")
            .relationshipTypeScan("(a)-[r:R]->()")
            .build()
        }
      }
    }

    // expressions with dependencies on target node
    for (arg <- List("b.prop2", "b")) {
      for (op <- List("<", "<=", ">", ">=", "=")) {
        val query = queryStr(op, arg)
        val plan = cfg.plan(query).stripProduceResults
        withClue(query) {
          plan shouldEqual cfg.subPlanBuilder()
            .projection("cacheR[r.prop] AS `r.prop`")
            .filter(s"cacheRFromStore[r.prop] $op $arg")
            .relationshipTypeScan("()-[r:R]->(b)")
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
        .produceResults(column("a", "cacheN[a.prop]"), column("a.prop"))
        .projection("cacheN[a.prop] AS `a.prop`")
        .nodeIndexOperator("a:A(prop > 10)", _ => GetValue, indexType = IndexType.RANGE)
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
        .produceResults(column("r", "cacheR[r.prop]"), column("r.prop"))
        .projection("cacheR[r.prop] AS `r.prop`")
        .relationshipIndexOperator("()-[r:R(prop > 10)]->()", _ => GetValue, indexType = IndexType.RANGE)
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
      .filter("point.distance(cacheN[a.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceNodeIndexSeek("a", "A", "prop", "{x:1, y:2}", 1.0, getValue = GetValue, indexType = IndexType.POINT)
      .build()
  }

  test("should plan node point index usage for point.withinBBox") {
    val cfg = plannerConfigForNodePointIndex
    val plan = cfg.plan(
      s"MATCH (a:A) WHERE point.withinBBox(a.prop, point({x:1, y:2}), point({x:3, y:4})) RETURN a, a.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheN[a.prop] AS `a.prop`")
      .pointBoundingBoxNodeIndexSeek(
        "a",
        "A",
        "prop",
        "{x:1, y:2}",
        "{x:3, y:4}",
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .build()
  }

  test("should plan node point index usage for inequalities with point literals") {
    val cfg = plannerConfigForNodePointIndex
    for (op <- Seq("<", "<=", ">", ">=")) {
      val plan = cfg.plan(s"MATCH (a:A) WHERE a.prop $op point({x:1, y:2}) RETURN a, a.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheN[a.prop] AS `a.prop`")
        .filter(s"cacheN[a.prop] $op point({x:1, y:2})")
        .nodeIndexOperator("a:A(prop)", _ => GetValue, indexType = IndexType.POINT, supportPartitionedScan = false)
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
      .projection("cacheN[a.prop] AS `a.prop`")
      .nodeIndexOperator("a:A(prop)", _ => GetValue, indexType = IndexType.POINT, supportPartitionedScan = false)
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
        "()-[r:REL(prop = ???)]->()",
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
      .filter("point.distance(cacheR[r.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceRelationshipIndexSeek(
        "()-[r:REL(prop)]->()",
        "{x:1, y:2}",
        1.0,
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .build()
  }

  test("should plan relationship point index usage for point.distance (case insensitive)") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan(
      s"MATCH (a)-[r:REL]->(b) WHERE pOinT.dIsTaNcE(r.prop, point({x:1, y:2})) < 1.0 RETURN r, r.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .filter("pOinT.dIsTaNcE(cacheR[r.prop], point({x:1, y:2})) < 1.0")
      .pointDistanceRelationshipIndexSeek(
        "()-[r:REL(prop)]->()",
        "{x:1, y:2}",
        1.0,
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .build()
  }

  test("should plan relationship point index usage for point.withinBBox") {
    val cfg = plannerConfigForRelPointIndex
    val plan = cfg.plan(
      s"MATCH (a)-[r:REL]->(b) WHERE point.withinBBox(r.prop, point({x:1, y:2}), point({x:3, y:4})) RETURN r, r.prop"
    ).stripProduceResults
    plan shouldEqual cfg.subPlanBuilder()
      .projection("cacheR[r.prop] AS `r.prop`")
      .pointBoundingBoxRelationshipIndexSeek(
        "()-[r:REL(prop)]->()",
        "{x:1, y:2}",
        "{x:3, y:4}",
        getValue = GetValue,
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
      .projection("cacheN[a.prop] AS `a.prop`")
      .pointBoundingBoxNodeIndexSeek(
        "a",
        "A",
        "prop",
        "{x:1, y:2}",
        "{x:3, y:4}",
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .build()
  }

  test("should plan relationship point index scan for inequalities") {
    val cfg = plannerConfigForRelPointIndex
    for (op <- Seq("<", "<=", ">", ">=")) {
      val plan =
        cfg.plan(s"MATCH (a)-[r:REL]->(b) WHERE r.prop $op point({x:1, y:2}) RETURN r, r.prop").stripProduceResults
      plan shouldEqual cfg.subPlanBuilder()
        .projection("cacheR[r.prop] AS `r.prop`")
        .filter(s"cacheR[r.prop] $op point({x:1, y:2})")
        .relationshipIndexOperator(
          "()-[r:REL(prop)]->()",
          _ => GetValue,
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
      .relationshipTypeScan("()-[r:REL]->()")
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
      .produceResults(column("a", "cacheN[a.prop]"))
      .filter(
        "cacheN[a.prop] CONTAINS 'hello'",
        "cacheN[a.prop] CONTAINS 'world'"
      )
      .nodeIndexOperator("a:A(prop)", _ => GetValue).withCardinality(500)

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
      .produceResults(column("r", "cacheR[r.prop]"))
      .filter(
        "cacheR[r.prop] CONTAINS 'hello'",
        "cacheR[r.prop] CONTAINS 'world'"
      )
      .relationshipIndexOperator("()-[r:REL(prop)]->()", _ => GetValue).withCardinality(500)

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
      .expandAll("(n)-[]->()")
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
      .produceResults(column("u", "cacheN[u.id]")).withCardinality(1)
      .nodeIndexOperator("u:User(id = 123)", _ => GetValue, unique = true).withCardinality(1)

    val actual = planner.planState(query)
    actual should haveSamePlanAndEffectiveCardinalitiesAsBuilder(expected)
  }

  case class ScannablePredicate(
    cypher: String,
    planWithCachedProp: String
  )

  object ScannablePredicate {

    def apply(prop: String, fCypher: String => String, cacheSuffix: String): ScannablePredicate =
      ScannablePredicate(fCypher(prop), fCypher(s"cache$cacheSuffix[$prop]"))
  }

  private def scannablePredicates(prop: String, cacheSuffix: String = "N"): Seq[ScannablePredicate] = {
    val binaryOpPredicates =
      Seq("=", ">", ">=", "<", "<=")
        .map(op => ScannablePredicate(prop, (x: String) => s"$x $op $$param", cacheSuffix))

    binaryOpPredicates ++ scannableTextPredicates(prop, cacheSuffix) ++ scannablePointPredicates(prop, cacheSuffix)
  }

  private def scannableTextPredicates(prop: String, cacheSuffix: String = "N"): Seq[ScannablePredicate] = {
    Seq("STARTS WITH", "ENDS WITH", "CONTAINS")
      .map(op => ScannablePredicate(prop, (x: String) => s"$x $op $$param", cacheSuffix))
  }

  private def scannablePointPredicates(prop: String, cacheSuffix: String = "N"): Seq[ScannablePredicate] = {
    Seq(
      ScannablePredicate(prop, (x: String) => s"point.withinBBox($x, $$p1, $$p2)", cacheSuffix),
      ScannablePredicate(prop, (x: String) => s"point.distance($x, $$p1) < $$p2", cacheSuffix)
    )
  }

  test("should plan a node index scan for negated predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), 0.1, 0.1)
      .build()

    for (pred <- scannablePredicates("a.prop")) {
      val q = s"MATCH (a:A) WHERE NOT ${pred.cypher} RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.planWithCachedProp}")
        .nodeIndexOperator("a:A(prop)", _ => GetValue)
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
      val q = s"MATCH (a:A) WHERE NOT ${pred.cypher} AND a.otherProp IS NOT NULL RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.planWithCachedProp}")
        .nodeIndexOperator("a:A(prop, otherProp)", _ => GetValue)
        .build()
    }
  }

  test("should plan a relationship index scan for negated predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1)
      .build()

    for (pred <- scannablePredicates("r.prop", "R")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT ${pred.cypher} RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.planWithCachedProp}")
        .relationshipIndexOperator("(a)-[r:REL(prop)]->(b)", _ => GetValue)
        .build()
    }
  }

  test("should plan a composite relationship index scan for negated predicates") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop", "otherProp"), 0.1, 0.1)
      .build()

    for (pred <- scannablePredicates("r.prop", "R")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT ${pred.cypher} AND r.otherProp IS NOT NULL RETURN *"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.planWithCachedProp}")
        .relationshipIndexOperator(
          "(a)-[r:REL(prop, otherProp)]->(b)",
          _ => GetValue
        )
        .build()
    }
  }

  test("should plan node text index scan with negated predicate") {
    val planner = plannerConfigForNodeTextIndex

    for (pred <- scannableTextPredicates("a.prop")) {
      val q = s"MATCH (a:A) WHERE NOT ${pred.cypher} RETURN a"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.cypher}")
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

    for (pred <- scannableTextPredicates("r.prop", "R")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT ${pred.cypher} RETURN r"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.cypher}")
        .relationshipIndexOperator(
          "()-[r:REL(prop)]->()",
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
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
      val q = s"MATCH (a:A) WHERE NOT ${pred.cypher} RETURN a"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.planWithCachedProp}")
        .nodeIndexOperator("a:A(prop)", _ => GetValue, indexType = IndexType.POINT, supportPartitionedScan = false)
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

    for (pred <- scannablePointPredicates("r.prop", "R")) {
      val q = s"MATCH (a)-[r:REL]->(b) WHERE NOT ${pred.cypher} RETURN r"
      val plan = planner.plan(q).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"NOT ${pred.planWithCachedProp}")
        .relationshipIndexOperator(
          "()-[r:REL(prop)]->()",
          _ => GetValue,
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
      .produceResults(column("a", "cacheN[a.prop]"))
      .filter("cacheN[a.prop] < point({x:1, y:2})")
      .nodeIndexOperator(
        "a:A(prop)",
        _ => GetValue,
        indexType = IndexType.POINT,
        supportPartitionedScan = false
      ).withCardinality(
        labelCardinality * existsSelectivity
      )

    planState should haveSamePlanAndCardinalitiesAsBuilder(
      expected,
      AttributeComparisonStrategy.ComparingProvidedAttributesOnly
    )
  }

  test("should stringify index seek with datetime function properly") {
    val planner = plannerConfigForRangeIndexOnLabelPropTests()

    planner.plan(
      """MATCH (a:Label)
        |  WHERE datetime('2021-01-01') < a.prop
        |RETURN a""".stripMargin
    ).asLogicalPlanBuilderString() should equal(
      """.produceResults(column("a", "cacheN[a.prop]"))
        |.nodeIndexOperator("a:Label(prop > RuntimeConstant(anon_0, datetime('2021-01-01')))", indexOrder = IndexOrderNone, paramExpr = Seq(), argumentIds = Set(), getValue = Map("prop" -> GetValue), unique = false, indexType = IndexType.RANGE, supportPartitionedScan = true)
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
      .produceResults(column("n", "cacheN[n.a]", "cacheN[n.b]", "cacheN[n.c]")).withCardinality(expectedCardinality)
      .nodeIndexOperator("n:A(a = 1, b = 2, c = 3)", _ => GetValue, supportPartitionedScan = false).withCardinality(
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
      .produceResults(column("n", "cacheN[n.c]", "cacheN[n.d]")).withCardinality(abCardinality)
      .filter(andsReorderable("n.a = 1", "n.b = 2")).withCardinality(abCardinality)
      .nodeIndexOperator("n:A(c = 3, d = 4)", _ => GetValue, supportPartitionedScan = false).withCardinality(
        cdCardinality
      )

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
      .produceResults(column("n", "cacheN[n.c]", "cacheN[n.d]")).withCardinality(abcCardinality)
      .filter(andsReorderable("n.a = 1", "n.b = 2")).withCardinality(abcCardinality)
      .nodeIndexOperator("n:A(c = 3, d = 4)", _ => GetValue, supportPartitionedScan = false).withCardinality(
        cdCardinality
      )

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
      .produceResults(column("n", "cacheN[n.a]", "cacheN[n.b]", "cacheN[n.c]")).withCardinality(expectedCardinality)
      .apply().withCardinality(expectedCardinality)
      .|.nodeIndexOperator(
        "n:A(a = ???, b = 2, c = 3)",
        _ => GetValue,
        argumentIds = Set("x"),
        paramExpr = Some(prop("x", "prop")),
        supportPartitionedScan = false
      ).withCardinality(expectedCardinality)
      .nodeIndexOperator("x:A(id = 123)", unique = true).withCardinality(1.0)

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
      .produceResults(column("q", "cacheR[q.a]", "cacheR[q.b]", "cacheR[q.c]")).withCardinality(expectedCardinality)
      .apply().withCardinality(expectedCardinality)
      .|.relationshipIndexOperator(
        "()-[q:REL(a = ???, b = 2, c = 3)]->()",
        _ => GetValue,
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

  test("should cache properties of returned indexed nodes when relying on index ordering") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("L", 800)
      .addNodeIndex("L", Seq("x"), 1, 0.1)
      .build()

    val q =
      """
        |MATCH (n:L)
        |WHERE n.x IS NOT NULL
        |RETURN n ORDER BY n.x
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .nodeIndexOperator("n:L(x)", indexOrder = IndexOrderAscending, getValue = Map("x" -> GetValue))
      .build()
  }

  test("should cache properties of returned indexed nodes on both sides of a union when relying on index ordering") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("L", 800)
      .addNodeIndex("L", Seq("x"), 1, 0.1)
      .build()

    val q =
      """
        |MATCH (a:L)
        |WHERE a.x IS NOT NULL
        |RETURN a AS n ORDER BY n.x ASC
        | UNION ALL
        |MATCH (b:L)
        |WHERE b.x IS NOT NULL
        |RETURN b AS n ORDER BY n.x DESC
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .union()
      .|.projection("n AS n")
      .|.projection("b AS n")
      .|.nodeIndexOperator("b:L(x)", indexOrder = IndexOrderDescending, getValue = Map("x" -> GetValue))
      .projection("n AS n")
      .projection("a AS n")
      .nodeIndexOperator("a:L(x)", indexOrder = IndexOrderAscending, getValue = Map("x" -> GetValue))
      .build()
  }

  test("should cache properties of indexed nodes passed to properties function") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("L", 800)
      .addNodeIndex("L", Seq("x"), 1, 0.1)
      .build()

    val q =
      """
        |MATCH (n:L)
        |WHERE n.x IS NOT NULL
        |RETURN properties(n) ORDER BY n.x
        |""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .projection(Map("properties(n)" -> PropertiesUsingCachedProperties(varFor("n"), Set(cachedNodeProp("n", "x")))))
      .nodeIndexOperator("n:L(x)", indexOrder = IndexOrderAscending, getValue = Map("x" -> GetValue))
      .build()
  }

  test("should not plan an index scan for NOT ANY over empty list") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(2)
      .setLabelCardinality("N0", 2)
      .setLabelCardinality("N1", 0)
      .addNodeIndex("N0", Seq("p1"), 0.0, 0.0)
      .build()

    val q =
      """
        |MATCH (n:N1) WITH collect(n.p0) AS c0
        |MATCH (n:N0)
        |WHERE NOT any(v IN c0 WHERE n.p1 = v)
        |RETURN *""".stripMargin

    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("NOT n.p1 IN c0")
      .apply()
      .|.nodeByLabelScan("n", "N0", IndexOrderNone, "c0")
      .aggregation(Seq(), Seq("collect(n.p0) AS c0"))
      .nodeByLabelScan("n", "N1")
      .build()
  }

  test("should not plan a node index scan for NOT IN unknown list") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop"), 0.1, 0.1)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1)
      .build()

    val q = "MATCH (a:A) WHERE NOT a.prop IN $param RETURN *"
    val plan = planner.plan(q).stripProduceResults

    plan shouldEqual planner.subPlanBuilder()
      .filter("NOT a.prop IN $param")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should not plan a composite node index scan for NOT IN unknown list") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 500)
      .addNodeIndex("A", Seq("prop", "otherProp"), 0.1, 0.1)
      .build()

    val q = "MATCH (a:A) WHERE NOT a.prop IN $param AND a.otherProp IS NOT NULL RETURN *"
    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("NOT a.prop IN $param", "a.otherProp IS NOT NULL")
      .nodeByLabelScan("a", "A")
      .build()
  }

  test("should plan a composite index with a partial predicate as a part of a disjunction") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(10_000)
      .setLabelCardinality("A", 1_500)
      .setLabelCardinality("B", 7_500)
      .addNodeIndex("A", Seq("prop1", "prop2"), 0.5, 0.01)
      .addNodeIndex("A", Seq("prop2", "prop3"), 1.0, 0.01)
      .build()

    val query =
      """
        |MATCH (a:A {prop2: "hello"})
        |WHERE a.prop4 = "disjunction" OR a.prop3 = "test"
        |WITH a
        |MATCH (b:B)
        |WHERE b.A_prop = a.prop1
        |RETURN *;
      """.stripMargin

    val plan = planner.plan(query).stripProduceResults
    // The planner will inject a partial predicate a.prop1 IS NOT NULL to make use of the index.
    // This predicate is anyway implied by b.A_prop = a.prop1.
    // When the union is planned, the planner doesn't realize that a partial predicate was added to the disjunction.
    // This results in a redundant filter being planned. This is probably cheaper than normalizing the disjunction and separating the partial predicates out.
    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin("cacheN[a.prop1] = b.A_prop")
      .|.nodeByLabelScan("b", "B", IndexOrderNone)
      .filter("cacheN[a.prop3]='test' OR cacheNFromStore[a.prop4] = 'disjunction'")
      .distinct("a AS a")
      .cacheProperties("cacheN[a.prop3]", "cacheN[a.prop4]")
      .union()
      .|.filter("cacheN[a.prop2] = 'hello'", "cacheNFromStore[a.prop4] = 'disjunction'")
      .|.nodeIndexOperator("a:A(prop1, prop2)", getValue = Map("prop1" -> GetValue, "prop2" -> GetValue))
      .nodeIndexOperator(
        "a:A(prop2 = 'hello', prop3 = 'test')",
        getValue = Map("prop2" -> GetValue, "prop3" -> GetValue),
        supportPartitionedScan = false
      )
      .build()
  }

  test("should not plan a relationship index scan for NOT IN unknown list") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1)
      .build()

    val q = "MATCH (a)-[r:REL]->(b) WHERE NOT r IN $param RETURN *"
    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("NOT r IN $param")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("should not plan a composite relationship index scan for NOT IN unknown list") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop", "otherProp"), 0.1, 0.1)
      .build()

    val q = "MATCH (a)-[r:REL]->(b) WHERE NOT r.prop IN $param AND r.otherProp IS NOT NULL RETURN *"
    val plan = planner.plan(q).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("NOT r.prop IN $param", "r.otherProp IS NOT NULL")
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("Should use an index scan independent of order of arguments in distance function") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Station", 50)
      .addNodeIndex("Station", Seq("name"), 0.5, 0.01, indexType = IndexType.RANGE)
      .addNodeIndex("Station", Seq("location"), 0.5, 0.01, indexType = IndexType.POINT)
      .build()

    for (argNodes <- List(Array("a", "b", "cacheNFromStore", "cacheN"), Array("b", "a", "cacheN", "cacheNFromStore")))
      yield {
        val plan = cfg.plan(
          s"""
             |MATCH (a:Station {name:'StationA'})
             |MATCH (b:Station) WHERE point.distance(${argNodes(0)}.location, ${argNodes(1)}.location) <= 5
             |RETURN b""".stripMargin
        ).stripProduceResults
        plan shouldEqual cfg.subPlanBuilder()
          .filter(
            s"point.distance(${argNodes(2)}[${argNodes(0)}.location], ${argNodes(3)}[${argNodes(1)}.location]) <= 5"
          )
          .apply()
          .|.cachedPropertyPointDistanceNodeIndexSeek(
            "b",
            "Station",
            "location",
            cachedNodePropFromStore("a", "location"),
            literalInt(5),
            inclusive = true,
            argumentIds = Set("a"),
            getValue = GetValue
          )
          .nodeIndexOperator("a:Station(name = 'StationA')")
          .build()
      }
  }

  test("Should use an index scan independent of order of property accesses in inequality") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Station", 50)
      .addNodeIndex("Station", Seq("id"), 0.5, 0.01, indexType = IndexType.RANGE)
      .addNodeIndex("Station", Seq("prop"), 0.5, 0.01, indexType = IndexType.RANGE)
      .build()

    for (pred <- List("a.prop >= b.prop", "b.prop <= a.prop")) yield {
      val plan = cfg.plan(
        s"""
           |MATCH (a:Station {id: 123})
           |MATCH (b:Station) WHERE $pred
           |RETURN b
           |""".stripMargin
      ).stripProduceResults

      plan shouldEqual cfg.subPlanBuilder()
        .apply()
        .|.nodeIndexOperator("b:Station(prop <= a.prop)", argumentIds = Set("a"), getValue = Map("prop" -> GetValue))
        .nodeIndexOperator("a:Station(id = 123)", paramExpr = Seq(parameter("id", CTAny)))
        .build()
    }
  }

  test(
    "Should use a range index scan on property in comparison predicate with distance function when there is no other index available"
  ) {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Station", 50)
      .addNodeIndex("Station", Seq("prop"), 0.5, 0.01, indexType = IndexType.RANGE)
      .build()

    val plan = cfg.plan(
      s"""
         |MATCH (a:Station) WHERE a.prop < point.distance(a.location, point({x: 10, y: 20}))
         |RETURN a
         |""".stripMargin
    ).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .filter("cacheN[a.prop] < point.distance(a.location, point({x: 10, y: 20}))")
      .nodeIndexOperator("a:Station(prop)", getValue = Map("prop" -> GetValue), indexType = IndexType.RANGE)
      .build()
  }

  test("Should use a point index scan on property in range function when there is no other index available") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Station", 50)
      .addNodeIndex("Station", Seq("location"), 0.5, 0.01, indexType = IndexType.POINT)
      .build()

    val plan = cfg.plan(
      s"""
         |MATCH (a:Station) WHERE a.prop < point.distance(a.location, point({x: 10, y: 20}))
         |RETURN a
         |""".stripMargin
    ).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .filter("a.prop < point.distance(cacheN[a.location], point({x: 10, y: 20}))")
      .nodeIndexOperator(
        "a:Station(location)",
        getValue = Map("location" -> GetValue),
        indexType = IndexType.POINT,
        supportPartitionedScan = false
      )
      .build()
  }

  test("Should use a point index seek on property in range function when the distance must be less than a value") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Station", 50)
      .addNodeIndex("Station", Seq("location"), 0.5, 0.01, indexType = IndexType.POINT)
      .build()

    val plan = cfg.plan(
      s"""
         |MATCH (a:Station) WHERE 5.0 > point.distance(a.location, point({x: 10, y: 20}))
         |RETURN a
         |""".stripMargin
    ).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .filter("5.0 > point.distance(cacheN[a.location], point({x: 10, y: 20}))")
      .pointDistanceNodeIndexSeek(
        "a",
        "Station",
        "location",
        "{x: 10, y: 20}",
        5.0,
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .build()
  }

  test("Should use POINT index scan when it is more selective than the RANGE index") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Station", 50)
      .addNodeIndex("Station", Seq("location"), 0.05, 0.001, indexType = IndexType.POINT)
      .addNodeIndex("Station", Seq("prop"), 0.5, 0.01, indexType = IndexType.RANGE)
      .build()

    val plan = cfg.plan(
      s"""
         |MATCH (a:Station) WHERE a.prop < point.distance(a.location, point({x: 10, y: 20}))
         |RETURN a
         |""".stripMargin
    ).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .filter("a.prop < point.distance(cacheN[a.location], point({x: 10, y: 20}))")
      .nodeIndexOperator(
        "a:Station(location)",
        getValue = Map("location" -> GetValue),
        indexType = IndexType.POINT,
        supportPartitionedScan = false
      )
      .build()
  }

  test("Should use RANGE index when it is more selective than the POINT index") {
    val cfg = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Station", 50)
      .addNodeIndex("Station", Seq("location"), 0.5, 0.01, indexType = IndexType.POINT)
      .addNodeIndex("Station", Seq("prop"), 0.05, 0.001, indexType = IndexType.RANGE)
      .build()

    val plan = cfg.plan(
      s"""
         |MATCH (a:Station) WHERE a.prop < point.distance(a.location, point({x: 10, y: 20}))
         |RETURN a
         |""".stripMargin
    ).stripProduceResults

    plan shouldEqual cfg.subPlanBuilder()
      .filter("cacheN[a.prop] < point.distance(a.location, point({x: 10, y: 20}))")
      .nodeIndexOperator("a:Station(prop)", getValue = Map("prop" -> GetValue), indexType = IndexType.RANGE)
      .build()
  }

  test("should plan a node index scan from an equality predicate spanning multiple components") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 800)
      .setLabelCardinality("B", 500)
      .addNodeIndex("A", Seq("prop"), 0.1, 0.1)
      .build()

    val query = "MATCH (a:A), (b:B) WHERE b.prop = a.prop RETURN *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin("cacheN[a.prop] = b.prop")
      .|.nodeByLabelScan("b", "B")
      .nodeIndexOperator("a:A(prop)", getValue = Map("prop" -> GetValue))
      .build()
  }

  test("should plan a relationship index scan from an equality predicate spanning multiple components") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setRelationshipCardinality("()-[:REL]->()", 500)
      .addRelationshipIndex("REL", Seq("prop"), 0.1, 0.1)
      .build()

    val query = "MATCH ()-[r:REL]->(), (b) WHERE b.prop = r.prop RETURN *"
    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .valueHashJoin("cacheR[r.prop] = b.prop")
      .|.allNodeScan("b")
      .relationshipIndexOperator("()-[r:REL(prop)]->()", getValue = Map("prop" -> GetValue))
      .build()
  }

  test("should plan index operator for both sides of equality within the same component, given two index hints") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 800)
      .setLabelCardinality("B", 500)
      .setRelationshipCardinality("()-[]->()", 400)
      .setRelationshipCardinality("(:A)-[]->()", 300)
      .setRelationshipCardinality("()-[]->(:B)", 350)
      .setRelationshipCardinality("(:A)-[]->(:B)", 200)
      .addNodeIndex("A", Seq("x"), 0.1, 0.1)
      .addNodeIndex("B", Seq("y"), 0.1, 0.1)
      .build()

    val query =
      """MATCH (a:A)-->(b:B)
        |USING INDEX a:A(x)
        |USING INDEX b:B(y)
        |WHERE a.x = b.y
        |RETURN a, b
        |""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .filter("cacheN[a.x] = cacheN[b.y]")
      .nodeHashJoin("b")
      .|.nodeIndexOperator("b:B(y)", getValue = Map("y" -> GetValue))
      .expandAll("(a)-[]->(b)")
      .nodeIndexOperator("a:A(x)", getValue = Map("x" -> GetValue))
      .build()
  }

  test("should plan a node index scan from a scannable non-equality predicate spanning multiple components") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 800)
      .setLabelCardinality("B", 500)
      .addNodeIndex("A", Seq("prop"), 0.1, 0.1)
      .build()

    for (op <- List("<", "<=", ">", ">=", "STARTS WITH", "CONTAINS", "ENDS WITH", "=~")) {
      val query = s"MATCH (a:A), (b:B) WHERE b.prop $op a.prop RETURN *"
      val plan = planner.plan(query).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"cacheN[b.prop] $op cacheN[a.prop]")
        .cartesianProduct()
        .|.cacheProperties("cacheNFromStore[b.prop]")
        .|.nodeByLabelScan("b", "B")
        .nodeIndexOperator("a:A(prop)", getValue = Map("prop" -> GetValue))
        .build()
    }
  }

  test("should plan a TEXT node index scan from a scannable non-equality predicate spanning multiple components") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("A", 800)
      .setLabelCardinality("B", 500)
      .addNodeIndex("A", Seq("prop"), 0.1, 0.1, indexType = IndexType.TEXT)
      .build()

    for (op <- List("STARTS WITH", "CONTAINS", "ENDS WITH", "=~")) {
      val query = s"MATCH (a:A), (b:B) WHERE b.prop $op a.prop RETURN *"
      val plan = planner.plan(query).stripProduceResults
      plan shouldEqual planner.subPlanBuilder()
        .filter(s"cacheN[b.prop] $op cacheN[a.prop]")
        .cartesianProduct()
        .|.cacheProperties("cacheNFromStore[b.prop]")
        .|.nodeByLabelScan("b", "B")
        .cacheProperties("cacheNFromStore[a.prop]")
        .nodeIndexOperator(
          "a:A(prop)",
          getValue = Map("prop" -> DoNotGetValue),
          indexType = IndexType.TEXT,
          supportPartitionedScan = false
        )
        .build()
    }
  }

}
