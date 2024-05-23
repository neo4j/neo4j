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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.compiler.planner.StubbedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.andsReorderable
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.column
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManyQueryExpression
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.RangeBetween
import org.neo4j.cypher.internal.logical.plans.RangeGreaterThan
import org.neo4j.cypher.internal.logical.plans.RangeLessThan
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SingleQueryExpression
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTStringNotNull
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.IndexHintException
import org.neo4j.graphdb.schema.IndexType

import java.lang.Boolean.TRUE

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp
    with LogicalPlanningIntegrationTestSupport {

  test("should plan index seek by prefix for simple prefix search based on STARTS WITH with prefix") {
    (new givenConfig {
      textIndexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' RETURN a")._1 should equal(
      nodeIndexSeek("a:Person(name STARTS WITH 'prefix')", indexType = IndexType.TEXT, supportPartitionedScan = false)
    )
  }

  test("should prefer cheaper optional expand over joins, even if not cheaper before rewriting") {
    (new givenConfig {
      cost = {
        // cost tweak for not choosing an all-relationships-scan
        case (s: Selection, _, _, _) if s.leftmostLeaf.isInstanceOf[DirectedAllRelationshipsScan] => Double.MaxValue

        case (_: Selection, _, _, _)          => 1.02731056E8
        case (_: RightOuterHashJoin, _, _, _) => 6.610321376825E9
        case (_: LeftOuterHashJoin, _, _, _)  => 8.1523761738E9
        case (_: Apply, _, _, _)              => 7.444573003149691E9
        case (_: OptionalExpand, _, _, _)     => 4.76310362E8
        case (_: Optional, _, _, _)           => 7.206417822149691E9

        case (_: Expand, _, _, _)           => 7.89155379E7
        case (_: AllNodesScan, _, _, _)     => 3.50735724E7
        case (_: Argument, _, _, _)         => 2.38155181E8
        case (_: ProjectEndpoints, _, _, _) => 11.0
      }
    } getLogicalPlanFor
      """UNWIND $createdRelationships as r
        |MATCH (source)-[r]->(target)
        |WITH source AS p
        |OPTIONAL MATCH (p)<-[follow]-() WHERE type(follow) STARTS WITH 'ProfileFavorites'
        |WITH p, count(follow) as fc
        |RETURN 1
      """.stripMargin)._1 should beLike {
      case Projection(Aggregation(_: OptionalExpand, _, _), _) => ()
    }
  }

  test("should plan index seek by prefix for simple prefix search based on CONTAINS substring") {
    (new givenConfig {
      textIndexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name CONTAINS 'substring' RETURN a")._1 should equal(
      nodeIndexSeek("a:Person(name CONTAINS 'substring')", indexType = IndexType.TEXT)
    )
  }

  test(
    "should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix"
  ) {
    (new givenConfig {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'short' AND a.lastname STARTS WITH 'longer' RETURN a")
      ._1 should equal(
      Selection(
        ands(startsWith(prop(v"a", "name"), literalString("short"))),
        nodeIndexSeek("a:Person(lastname STARTS WITH 'longer')", propIds = Some(Map("lastname" -> 1)))
      )
    )
  }

  test(
    "should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix even with predicates reversed"
  ) {
    (new givenConfig {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.lastname STARTS WITH 'longer' AND a.name STARTS WITH 'short' RETURN a")
      ._1 should equal(
      Selection(
        ands(startsWith(prop(v"a", "name"), literalString("short"))),
        nodeIndexSeek("a:Person(lastname STARTS WITH 'longer')", propIds = Some(Map("lastname" -> 1)))
      )
    )
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHs combined with AND NOT") {
    (new givenConfig {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'longer' AND NOT a.lastname STARTS WITH 'short' RETURN a")
      ._1 should equal(
      Selection(
        ands(not(startsWith(prop(v"a", "lastname"), literalString("short")))),
        nodeIndexSeek("a:Person(name STARTS WITH 'longer')")
      )
    )
  }

  test("should plan property equality index seek instead of index seek by prefix") {
    (new givenConfig {
      indexOn("Person", "name")
      cardinality = mapCardinality(promoteOnlyPlansSolving(
        Set("a"),
        Set(hasLabels("a", "Person"), in(prop("a", "name"), listOfString("prefix1")))
      ))

    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' AND a.name = 'prefix1' RETURN a")._1 should equal(
      Selection(
        ands(startsWith(cachedNodeProp("a", "name"), literalString("prefix"))),
        nodeIndexSeek("a:Person(name = 'prefix1')", _ => GetValue)
      )
    )
  }

  test("should plan property equality index seek using IN instead of index seek by prefix") {
    (new givenConfig {
      indexOn("Person", "name")
      cardinality = mapCardinality(promoteOnlyPlansSolving(
        Set("a"),
        Set(hasLabels(v"a", "Person"), in(prop(v"a", "name"), listOfString("prefix1", "prefix2")))
      ))

    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix%' AND a.name in ['prefix1', 'prefix2'] RETURN a")._1 should equal(
      Selection(
        ands(startsWith(cachedNodeProp("a", "name"), literalString("prefix%"))),
        NodeIndexSeek(
          v"a",
          LabelToken("Person", LabelId(0)),
          Seq(indexedProperty("name", 0, GetValue, NODE_TYPE)),
          ManyQueryExpression(listOfString("prefix1", "prefix2")),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = false
        )
      )
    )
  }

  test("should plan index seek by numeric range for numeric inequality predicate") {
    (new givenConfig {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.age < 12 RETURN a")._1 should equal(
      nodeIndexSeek("a:Person(age < 12)")
    )
  }

  test("should plan index seek by numeric range for numeric chained operator") {
    val than = RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(6))))
    val than1 = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(12))))
    (new givenConfig {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE 6 < a.age < 12 RETURN a")._1 should equal(
      NodeIndexSeek(
        v"a",
        LabelToken("Person", LabelId(0)),
        Seq(indexedProperty("age", 0, DoNotGetValue, NODE_TYPE)),
        RangeQueryExpression(
          InequalitySeekRangeWrapper(
            RangeBetween(
              than,
              than1
            )
          )(pos)
        ),
        Set.empty,
        IndexOrderNone,
        IndexType.RANGE,
        supportPartitionedScan = true
      )
    )
  }

  test(
    "should plan index seek for multiple inequality predicates and prefer the index seek with the lower cost per row"
  ) {
    (new givenConfig {
      indexOn("Person", "name")
      indexOn("Person", "age")
      cost = {
        case (_: AllNodesScan, _, _, _)    => 1000.0
        case (_: NodeByLabelScan, _, _, _) => 50.0
        case (_: NodeIndexScan, _, _, _)   => 10.0
        case (plan: NodeIndexSeek, _, _, _)
          if plan.properties.headOption.map(_.propertyKeyToken.name).contains("name") => 1.0
        case (plan: NodeIndexSeek, _, _, _)
          if plan.properties.headOption.map(_.propertyKeyToken.name).contains("age") => 5.0
        case (Selection(_, source), x, y, z) => cost((source, x, y, z)) + 30.0
        case _                               => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.age > 40 AND a.name >= 'Cinderella' RETURN a")._1 should equal(
      Selection(
        Seq(greaterThan(prop(v"a", "age"), literalInt(40))),
        nodeIndexSeek("a:Person(name >= 'Cinderella')")
      )
    )
  }

  test("should plan index seek by string range for textual inequality predicate") {
    (new givenConfig {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name >= 'Frodo' RETURN a")._1 should equal(
      nodeIndexSeek("a:Person(name >= 'Frodo')")
    )
  }

  test("should plan all nodes scans") {
    (new givenConfig {} getLogicalPlanFor "MATCH (n) RETURN n")._1 should equal(
      AllNodesScan(v"n", Set.empty)
    )
  }

  test("should plan label scans even without having a compile-time label id") {
    (new givenConfig {
      cost = {
        case (_: AllNodesScan, _, _, _)    => 1000.0
        case (_: NodeByIdSeek, _, _, _)    => 2.0
        case (_: NodeByLabelScan, _, _, _) => 1.0
        case _                             => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n")._1 should equal(
      NodeByLabelScan(v"n", labelName("Awesome"), Set.empty, IndexOrderNone)
    )
  }

  test("should plan label scans when having a compile-time label id") {
    val plan =
      new givenConfig {
        cost = {
          case (_: AllNodesScan, _, _, _)    => 1000.0
          case (_: NodeByIdSeek, _, _, _)    => 2.0
          case (_: NodeByLabelScan, _, _, _) => 1.0
          case _                             => Double.MaxValue
        }
        knownLabels = Set("Awesome")
      } getLogicalPlanFor "MATCH (n:Awesome) RETURN n"

    plan._1 should equal(
      NodeByLabelScan(v"n", labelName("Awesome"), Set.empty, IndexOrderNone)
    )
  }

  private val nodeIndexScanCost
    : PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities, ProvidedOrders), Cost] = {
    case (_: AllNodesScan, _, _, _)          => 1000.0
    case (_: NodeByLabelScan, _, _, _)       => 50.0
    case (_: NodeIndexScan, _, _, _)         => 10.0
    case (_: NodeIndexContainsScan, _, _, _) => 10.0
    case (nodeIndexSeek: NodeIndexSeek, _, cardinalities, providedOrders) =>
      val planCardinality = cardinalities.get(nodeIndexSeek.id).amount
      val rowCost = 1.0
      val allNodesCardinality = 1000.0
      rowCost * planCardinality / allNodesCardinality
    case (Selection(_, plan), input, c, p) => nodeIndexScanCost((plan, input, c, p)) + 1.0
    case _                                 => Double.MaxValue
  }

  private val nodeIndexSeekCost
    : PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities, ProvidedOrders), Cost] = {
    case (_: AllNodesScan, _, _, _)                    => 1000000000.0
    case (_: NodeIndexSeek, _, _, _)                   => 0.1
    case (Expand(plan, _, _, _, _, _, _), input, c, p) => nodeIndexSeekCost((plan, input, c, p))
    case (Selection(_, plan), input, c, p)             => nodeIndexSeekCost((plan, input, c, p))
    case _                                             => 1000.0
  }

  private def promoteOnlyPlansSolving(
    patternNodes: Set[String],
    expressions: Set[Expression]
  ): PartialFunction[PlannerQuery, Double] = {
    case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == patternNodes.map(varFor) =>
      queryGraph.selections.predicates.map(_.expr) match {
        case es if es == expressions => 10.0
        case _                       => Double.MaxValue
      }

    case _ => Double.MaxValue
  }

  test("should plan index scan for n.prop IS NOT NULL") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
        cost = nodeIndexScanCost
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN n"

    plan._1 should equal(nodeIndexSeek("n:Awesome(prop)"))
  }

  test("should plan unique index scan for n.prop IS NOT NULL") {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop")
        cost = nodeIndexScanCost
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN n"

    plan._1 should equal(nodeIndexSeek("n:Awesome(prop)"))
  }

  test("should plan index seek instead of index scan when there are predicates for both") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
        cost = nodeIndexScanCost
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL AND n.prop = 42 RETURN n"

    plan._1 should equal(
      Selection(ands(isNotNull(cachedNodeProp("n", "prop"))), nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue))
    )
  }

  test("should plan index seek when there is an index on the property") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan._1 should equal(
      nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue)
    )
  }

  test("should plan unique index seek when there is an unique index on the property") {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan._1 should equal(
      NodeUniqueIndexSeek(
        v"n",
        LabelToken("Awesome", LabelId(0)),
        Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
        SingleQueryExpression(literalInt(42)),
        Set.empty,
        IndexOrderNone,
        IndexType.RANGE,
        supportPartitionedScan = true
      )
    )
  }

  test("should plan node by ID lookup instead of label scan when the node by ID lookup is cheaper") {
    (new givenConfig {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) = 42 RETURN n")._1 should equal(
      Selection(
        ands(hasLabels(v"n", "Awesome")),
        NodeByIdSeek(v"n", ManySeekableArgs(listOfInt(42)), Set.empty)
      )
    )
  }

  test("should plan node by ID lookup based on an IN predicate with a param as the rhs") {
    (new givenConfig {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) IN $param RETURN n")._1 should equal(
      Selection(
        ands(hasLabels(v"n", "Awesome")),
        NodeByIdSeek(v"n", ManySeekableArgs(parameter("param", CTAny)), Set.empty)
      )
    )
  }

  test("should plan NodeByIdSeek and Argument instead of scans") {
    val config =
      plannerBuilder()
        .setAllNodesCardinality(100)
        .setRelationshipCardinality("()-[]->()", 10)
        .setRelationshipCardinality("()-[:REL]->()", 10)
        .setRelationshipCardinality("()-[]->(:Role)", 1)
        .setLabelCardinality("Role", 10)
        .build()

    val query =
      """
        |MATCH (n)-[:REL]->(m)
        |WHERE id(m) = 1
        |OPTIONAL MATCH (n)-->(middle)-->(role:Role)
        |RETURN n
        |""".stripMargin

    val plan = config.plan(query)

    val expected =
      config
        .planBuilder()
        .produceResults("n")
        .apply()
        .|.optional("m", "anon_0", "n")
        .|.filter("role:Role", "not anon_2 = anon_1")
        .|.expandAll("(middle)-[anon_2]->(role)")
        .|.expandAll("(n)-[anon_1]->(middle)")
        .|.argument("n", "m", "anon_0")
        .expandAll("(m)<-[anon_0:REL]-(n)")
        .nodeByIdSeek("m", Set(), 1)
        .build()

    plan shouldEqual expected
  }

  test("should plan directed rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new givenConfig {} getLogicalPlanFor "MATCH (a)-[r]->(b) WHERE id(r) IN $param RETURN a, r, b")._1 should equal(
      DirectedRelationshipByIdSeek(
        v"r",
        ManySeekableArgs(parameter("param", CTAny)),
        v"a",
        v"b",
        Set.empty
      )
    )
  }

  test("should plan undirected rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new givenConfig {} getLogicalPlanFor "MATCH (a)-[r]-(b) WHERE id(r) IN $param RETURN a, r, b")._1 should equal(
      UndirectedRelationshipByIdSeek(
        v"r",
        ManySeekableArgs(parameter("param", CTAny)),
        v"a",
        v"b",
        Set.empty
      )
    )
  }

  test("should plan node by ID lookup based on an IN predicate") {
    (new givenConfig {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) IN [42, 64] RETURN n")._1 should equal(
      Selection(
        ands(hasLabels(v"n", "Awesome")),
        NodeByIdSeek(v"n", ManySeekableArgs(listOfInt(42, 64)), Set.empty)
      )
    )
  }

  test("should plan index seek when there is an index on the property and an IN predicate") {
    (new givenConfig {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IN [42] RETURN n")._1 should beLike {
      case NodeIndexSeek(
          LogicalVariable("n"),
          LabelToken("Awesome", _),
          Seq(IndexedProperty(PropertyKeyToken("prop", _), GetValue, NODE_TYPE)),
          SingleQueryExpression(SignedDecimalIntegerLiteral("42")),
          _,
          _,
          _,
          _
        ) => ()
    }
  }

  test("should use indexes for large collections if it is a unique index") {
    val result =
      new givenConfig {
        cost = {
          case (_: AllNodesScan, _, _, _)    => 10000.0
          case (_: NodeByLabelScan, _, _, _) => 1000.0
          case (_: NodeByIdSeek, _, _, _)    => 2.0
          case _                             => Double.MaxValue
        }
        uniqueIndexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5] RETURN n"

    result._1 should beLike {
      case _: NodeUniqueIndexSeek => ()
    }
  }

  //
  // Composite indexes
  // WHERE n:Label AND (n.prop = $val1 OR (n.prop = $val2 AND n.bar = $val3))

  test(
    "should plan composite index seek when there is an index on two properties and both are in equality predicates"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop", "prop2")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.prop2 = 'foo' RETURN n"

    plan._1 should equal(
      nodeIndexSeek("n:Awesome(prop = 42, prop2 = 'foo')", _ => GetValue, supportPartitionedScan = false)
    )
  }

  test(
    "should plan composite index seek when there is an index on two properties and both are in equality predicates regardless of predicate order"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop", "prop2")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop2 = 'foo' AND n.prop = 42 RETURN n"

    plan._1 should equal(
      nodeIndexSeek("n:Awesome(prop = 42, prop2 = 'foo')", _ => GetValue, supportPartitionedScan = false)
    )
  }

  test(
    "should plan composite index seek and filter when there is an index on two properties and both are in equality predicates together with other predicates"
  ) {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop", "prop2")
      } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop2 = 'foo' AND n.name IS NOT NULL AND n.prop = 42 RETURN n"

    plan._1 should equal(
      Selection(
        ands(isNotNull(prop(v"n", "name"))),
        nodeIndexSeek("n:Awesome(prop = 42, prop2 = 'foo')", _ => GetValue, supportPartitionedScan = false)
      )
    )
  }

  //
  // index hints
  //

  test("should plan hinted label scans") {

    val plan =
      new givenConfig {
        cost = {
          case (_: Selection, _, _, _)       => 20.0
          case (_: NodeHashJoin, _, _, _)    => 1000.0
          case (_: NodeByLabelScan, _, _, _) => 20.0
        }
      } getLogicalPlanFor "MATCH (n:Foo:Bar:Baz) USING SCAN n:Bar RETURN n"

    plan._1 should equal(
      IntersectionNodeByLabelsScan(
        v"n",
        Seq(labelName("Foo"), labelName("Bar"), labelName("Baz")),
        Set.empty,
        IndexOrderNone
      )
    )
  }

  test("should plan hinted index seek") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100)
      .addNodeIndex("Awesome", Seq("prop"), 1.0, 0.01)
      .setLabelCardinality("Awesome", 10)
      .build()

    val plan = planner.plan("MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n")

    plan shouldBe planner.planBuilder()
      .produceResults(column("n", "cacheN[n.prop]"))
      .nodeIndexOperator("n:Awesome(prop = 42)", _ => GetValue)
      .build()
  }

  test("should plan hinted index seek when returning *") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN *"

    plan._1 should equal(
      nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue)
    )
  }

  test("should plan hinted index seek with or") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n:Awesome) USING INDEX n:Awesome(prop) WHERE n.prop = 42 OR n.prop = 1337 RETURN n"

    plan._1 should equal(
      NodeIndexSeek(
        v"n",
        LabelToken("Awesome", LabelId(0)),
        Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
        ManyQueryExpression(listOfInt(42, 1337)),
        Set.empty,
        IndexOrderNone,
        IndexType.RANGE,
        supportPartitionedScan = false
      )
    )
  }

  test("should plan hinted index seek when there are multiple indices") {
    val plan =
      new givenConfig {
        indexOn("Awesome", "prop1")
        indexOn("Awesome", "prop2")
      } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n "

    plan._1 should equal(
      Selection(
        ands(equals(prop(v"n", "prop1"), literalInt(42))),
        nodeIndexSeek("n:Awesome(prop2 = 3)", _ => GetValue, propIds = Some(Map("prop2" -> 1)))
      )
    )
  }

  val awesomePlanner = plannerBuilder()
    .setAllNodesCardinality(1000)
    .setLabelCardinality("Awesome", 100)
    .addNodeIndex("Awesome", Seq("prop1"), 0.1, 0.01)
    .addNodeIndex("Awesome", Seq("prop2"), 0.1, 0.01)
    .build()

  test("should plan hinted index seek when there are multiple or indices") {
    val query = "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND (n.prop1 = 42 OR n.prop2 = 3) RETURN n "
    awesomePlanner.plan(query) should equal(
      awesomePlanner.planBuilder()
        .produceResults(column("n", "cacheN[n.prop2]", "cacheN[n.prop1]"))
        .distinct("n AS n")
        .union()
        .|.nodeIndexOperator("n:Awesome(prop2 = 3)", _ => GetValue)
        .nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should plan hinted unique index seek") {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop")
      } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan._1 should equal(
      NodeUniqueIndexSeek(
        v"n",
        LabelToken("Awesome", LabelId(0)),
        Seq(indexedProperty("prop", 0, GetValue, NODE_TYPE)),
        SingleQueryExpression(literalInt(42)),
        Set.empty,
        IndexOrderNone,
        IndexType.RANGE,
        supportPartitionedScan = true
      )
    )
  }

  test("should plan hinted unique index seek when there are multiple unique indices") {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop1")
        uniqueIndexOn("Awesome", "prop2")
      } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n"

    plan._1 should equal(
      Selection(
        ands(equals(prop(v"n", "prop1"), literalInt(42))),
        NodeUniqueIndexSeek(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 1, GetValue, NODE_TYPE)),
          SingleQueryExpression(literalInt(3)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        )
      )
    )
  }

  test("should plan hinted unique index seek based on an IN predicate  when there are multiple unique indices") {
    val plan =
      new givenConfig {
        uniqueIndexOn("Awesome", "prop1")
        uniqueIndexOn("Awesome", "prop2")
      } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 IN [3] RETURN n"

    plan._1 should equal(
      Selection(
        ands(equals(prop(v"n", "prop1"), literalInt(42))),
        NodeUniqueIndexSeek(
          v"n",
          LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 1, GetValue, NODE_TYPE)),
          SingleQueryExpression(literalInt(3)),
          Set.empty,
          IndexOrderNone,
          IndexType.RANGE,
          supportPartitionedScan = true
        )
      )
    )
  }

  private object nodeIndexHints {

    val config: StatisticsBackedLogicalPlanningConfigurationBuilder =
      plannerBuilder()
        .setAllNodesCardinality(1000)
        .setLabelCardinality("A", 1)
        .setLabelCardinality("B", 1000)
        .setAllRelationshipsCardinality(10)
        .setRelationshipCardinality("()-[:R]->()", 10)
        .setRelationshipCardinality("(:A)-[:R]->()", 10)
        .setRelationshipCardinality("(:A)<-[:R]-()", 0)
        .setRelationshipCardinality("()-[:R]->(:B)", 10)
        .setRelationshipCardinality("()<-[:R]-(:B)", 0)
        .setRelationshipCardinality("(:A)-[:R]->(:B)", 10)
        .setRelationshipCardinality("(:A)<-[:R]-(:B)", 0)

    def query(hint: String): String =
      s"""MATCH (a:A)-[r:R]->(b:B) $hint
         |WHERE b.prop STARTS WITH 'x'
         |RETURN a""".stripMargin
  }

  private object nodePointIndexHints {

    val pointQueryExpression = Some(SingleQueryExpression(
      FunctionInvocation(
        FunctionName("point")(pos),
        MapExpression(Seq(
          PropertyKeyName("x")(pos) -> DecimalDoubleLiteral("22.0")(pos),
          PropertyKeyName("y")(pos) -> DecimalDoubleLiteral("44.0")(pos)
        ))(pos)
      )(pos)
    ))

    def nodeQuery(hint: String): String =
      s"""MATCH (a:A)-[r:R]->(b:B) $hint
         |WHERE b.prop = point({x:22.0, y:44.0})
         |RETURN a""".stripMargin

    def relQuery(hint: String): String =
      s"""MATCH (a)-[r:R]->(b) $hint
         |WHERE r.prop = point({x:22.0, y:44.0})
         |RETURN a""".stripMargin
  }

  test("should not plan node index for this query without hints") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.RANGE)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    planner.plan(nodeIndexHints.query(""))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("b.prop STARTS WITH 'x'", "b:B")
          .expandAll("(a)-[r:R]->(b)")
          .nodeByLabelScan("a", "A", IndexOrderNone)
          .build()
      )
  }

  test("should plan text node index when index hint has unspecified type") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    planner.plan(nodeIndexHints.query("USING INDEX b:B(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("a:A")
          .expandAll("(b)<-[r:R]-(a)")
          .nodeIndexOperator("b:B(prop STARTS WITH 'x')", indexType = IndexType.TEXT, supportPartitionedScan = false)
          .build()
      )
  }

  test("should plan range node index when index hint has range type") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.RANGE)
      .build()

    planner.plan(nodeIndexHints.query("USING RANGE INDEX b:B(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("a:A")
          .expandAll("(b)<-[r:R]-(a)")
          .nodeIndexOperator("b:B(prop STARTS WITH 'x')", indexType = IndexType.RANGE)
          .build()
      )
  }

  test("should plan point node index when index hint has point type") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.RANGE)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.POINT)
      .build()

    planner.plan(nodePointIndexHints.nodeQuery("USING POINT INDEX b:B(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("a:A")
          .expandAll("(b)<-[r:R]-(a)")
          .nodeIndexOperator(
            "b:B(prop)",
            indexType = IndexType.POINT,
            customQueryExpression = nodePointIndexHints.pointQueryExpression,
            supportPartitionedScan = false
          )
          .build()
      )
  }

  test("should plan text node index when index hint has text type") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.RANGE)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    planner.plan(nodeIndexHints.query("USING TEXT INDEX b:B(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("a:A")
          .expandAll("(b)<-[r:R]-(a)")
          .nodeIndexOperator("b:B(prop STARTS WITH 'x')", indexType = IndexType.TEXT, supportPartitionedScan = false)
          .build()
      )
  }

  test("should warn when node index hint specifies an index type that does not exist") {

    val baseCfg = nodeIndexHints.config
      .withSetting(GraphDatabaseSettings.cypher_hints_error, TRUE)

    val basePlanner = baseCfg.build()

    val allTypes = Seq(IndexType.TEXT, IndexType.RANGE, IndexType.POINT)

    allTypes.foreach { hintType =>
      val otherTypes = allTypes.filterNot(_ == hintType)
      val hasOtherIndexes = otherTypes.foldLeft(baseCfg) { case (cfg, indexType) =>
        cfg.addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = indexType)
      }
      val planner = hasOtherIndexes.build()

      val hint = s"USING $hintType INDEX b:B(prop)"
      val expectedMessage = s"No such index: $hintType INDEX FOR (`b`:`B`) ON (`b`.`prop`)"

      withClue(s"Hinting for $hintType when existing types are $otherTypes") {
        the[IndexHintException]
          .thrownBy(planner.plan(nodeIndexHints.query(hint)))
          .getMessage.should(include(expectedMessage))
      }

      withClue(s"Hinting for $hintType when no indexes exist") {
        the[IndexHintException]
          .thrownBy(basePlanner.plan(nodeIndexHints.query(hint)))
          .getMessage.should(include(expectedMessage))
      }
    }
  }

  private object relIndexHints {

    val config: StatisticsBackedLogicalPlanningConfigurationBuilder =
      plannerBuilder()
        .setAllNodesCardinality(10)
        .setAllRelationshipsCardinality(100000)
        .setRelationshipCardinality("()-[:R]->()", 100000)
        .setRelationshipCardinality("()-[:R]->(:B)", 100000)
        .setLabelCardinality("B", 9)

    def query(hint: String): String =
      s"""MATCH (a)-[r:R]->(b:B) $hint
         |WHERE r.prop IS :: STRING NOT NULL
         |RETURN a""".stripMargin
  }

  test("should not plan relationship index for this query without hints") {

    val planner = relIndexHints.config
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.RANGE
      )
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.TEXT
      )
      .build()

    planner.plan(relIndexHints.query(""))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("r.prop IS :: STRING NOT NULL")
          .expandAll("(b)<-[r:R]-(a)")
          .nodeByLabelScan("b", "B")
          .build()
      )
  }

  test("should warn when relationship index hint specifies an index type that does not exist") {

    val baseCfg = relIndexHints.config
      .withSetting(GraphDatabaseSettings.cypher_hints_error, TRUE)

    val basePlanner = baseCfg.build()

    val allTypes = Seq(IndexType.TEXT, IndexType.RANGE, IndexType.POINT)

    allTypes.foreach { hintType =>
      val otherTypes = allTypes.filterNot(_ == hintType)
      val hasOtherIndexes = otherTypes.foldLeft(baseCfg) { case (cfg, indexType) =>
        cfg.addRelationshipIndex(
          "R",
          Seq("prop"),
          existsSelectivity = 1.0,
          uniqueSelectivity = 1.0,
          indexType = indexType
        )
      }
      val planner = hasOtherIndexes.build()

      val hint = s"USING $hintType INDEX r:R(prop)"
      val expectedMessage = s"No such index: $hintType INDEX FOR ()-[`r`:`R`]-() ON (`r`.`prop`)"

      withClue(s"Hinting for $hintType when existing types are $otherTypes") {
        the[IndexHintException]
          .thrownBy(planner.plan(relIndexHints.query(hint)))
          .getMessage.should(include(expectedMessage))
      }

      withClue(s"Hinting for $hintType when no indexes exist") {
        the[IndexHintException]
          .thrownBy(basePlanner.plan(relIndexHints.query(hint)))
          .getMessage.should(include(expectedMessage))
      }
    }
  }

  test("should plan text relationship index when index hint has text type") {

    val planner = relIndexHints.config
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.RANGE
      )
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.TEXT
      )
      .build()

    planner.plan(relIndexHints.query("USING TEXT INDEX r:R(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("b:B")
          .relationshipIndexOperator("(a)-[r:R(prop)]->(b)", indexType = IndexType.TEXT, supportPartitionedScan = false)
          .build()
      )
  }

  test("should plan range relationship index when index hint has range type") {

    val planner = relIndexHints.config
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.RANGE
      )
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.TEXT
      )
      .build()

    planner.plan(relIndexHints.query("USING RANGE INDEX r:R(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filterExpression(hasLabels("b", "B"), isTyped(cachedRelProp("r", "prop"), CTStringNotNull))
          .relationshipIndexOperator("(a)-[r:R(prop)]->(b)", _ => GetValue, indexType = IndexType.RANGE)
          .build()
      )
  }

  test("should plan point relationship index when index hint has point type") {

    val planner = relIndexHints.config
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.RANGE
      )
      .addRelationshipIndex(
        "R",
        Seq("prop"),
        existsSelectivity = 1.0,
        uniqueSelectivity = 1.0,
        indexType = IndexType.POINT
      )
      .build()

    planner.plan(nodePointIndexHints.relQuery("USING POINT INDEX r:R(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .relationshipIndexOperator(
            "(a)-[r:R(prop)]->(b)",
            indexType = IndexType.POINT,
            customQueryExpression = nodePointIndexHints.pointQueryExpression,
            supportPartitionedScan = false
          )
          .build()
      )
  }

  test("should plan node by ID seek based on a predicate with an id collection variable as the rhs") {
    val plan =
      new givenConfig {
        cost = {
          case (_: AllNodesScan, _, _, _)    => 1000.0
          case (_: NodeByIdSeek, _, _, _)    => 2.0
          case (_: NodeByLabelScan, _, _, _) => 1.0
          case _                             => Double.MaxValue
        }
      } getLogicalPlanFor "WITH [0,1,3] AS arr MATCH (n) WHERE id(n) IN arr return count(*)"

    plan._1 should equal(
      Aggregation(
        Apply(
          Projection(Argument(), Map(v"arr" -> listOfInt(0, 1, 3))),
          NodeByIdSeek(v"n", ManySeekableArgs(v"arr"), Set(v"arr"))
        ),
        Map(),
        Map(v"count(*)" -> CountStar() _)
      )
    )
  }

  test("should use index on label and property") {
    val plan = (new givenConfig {
      indexOn("Crew", "name")
    } getLogicalPlanFor "MATCH (n:Crew) WHERE n.name = 'Neo' RETURN n")._1

    plan shouldBe using[NodeIndexSeek]
  }

  test("should use index when there are multiple labels on the node") {
    val plan = (new givenConfig {
      indexOn("Crew", "name")
      indexOn("Matrix", "someOther")
    } getLogicalPlanFor "MATCH (n:Matrix:Crew) WHERE n.name = 'Neo' RETURN n")._1

    plan shouldBe using[NodeIndexSeek]
  }

  test("should be able to OR together two index seeks") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 = 42 OR n.prop2 = 'apa' RETURN n"
    awesomePlanner.plan(query) should equal(
      awesomePlanner.planBuilder()
        .produceResults(column("n", "cacheN[n.prop2]", "cacheN[n.prop1]"))
        .distinct("n as n")
        .union()
        .|.nodeIndexOperator("n:Awesome(prop1 = 42)", _ => GetValue)
        .nodeIndexOperator("n:Awesome(prop2 = 'apa')", _ => GetValue)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should be able to OR together two index range seeks") {
    val query = "MATCH (n:Awesome) WHERE n.prop1 >= 42 OR n.prop2 STARTS WITH 'apa' RETURN n"
    awesomePlanner.plan(query) should equal(
      awesomePlanner.planBuilder()
        .produceResults(column("n", "cacheN[n.prop1]", "cacheN[n.prop2]"))
        .distinct("n as n")
        .union()
        .|.nodeIndexOperator("n:Awesome(prop1 >= 42)", _ => GetValue)
        .nodeIndexOperator("n:Awesome(prop2 STARTS WITH 'apa')", _ => GetValue)
        .build()
    )(SymmetricalLogicalPlanEquality)
  }

  test("should use transitive closure to figure out we can use index") {
    (new givenConfig {
      indexOn("Person", "name")
      cost = nodeIndexSeekCost
    } getLogicalPlanFor "MATCH (a:Person)-->(b) WHERE a.name = b.prop AND b.prop = 42 RETURN b")._1 should beLike {
      case Selection(_, Expand(NodeIndexSeek(LogicalVariable("a"), _, _, _, _, _, _, _), _, _, _, _, _, _)) => ()
    }
  }

  test("should use transitive closure to figure out emergent equalities") {
    (new givenConfig {
      indexOn("Person", "name")
      cost = nodeIndexSeekCost
    } getLogicalPlanFor "MATCH (a:Person)-->(b) WHERE b.prop = a.name AND b.prop = 42 RETURN b")._1 should beLike {
      case Selection(_, Expand(NodeIndexSeek(LogicalVariable("a"), _, _, _, _, _, _, _), _, _, _, _, _, _)) => ()
    }
  }

  // ---------------------------------------------------------------------------
  // Test expand order with multiple configurations and
  // internal.cypher.plan_with_minimum_cardinality_estimates setting
  //
  // To succeed this test assumes:
  // *  (:A) should have lower cardinality than (:B) and (:C) so it is selected as starting point
  // * (a)--(b) should have lower cardinality than (a)--(c) so that it is expanded first
  //
  // Ideally (and at the time of writing) the intrinsic order when the cardinalities are equal
  // is different from the assertion and would cause failure
  private def testAndAssertExpandOrder(config: StubbedLogicalPlanningConfiguration): Unit = {
    val query = "MATCH (b:B)-[rB]->(a:A)<-[rC]-(c:C) RETURN a, b, c"

    val plan = (config getLogicalPlanFor query)._1

    // Expected plan
    // Since (a)--(b) has a lower cardinality estimate than (a)--(c) it should be selected first
    val scanA = NodeByLabelScan(v"a", labelName("A"), Set.empty, IndexOrderNone)
    val expandB = Expand(scanA, v"a", INCOMING, Seq.empty, v"b", v"rB", ExpandAll)
    val selectionB = Selection(Seq(hasLabels(v"b", "B")), expandB)
    val expandC = Expand(selectionB, v"a", INCOMING, Seq.empty, v"c", v"rC", ExpandAll)
    val selectionC = Selection(Seq(hasLabels("c", "C"), not(equals(v"rC", v"rB"))), expandC)
    val expected = selectionC

    plan should equal(expected)
  }

  test("should pick expands in an order that minimizes early cardinality increase") {
    val config = new givenConfig {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a")       => 100.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b")       => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"c")       => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b") => 200.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"c") => 300.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b", v"c") =>
          100.0
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test(
    "should pick expands in an order that minimizes early cardinality increase (plan_with_minimum_cardinality_estimates enabled)"
  ) {
    val config = new givenPlanWithMinimumCardinalityEnabled {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a")       => 100.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b")       => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"c")       => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b") => 200.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"c") => 300.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b", v"c") =>
          100.0
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test("should pick expands in an order that minimizes early cardinality increase with estimates < 1.0") {
    val config = new givenConfig {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a")       => 0.1
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b")       => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"c")       => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b") => 0.4
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"c") => 0.5
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b", v"c") =>
          0.1
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test(
    "should pick expands in an order that minimizes early cardinality increase with estimates < 1.0 (plan_with_minimum_cardinality_estimates enabled)"
  ) {
    val config = new givenPlanWithMinimumCardinalityEnabled {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a")       => 0.1
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"b")       => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"c")       => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b") => 0.4
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"c") => 0.5
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set(v"a", v"b", v"c") =>
          0.1
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  private val relationshipTypeScanConfig: StatisticsBackedLogicalPlanningConfiguration = plannerBuilder()
    .setAllNodesCardinality(100)
    .setRelationshipCardinality("()-[:REL]->()", 10000)
    .build()

  private val expandConfig: StatisticsBackedLogicalPlanningConfiguration = plannerBuilder()
    .setAllNodesCardinality(100)
    .setRelationshipCardinality("()-[:REL]->()", 1000)
    .build()

  test("should plan relationship type scan with inlined type predicate") {
    val query =
      """MATCH (a)-[r:REL]->(b)
        |RETURN a, b, r""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()

    plan shouldEqual expectedPlan
  }

  test("should plan relationship type scan with not-inlined type predicate") {
    val query =
      """MATCH (a)-[r]->(b)
        |WHERE r:REL
        |RETURN a, b, r""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()

    plan shouldEqual expectedPlan
  }

  test("should provide ascending order from directed relationship type scan") {
    val query =
      """MATCH (a)-[r:REL]->(b)
        |RETURN a, b, r ORDER BY r ASC""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .relationshipTypeScan("(a)-[r:REL]->(b)", IndexOrderAscending)
      .build()

    plan shouldEqual expectedPlan
  }

  test("should provide descending order from directed relationship type scan") {
    val query =
      """MATCH (a)-[r]->(b)
        |WHERE r:REL
        |RETURN a, b, r ORDER BY r DESC""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .relationshipTypeScan("(a)-[r:REL]->(b)", IndexOrderDescending)
      .build()

    plan shouldEqual expectedPlan
  }

  test("should provide ascending order from undirected relationship type scan") {
    val query =
      """MATCH (a)-[r:REL]-(b)
        |RETURN a, b, r ORDER BY r ASC""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .relationshipTypeScan("(a)-[r:REL]-(b)", IndexOrderAscending)
      .build()

    plan shouldEqual expectedPlan
  }

  test("should provide descending order from undirected relationship type scan") {
    val query =
      """MATCH (a)-[r]-(b)
        |WHERE r:REL
        |RETURN a, b, r ORDER BY r DESC""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .relationshipTypeScan("(a)-[r:REL]-(b)", IndexOrderDescending)
      .build()

    plan shouldEqual expectedPlan
  }

  test("should plan full sort if first sorting on unlabeled node and then labeled relationship") {
    val query =
      """MATCH (a)-[r]->(b)
        |WHERE r:REL
        |RETURN a, b, r ORDER BY a ASC, r ASC""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .sort("a ASC", "r ASC")
      .relationshipTypeScan("(a)-[r:REL]->(b)", IndexOrderNone)
      .build()

    plan shouldEqual expectedPlan
  }

  test("should plan only partial sort when first sorting on labeled relationship and then unlabeled node") {
    val query =
      """MATCH (a)-[r]->(b)
        |WHERE r:REL
        |RETURN a, b, r ORDER BY r DESC, a DESC""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    val expectedPlan = relationshipTypeScanConfig.subPlanBuilder()
      .partialSortColumns(Seq(Descending(v"r")), Seq(Descending(v"a")))
      .relationshipTypeScan("(a)-[r:REL]->(b)", IndexOrderDescending)
      .build()

    plan shouldEqual expectedPlan
  }

  test("should plan relationship type scan with filter for already bound start node with hint") {
    val query =
      """
        |MATCH (a)
        |WITH a SKIP 0
        |MATCH (a)-[r:REL]-(b)
        |USING SCAN r:REL
        |RETURN r""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    plan should equal(
      relationshipTypeScanConfig.subPlanBuilder()
        .filter("a = anon_0")
        .apply()
        .|.relationshipTypeScan("(anon_0)-[r:REL]-(b)", "a")
        .skip(0)
        .allNodeScan("a")
        .build()
    )
  }

  test("should plan relationship type scan with filter for already bound end node with hint") {
    val query =
      """
        |MATCH (b)
        |WITH b SKIP 0
        |MATCH (a)-[r:REL]-(b)
        |USING SCAN r:REL
        |RETURN r""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    plan should equal(
      relationshipTypeScanConfig.subPlanBuilder()
        .filter("b = anon_0")
        .apply()
        .|.relationshipTypeScan("(a)-[r:REL]-(anon_0)", "b")
        .skip(0)
        .allNodeScan("b")
        .build()
    )
  }

  test("should plan relationship type scan with filter for already bound start and end node with hint") {
    val query =
      """
        |MATCH (a), (b)
        |WITH a, b SKIP 0
        |MATCH (a)-[r:REL]-(b)
        |USING SCAN r:REL
        |RETURN r""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    plan should equal(
      relationshipTypeScanConfig.subPlanBuilder()
        .filter("a = anon_0", "b = anon_1")
        .apply()
        .|.relationshipTypeScan("(anon_0)-[r:REL]-(anon_1)", "a", "b")
        .skip(0)
        .cartesianProduct()
        .|.allNodeScan("b")
        .allNodeScan("a")
        .build()
    )
  }

  test("should not plan relationship type scan with filter for already bound start node with many relationships") {
    val query =
      """
        |MATCH (a)
        |WITH a SKIP 0
        |MATCH (a)-[r:REL]-(b)
        |RETURN r""".stripMargin

    val plan = expandConfig
      .plan(query)
      .stripProduceResults

    withClue("Used relationshipTypeScan when not expected:") {
      plan.folder.treeExists {
        case _: DirectedRelationshipTypeScan   => true
        case _: UndirectedRelationshipTypeScan => true
      } should be(false)
    }
  }

  test("should plan relationship type scan with filter for self loop with few relationships") {
    val query = "MATCH (a)-[r:REL]-(a) RETURN r"

    val config = plannerBuilder()
      .setAllNodesCardinality(100)
      .setRelationshipCardinality("()-[:REL]->()", 10)
      .build()

    val plan = config.plan(query).stripProduceResults

    plan should equal(
      config.subPlanBuilder()
        .filter("a = anon_0")
        .relationshipTypeScan("(a)-[r:REL]-(anon_0)")
        .build()
    )
  }

  test("should plan relationship type scan with filter for self loop for already bound start and end node with hint") {
    val query =
      """
        |MATCH (a)
        |WITH a SKIP 0
        |MATCH (a)-[r:REL]-(a)
        |USING SCAN r:REL
        |RETURN r""".stripMargin

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    plan should equal(
      relationshipTypeScanConfig.subPlanBuilder()
        .filter("a = anon_0", "a = anon_1")
        .apply()
        .|.relationshipTypeScan("(anon_0)-[r:REL]-(anon_1)", "a")
        .skip(0)
        .allNodeScan("a")
        .build()
    )
  }

  test("should start with a relationship type scan when node and relationship counts are the same") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(100000)
      .setRelationshipCardinality("()-[:REL]->()", 100000)
      .build()

    val query = "MATCH (a)-[r:REL]->(b) RETURN a"

    val plan = planner.plan(query).stripProduceResults
    plan shouldBe planner.subPlanBuilder()
      .relationshipTypeScan("(a)-[r:REL]->(b)")
      .build()
  }

  test("should not plan relationship type scan with filter for self loop with many relationships") {
    val query = "MATCH (a)-[r:REL]-(a) RETURN r"

    val plan = expandConfig
      .plan(query)
      .stripProduceResults

    withClue("Used relationshipTypeScan when not expected:") {
      plan.folder.treeExists {
        case _: DirectedRelationshipTypeScan   => true
        case _: UndirectedRelationshipTypeScan => true
      } should be(false)
    }
  }

  test("should plan additional filter after nodeIndexSeek with distance seekable predicate") {
    val pb = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 50)
      .addNodeIndex("Person", Seq("prop"), 0.5, 1, indexType = IndexType.POINT)
      .build()

    val query =
      """MATCH (n:Person)
        |WHERE point.distance(n.prop, point({x: 1.1, y: 5.4})) < 0.5
        |RETURN n
        |""".stripMargin

    val plan = pb.plan(query).stripProduceResults

    val expectedPlan = pb.subPlanBuilder()
      .filter("point.distance(cacheN[n.prop], point({x: 1.1, y: 5.4})) < 0.5")
      .pointDistanceNodeIndexSeek(
        "n",
        "Person",
        "prop",
        "{x: 1.1, y: 5.4}",
        0.5,
        indexOrder = IndexOrderNone,
        argumentIds = Set(),
        getValue = GetValue,
        indexType = IndexType.POINT
      )
      .build()

    plan shouldEqual expectedPlan
  }

  test("should work with label scans of label conjunctions only") {
    val cfg = plannerBuilder()
      .enablePlanningIntersectionScans()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .setLabelCardinality("P", 50)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |WHERE (n:L AND n:P)
        |RETURN n""".stripMargin
    )

    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .intersectionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .intersectionNodeByLabelsScan("n", Seq("L", "P"))
        .build()
    ))
  }

  test("should work with label scans of label conjunctions only and solve single scan hint") {
    val cfg = plannerBuilder()
      .enablePlanningIntersectionScans()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .setLabelCardinality("P", 50)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |USING SCAN n:L
        |WHERE n:L AND n:P
        |RETURN n""".stripMargin
    )

    // It is impossible to make up statistics where an AllNodeScan would be better, so we will get the same plan even without the hint
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .intersectionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .intersectionNodeByLabelsScan("n", Seq("L", "P"))
        .build()
    ))
  }

  test("should work with label scans of label conjunctions only and solve two scan hints") {
    val cfg = plannerBuilder()
      .enablePlanningIntersectionScans()
      .setAllNodesCardinality(100)
      .setLabelCardinality("L", 50)
      .setLabelCardinality("P", 50)
      .build()

    val plan = cfg.plan(
      """MATCH (n)
        |USING SCAN n:L
        |USING SCAN n:P
        |WHERE n:L AND n:P
        |RETURN n""".stripMargin
    )

    // It is impossible to make up statistics where an AllNodeScan would be better, so we will get the same plan even without the hint
    plan should (equal(
      cfg.planBuilder()
        .produceResults("n")
        .intersectionNodeByLabelsScan("n", Seq("P", "L"))
        .build()
    ) or equal(
      cfg.planBuilder()
        .produceResults("n")
        .intersectionNodeByLabelsScan("n", Seq("L", "P"))
        .build()
    ))
  }

  test("should not use intersection scan if there is a better node to start from") {
    val cfg = plannerBuilder()
      .enablePlanningIntersectionScans()
      .setRelationshipCardinality("()-[]->()", 100)
      .setRelationshipCardinality("(:A)-[]->()", 10)
      .setRelationshipCardinality("(:A)-[]->(:B)", 10)
      .setRelationshipCardinality("(:A)-[]->(:C)", 10)
      .setRelationshipCardinality("()-[]->(:B)", 100)
      .setRelationshipCardinality("()-[]->(:C)", 100)
      .setAllNodesCardinality(10000)
      .setLabelCardinality("A", 1)
      .setLabelCardinality("B", 50)
      .setLabelCardinality("C", 50)
      .build()

    val plan = cfg.plan(
      """MATCH (start:A)-[r]->(end:B&C)
        |RETURN end""".stripMargin
    )

    plan should (equal(
      cfg.planBuilder()
        .produceResults("end")
        .filterExpression(andsReorderable("end:B", "end:C"))
        .expandAll("(start)-[r]->(end)")
        .nodeByLabelScan("start", "A")
        .build()
    ))
  }

  private def plannerForIntersectionScanTests(aLabelCardinality: Double, bLabelCardinality: Double) = {
    plannerBuilder()
      .setLabelCardinality("A", aLabelCardinality)
      .setLabelCardinality("B", bLabelCardinality)
      .setAllNodesCardinality(aLabelCardinality + bLabelCardinality)
      .build()
  }

  test("should not prefer node by label scan when one label has much lower cardinality") {
    val aCardinality = 25_000_000
    val bCardinality = 10
    val planner = plannerForIntersectionScanTests(aCardinality, bCardinality)

    val q = "MATCH (n:A&B) RETURN count(n) AS result"
    val plan = planner.plan(q).stripProduceResults

    plan.leftmostLeaf shouldBe a[IntersectionNodeByLabelsScan]
  }

  test("should not prefer node by label scan when one label has cardinality <20% of the other label") {
    val aCardinality = 25_000_000
    val bCardinality = aCardinality * 0.2 - 1
    val planner = plannerForIntersectionScanTests(aCardinality, bCardinality)

    val q = "MATCH (n:A&B) RETURN count(n) AS result"
    val plan = planner.plan(q).stripProduceResults

    plan.leftmostLeaf shouldBe a[IntersectionNodeByLabelsScan]
  }

  test("should prefer intersection scan when one label has cardinality >20% of the other label") {
    val aCardinality = 25_000_000
    val bCardinality = aCardinality * 0.2 + 1
    val planner = plannerForIntersectionScanTests(aCardinality, bCardinality)

    val q = "MATCH (n:A&B) RETURN count(n) AS result"
    val plan = planner.plan(q).stripProduceResults

    plan.leftmostLeaf shouldBe a[IntersectionNodeByLabelsScan]
  }

  test("should prefer intersection scan with multiple hints") {
    val aCardinality = 25_000_000
    val bCardinality = 10
    val planner = plannerForIntersectionScanTests(aCardinality, bCardinality)

    val q =
      """MATCH (n:A&B)
        |USING SCAN n:A
        |USING SCAN n:B
        |RETURN count(n) AS result""".stripMargin

    val plan = planner.plan(q).stripProduceResults

    plan.leftmostLeaf shouldBe a[IntersectionNodeByLabelsScan]
  }

  test("should plan subtraction node label scan for one positive label and one negative label") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1000)
      .setAllNodesCardinality(5000)
      .build()

    val q = "MATCH (n:A&!B) RETURN n"
    val plan = planner.plan(q)

    plan should (equal(
      planner.planBuilder()
        .produceResults("n")
        .subtractionNodeByLabelsScan("n", Seq("A"), Seq("B"))
        .build()
    ))
  }

  test("should plan subtraction node label scan for one positive label and two negative label") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1000)
      .setLabelCardinality("C", 1000)
      .setAllNodesCardinality(5000)
      .build()

    val q = "MATCH (n:A&!B&!C) RETURN n"
    val plan = planner.plan(q)

    plan should (equal(
      planner.planBuilder()
        .produceResults("n")
        .subtractionNodeByLabelsScan("n", Seq("A"), Seq("B", "C"))
        .build()
    ))
  }

  test("should plan subtraction node label scan for two positive label and one negative label") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1000)
      .setLabelCardinality("C", 1000)
      .setAllNodesCardinality(5000)
      .build()

    val q = "MATCH (n:A&B&!C) RETURN n"
    val plan = planner.plan(q)

    plan should (equal(
      planner.planBuilder()
        .produceResults("n")
        .subtractionNodeByLabelsScan("n", Seq("A", "B"), Seq("C"))
        .build()
    ))
  }

  test("should plan subtraction node label scan for two positive label and two negative label") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1000)
      .setLabelCardinality("C", 1000)
      .setLabelCardinality("D", 1000)
      .setAllNodesCardinality(5000)
      .build()

    val q = "MATCH (n:!A&B&!C&D) RETURN n"
    val plan = planner.plan(q)

    plan should (equal(
      planner.planBuilder()
        .produceResults("n")
        .subtractionNodeByLabelsScan("n", Seq("B", "D"), Seq("A", "C"))
        .build()
    ))
  }

  test("should plan subtraction node label scan for two positive label and two negative label descending order") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1000)
      .setLabelCardinality("C", 1000)
      .setLabelCardinality("D", 1000)
      .setAllNodesCardinality(5000)
      .build()

    val q = "MATCH (n:!A&B&!C&D) RETURN n ORDER BY n DESC"
    val plan = planner.plan(q)

    plan should (equal(
      planner.planBuilder()
        .produceResults("n")
        .subtractionNodeByLabelsScan("n", Seq("B", "D"), Seq("A", "C"), IndexOrderDescending)
        .build()
    ))
  }

  test("should not plan subtraction node label scan when hint for relationship type scan is given") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1000)
      .setAllNodesCardinality(5000)
      .setAllRelationshipsCardinality(8000)
      .setRelationshipCardinality("()-[R1]->()", 4000)
      .setRelationshipCardinality("()-[R1]->(:A)", 400)
      .setRelationshipCardinality("()-[R1]->(:B)", 400)
      .build()

    val q = "MATCH (a)-[r:R1]->(b:!A&B) USING SCAN r:R1 RETURN r"
    val plan = planner.plan(q)

    plan should (equal(
      planner.planBuilder()
        .produceResults("r")
        .filter("b:B", "NOT b:A")
        .relationshipTypeScan("(a)-[r:R1]->(b)")
        .build()
    ))
  }

  test("should plan subtraction node label scan when hint for node label scan is given") {
    val planner = plannerBuilder()
      .setLabelCardinality("A", 1000)
      .setLabelCardinality("B", 1000)
      .setAllNodesCardinality(5000)
      .setAllRelationshipsCardinality(8000)
      .setRelationshipCardinality("()-[R1]->()", 4000)
      .setRelationshipCardinality("()-[R1]->(:A)", 400)
      .setRelationshipCardinality("()-[R1]->(:B)", 400)
      .build()

    val q = "MATCH (a)-[r:R1]->(b:!A&B) USING SCAN b:B RETURN r"
    val plan = planner.plan(q)

    plan should (equal(
      planner.planBuilder()
        .produceResults("r")
        .expandAll("(b)<-[r:R1]-(a)")
        .subtractionNodeByLabelsScan("b", Seq("B"), Seq("A"))
        .build()
    ))
  }
}
