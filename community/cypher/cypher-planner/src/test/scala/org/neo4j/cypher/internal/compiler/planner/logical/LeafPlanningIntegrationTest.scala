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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.compiler.planner.BeLikeMatcher.beLike
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningIntegrationTestSupport
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.LookupRelationshipsByTypeDisabled
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.StatisticsBackedLogicalPlanningConfigurationBuilder
import org.neo4j.cypher.internal.compiler.planner.StubbedLogicalPlanningConfiguration
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.Ascending
import org.neo4j.cypher.internal.logical.plans.Descending
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.ExclusiveBound
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.GetValue
import org.neo4j.cypher.internal.logical.plans.IndexOrderAscending
import org.neo4j.cypher.internal.logical.plans.IndexOrderDescending
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek.nodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
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
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.planner.spi.DelegatingGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MinimumGraphStatistics
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.ProvidedOrders
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.Extractors.SetExtractor
import org.neo4j.exceptions.HintException
import org.neo4j.exceptions.IndexHintException
import org.neo4j.graphdb.schema.IndexType

import java.lang.Boolean.TRUE

class LeafPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp with LogicalPlanningIntegrationTestSupport {

  test("should plan index seek by prefix for simple prefix search based on STARTS WITH with prefix") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' RETURN a")._2 should equal(
      nodeIndexSeek("a:Person(name STARTS WITH 'prefix')")
    )
  }

  test("should prefer cheaper optional expand over joins, even if not cheaper before rewriting") {
    (new given {
      cost = {
        case (_: RightOuterHashJoin, _, _, _) => 6.610321376825E9
        case (_: LeftOuterHashJoin, _, _, _) => 8.1523761738E9
        case (_: Apply, _, _, _) => 7.444573003149691E9
        case (_: OptionalExpand, _, _, _) => 4.76310362E8
        case (_: Optional, _, _, _) => 7.206417822149691E9
        case (_: Selection, _, _, _) => 1.02731056E8
        case (_: Expand, _, _, _) => 7.89155379E7
        case (_: AllNodesScan, _, _, _) => 3.50735724E7
        case (_: Argument, _, _, _) => 2.38155181E8
        case (_: ProjectEndpoints, _, _, _) => 11.0
      }
    } getLogicalPlanFor
      """UNWIND $createdRelationships as r
        |MATCH (source)-[r]->(target)
        |WITH source AS p
        |OPTIONAL MATCH (p)<-[follow]-() WHERE type(follow) STARTS WITH 'ProfileFavorites'
        |WITH p, count(follow) as fc
        |RETURN 1
      """.stripMargin)._2 should beLike {
      case Projection(Aggregation(_: OptionalExpand, _, _), _) => ()
    }
  }

  test("should plan index seek by prefix for simple prefix search based on CONTAINS substring") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name CONTAINS 'substring' RETURN a")._2 should equal(
      nodeIndexSeek("a:Person(name CONTAINS 'substring')")
    )
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'short' AND a.lastname STARTS WITH 'longer' RETURN a")
      ._2 should equal(
      Selection(ands(startsWith(prop("a", "name"), literalString("short"))),
        nodeIndexSeek("a:Person(lastname STARTS WITH 'longer')", propIds = Some(Map("lastname" -> 1)))
      ))
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHSs combined with AND, and choose the longer prefix even with predicates reversed") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.lastname STARTS WITH 'longer' AND a.name STARTS WITH 'short' RETURN a")
      ._2 should equal(
      Selection(ands(startsWith(prop("a", "name"), literalString("short"))),
        nodeIndexSeek("a:Person(lastname STARTS WITH 'longer')", propIds = Some(Map("lastname" -> 1)))
      ))
  }

  test("should plan index seek by prefix for prefix search based on multiple STARTS WITHs combined with AND NOT") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "lastname")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'longer' AND NOT a.lastname STARTS WITH 'short' RETURN a")
      ._2 should equal(
      Selection(ands(not(startsWith(prop("a", "lastname"), literalString("short")))),
        nodeIndexSeek("a:Person(name STARTS WITH 'longer')")
      ))
  }

  test("should plan property equality index seek instead of index seek by prefix") {
    (new given {
      indexOn("Person", "name")
      cardinality = mapCardinality(promoteOnlyPlansSolving(Set("a"), Set(hasLabels("a", "Person"), in(prop("a", "name"), listOfString("prefix1")))))

    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix' AND a.name = 'prefix1' RETURN a")._2 should equal(
      Selection(ands(startsWith(cachedNodeProp("a", "name"), literalString("prefix"))),
        nodeIndexSeek("a:Person(name = 'prefix1')", _ => GetValue)
      ))
  }

  test("should plan property equality index seek using IN instead of index seek by prefix") {
    (new given {
      indexOn("Person", "name")
      cardinality = mapCardinality(promoteOnlyPlansSolving(Set("a"), Set(hasLabels("a", "Person"), in(prop("a", "name"), listOfString("prefix1", "prefix2")))))

    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name STARTS WITH 'prefix%' AND a.name in ['prefix1', 'prefix2'] RETURN a")._2 should equal(
      Selection(ands(startsWith(cachedNodeProp("a", "name"), literalString("prefix%"))),
        NodeIndexSeek(
          "a",
          LabelToken("Person", LabelId(0)),
          Seq(indexedProperty("name", 0, GetValue, NODE_TYPE)),
          ManyQueryExpression(listOfString("prefix1", "prefix2")),
          Set.empty,
          IndexOrderNone,
          IndexType.BTREE)
      ))
  }

  test("should plan index seek by numeric range for numeric inequality predicate") {
    (new given {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.age < 12 RETURN a")._2 should equal(
      nodeIndexSeek("a:Person(age < 12)")
    )
  }

  test("should plan index seek by numeric range for numeric chained operator") {
    val than =  RangeGreaterThan(NonEmptyList(ExclusiveBound(literalInt(6))))
    val than1 = RangeLessThan(NonEmptyList(ExclusiveBound(literalInt(12))))
    (new given {
      indexOn("Person", "age")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE 6 < a.age < 12 RETURN a")._2 should equal(
      NodeIndexSeek(
        "a",
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
        IndexType.BTREE)
    )
  }

  test("should plan index seek for multiple inequality predicates and prefer the index seek with the lower cost per row") {
    (new given {
      indexOn("Person", "name")
      indexOn("Person", "age")
      cost = {
        case (_: AllNodesScan, _, _, _) => 1000.0
        case (_: NodeByLabelScan, _, _, _) => 50.0
        case (_: NodeIndexScan, _, _, _) => 10.0
        case (plan: NodeIndexSeek, _, _, _) if plan.properties.headOption.map(_.propertyKeyToken.name).contains("name") => 1.0
        case (plan: NodeIndexSeek, _, _, _) if plan.properties.headOption.map(_.propertyKeyToken.name).contains("age")  => 5.0
        case (Selection(_, source), x, y, z) => cost((source, x, y, z)) + 30.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.age > 40 AND a.name >= 'Cinderella' RETURN a")._2 should equal(
      Selection(
        Seq(greaterThan(prop("a", "age"), literalInt(40))),
        nodeIndexSeek("a:Person(name >= 'Cinderella')")
      )
    )
  }

  test("should plan index seek by string range for textual inequality predicate") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (a:Person) WHERE a.name >= 'Frodo' RETURN a")._2 should equal(
      nodeIndexSeek("a:Person(name >= 'Frodo')")
    )
  }

  test("should plan all nodes scans") {
    (new given {
    } getLogicalPlanFor "MATCH (n) RETURN n")._2 should equal(
      AllNodesScan("n", Set.empty)
    )
  }

  test("should plan label scans even without having a compile-time label id") {
    (new given {
      cost =  {
        case (_: AllNodesScan, _, _, _) => 1000.0
        case (_: NodeByIdSeek, _, _, _) => 2.0
        case (_: NodeByLabelScan, _, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n")._2 should equal(
      NodeByLabelScan("n", labelName("Awesome"), Set.empty, IndexOrderNone)
    )
  }

  test("should plan label scans when having a compile-time label id") {
    val plan = new given {
      cost =  {
        case (_: AllNodesScan, _, _, _) => 1000.0
        case (_: NodeByIdSeek, _, _, _) => 2.0
        case (_: NodeByLabelScan, _, _, _) => 1.0
        case _ => Double.MaxValue
      }
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) RETURN n"

    plan._2 should equal(
      NodeByLabelScan("n", labelName("Awesome"), Set.empty, IndexOrderNone)
    )
  }

  private val nodeIndexScanCost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities, ProvidedOrders), Cost] = {
    case (_: AllNodesScan, _, _, _) => 1000.0
    case (_: NodeByLabelScan, _, _, _) => 50.0
    case (_: NodeIndexScan, _, _, _) => 10.0
    case (_: NodeIndexContainsScan, _, _, _) => 10.0
    case (nodeIndexSeek: NodeIndexSeek, _, cardinalities, providedOrders) =>
      val planCardinality = cardinalities.get(nodeIndexSeek.id).amount
      val rowCost = 1.0
      val allNodesCardinality = 1000.0
      rowCost * planCardinality / allNodesCardinality
    case (Selection(_, plan), input, c, p) => nodeIndexScanCost((plan, input, c, p)) + 1.0
    case _ => Double.MaxValue
  }

  private val nodeIndexSeekCost: PartialFunction[(LogicalPlan, QueryGraphSolverInput, Cardinalities, ProvidedOrders), Cost] = {
    case (_: AllNodesScan, _, _, _) => 1000000000.0
    case (_: NodeIndexSeek, _, _, _) => 0.1
    case (Expand(plan, _, _, _, _, _, _), input, c, p) => nodeIndexSeekCost((plan, input, c, p))
    case (Selection(_, plan), input, c, p) => nodeIndexSeekCost((plan, input, c, p))
    case _ => 1000.0
  }

  private def promoteOnlyPlansSolving(patternNodes: Set[String], expressions: Set[Expression]): PartialFunction[PlannerQueryPart, Double] = {
    case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == patternNodes =>
      queryGraph.selections.predicates.map(_.expr) match {
        case es if es == expressions => 10.0
        case _                       => Double.MaxValue
      }

    case _ => Double.MaxValue
  }

  test("should plan index scan for exists(n.prop)") {
    val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE exists(n.prop) RETURN n"

    plan._2 should equal(nodeIndexSeek("n:Awesome(prop)"))
  }

  test("should plan index scan for n.prop IS NOT NULL") {
    val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN n"

    plan._2 should equal(nodeIndexSeek("n:Awesome(prop)"))
  }

  test("should plan unique index scan for n.prop IS NOT NULL") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL RETURN n"

    plan._2 should equal(nodeIndexSeek("n:Awesome(prop)"))
  }

  test("should plan index seek instead of index scan when there are predicates for both") {
    val plan = new given {
      indexOn("Awesome", "prop")
      cost = nodeIndexScanCost
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IS NOT NULL AND n.prop = 42 RETURN n"

    plan._2 should equal(
      Selection(ands(isNotNull(cachedNodeProp("n", "prop"))),
        nodeIndexSeek("n:Awesome(prop = 42)", _ => GetValue)
      ))
  }

  test("should plan index seek when there is an index on the property") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan._2 should equal(
      nodeIndexSeek("n:Awesome(prop = 42)")
    )
  }

  test("should plan unique index seek when there is an unique index on the property") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 RETURN n"

    plan._2 should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)),
        Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
        SingleQueryExpression(literalInt(42)), Set.empty, IndexOrderNone, IndexType.BTREE)
    )
  }

  test("should plan node by ID lookup instead of label scan when the node by ID lookup is cheaper") {
    (new given {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) = 42 RETURN n")._2 should equal (
      Selection(
        ands(hasLabels("n", "Awesome")),
        NodeByIdSeek("n", ManySeekableArgs(listOfInt(42)), Set.empty)
      )
    )
  }

  test("should plan node by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) IN $param RETURN n")._2 should equal (
      Selection(
        ands(hasLabels("n", "Awesome")),
        NodeByIdSeek("n", ManySeekableArgs(parameter("param", CTAny)), Set.empty)
      )
    )
  }

  test("should plan NodeByIdSeek and Argument instead of scans") {
    val query =
      """
        |MATCH (n)-[:REL]->(m)
        |WHERE id(m) = 1
        |OPTIONAL MATCH (n)-->(middle)-->(role:Role)
        |RETURN n
        |""".stripMargin

      val plan = (new given {
        statistics = new MinimumGraphStatistics(
          new DelegatingGraphStatistics(parent.graphStatistics) {
            override def nodesWithLabelCardinality(labelId: Option[LabelId]): Cardinality = Cardinality(10.0)
            override def nodesAllCardinality(): Cardinality = Cardinality(100.0)
            override def patternStepCardinality(fromLabel: Option[LabelId],
                                                relTypeId: Option[RelTypeId],
                                                toLabel: Option[LabelId]): Cardinality = Cardinality(0.0)
          }
        )
        lookupRelationshipsByType = LookupRelationshipsByTypeDisabled
      } getLogicalPlanFor query)._2

      plan should beLike {
        case Apply(
              Expand(
                NodeByIdSeek("m", _, _),
              "m", _, _, "n", _, _),
              Optional(
                Selection(_,
                  Expand(
                    Expand(
                        Argument(SetExtractor("n")),
                    _, _, _, _, _, _)
                  , _, _, _, _, _, _)
                ),
              _), _
            ) => ()
      }
  }

  test("should plan directed rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
    } getLogicalPlanFor "MATCH (a)-[r]->(b) WHERE id(r) IN $param RETURN a, r, b")._2 should equal (
      DirectedRelationshipByIdSeek("r", ManySeekableArgs(parameter("param", CTAny)), "a", "b", Set.empty)
    )
  }

  test("should plan undirected rel by ID lookup based on an IN predicate with a param as the rhs") {
    (new given {
    } getLogicalPlanFor "MATCH (a)-[r]-(b) WHERE id(r) IN $param RETURN a, r, b")._2 should equal (
      UndirectedRelationshipByIdSeek("r", ManySeekableArgs(parameter("param", CTAny)), "a", "b", Set.empty)
    )
  }

  test("should plan node by ID lookup based on an IN predicate") {
    (new given {
      knownLabels = Set("Awesome")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE id(n) IN [42, 64] RETURN n")._2 should equal (
      Selection(
        ands(hasLabels("n", "Awesome")),
        NodeByIdSeek("n", ManySeekableArgs(listOfInt(42, 64)), Set.empty)
      )
    )
  }

  test("should plan index seek when there is an index on the property and an IN predicate") {
    (new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IN [42] RETURN n")._2 should beLike {
      case NodeIndexSeek(
              "n",
              LabelToken("Awesome", _),
      Seq(IndexedProperty(PropertyKeyToken("prop", _), DoNotGetValue, NODE_TYPE)),
              SingleQueryExpression(SignedDecimalIntegerLiteral("42")), _, _, _) => ()
    }
  }

  test("should use indexes for large collections if it is a unique index") {
    val result = new given {
      cost =  {
        case (_: AllNodesScan, _, _, _)    => 10000.0
        case (_: NodeByLabelScan, _, _, _) =>  1000.0
        case (_: NodeByIdSeek, _, _, _)    =>     2.0
        case _                       => Double.MaxValue
      }
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop IN [1,2,3,4,5] RETURN n"

    result._2 should beLike {
      case _: NodeUniqueIndexSeek => ()
    }
  }

  //
  // Composite indexes
  // WHERE n:Label AND (n.prop = $val1 OR (n.prop = $val2 AND n.bar = $val3))

  test("should plan composite index seek when there is an index on two properties and both are in equality predicates") {
    val plan = new given {
      indexOn("Awesome", "prop", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop = 42 AND n.prop2 = 'foo' RETURN n"

    plan._2 should equal(
      nodeIndexSeek("n:Awesome(prop = 42, prop2 = 'foo')")
    )
  }

  test("should plan composite index seek when there is an index on two properties and both are in equality predicates regardless of predicate order") {
    val plan = new given {
      indexOn("Awesome", "prop", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop2 = 'foo' AND n.prop = 42 RETURN n"

    plan._2 should equal(
      nodeIndexSeek("n:Awesome(prop = 42, prop2 = 'foo')")
    )
  }

  test("should plan composite index seek and filter when there is an index on two properties and both are in equality predicates together with other predicates") {
    val plan = new given {
      indexOn("Awesome", "prop", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop2 = 'foo' AND n.name IS NOT NULL AND n.prop = 42 RETURN n"

    plan._2 should equal(
      Selection(ands(isNotNull(prop("n", "name"))),
        nodeIndexSeek("n:Awesome(prop = 42, prop2 = 'foo')")
      )
    )
  }

  //
  // index hints
  //

  test("should plan hinted label scans") {

    val plan = new given {
      cost = {
        case (_: Selection, _, _, _) => 20.0
        case (_: NodeHashJoin, _, _, _) => 1000.0
        case (_: NodeByLabelScan, _, _, _) => 20.0
      }
    } getLogicalPlanFor "MATCH (n:Foo:Bar:Baz) USING SCAN n:Bar RETURN n"

    plan._2 should equal(
      Selection(
        ands(hasLabels("n", "Foo"), hasLabels("n", "Baz")),
        NodeByLabelScan("n", labelName("Bar"), Set.empty, IndexOrderNone)
      )
    )
  }

  test("should plan hinted index seek") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan._2 should equal(
      nodeIndexSeek("n:Awesome(prop = 42)")
    )
  }

  test("should plan hinted index seek when returning *") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN *"

    plan._2 should equal(
      nodeIndexSeek("n:Awesome(prop = 42)")
    )
  }

  test("should plan hinted index seek with or") {
    val plan = new given {
      indexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n:Awesome) USING INDEX n:Awesome(prop) WHERE n.prop = 42 OR n.prop = 1337 RETURN n"

    plan._2 should equal(
      NodeIndexSeek("n", LabelToken("Awesome", LabelId(0)),
        Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
        ManyQueryExpression(listOfInt(42, 1337)), Set.empty, IndexOrderNone, IndexType.BTREE)
    )
  }

  test("should plan hinted index seek when there are multiple indices") {
    val plan = new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n "

    plan._2 should equal(
      Selection(
        ands(equals(prop("n", "prop1"), literalInt(42))),
        nodeIndexSeek("n:Awesome(prop2 = 3)", propIds = Some(Map("prop2" -> 1)))
      )
    )
  }

  test("should plan hinted index seek when there are multiple or indices") {
    val plan = new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND (n.prop1 = 42 OR n.prop2 = 3) RETURN n "

    val seek1 = nodeIndexSeek("n:Awesome(prop1 = 42)", _ => DoNotGetValue)
    val seek2 = nodeIndexSeek("n:Awesome(prop2 = 3)", _ => DoNotGetValue, propIds = Some(Map("prop2" -> 1)))
    val alt1 = Distinct(Union(seek2, seek1), Map("n" -> varFor("n")))
    val alt2 = Distinct(Union(seek1, seek2), Map("n" -> varFor("n")))

    plan._2 should (equal(alt1) or equal(alt2))
  }

  test("should plan hinted unique index seek") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop) WHERE n:Awesome AND n.prop = 42 RETURN n"

    plan._2 should equal(
      NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)),
        Seq(indexedProperty("prop", 0, DoNotGetValue, NODE_TYPE)),
        SingleQueryExpression(literalInt(42)), Set.empty, IndexOrderNone, IndexType.BTREE)
    )
  }

  test("should plan hinted unique index seek when there are multiple unique indices") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 = 3 RETURN n"

    plan._2 should equal(
      Selection(
        ands(equals(prop("n", "prop1"), literalInt(42))),
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 1, DoNotGetValue, NODE_TYPE)),
          SingleQueryExpression(literalInt(3)), Set.empty, IndexOrderNone, IndexType.BTREE)
      )
    )
  }

  test("should plan hinted unique index seek based on an IN predicate  when there are multiple unique indices") {
    val plan = new given {
      uniqueIndexOn("Awesome", "prop1")
      uniqueIndexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n) USING INDEX n:Awesome(prop2) WHERE n:Awesome AND n.prop1 = 42 and n.prop2 IN [3] RETURN n"

    plan._2 should equal(
      Selection(
        ands(equals(prop("n", "prop1"), literalInt(42))),
        NodeUniqueIndexSeek("n", LabelToken("Awesome", LabelId(0)),
          Seq(indexedProperty("prop2", 1, DoNotGetValue, NODE_TYPE)),
          SingleQueryExpression(literalInt(3)), Set.empty, IndexOrderNone, IndexType.BTREE)
      )
    )
  }

  private object nodeIndexHints {
    val config: StatisticsBackedLogicalPlanningConfigurationBuilder =
      plannerBuilder()
        .enablePlanningTextIndexes()
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

  test("should not plan node index for this query without hints") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.BTREE)
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
          .nodeIndexOperator("b:B(prop STARTS WITH 'x')", indexType = IndexType.TEXT)
          .build()
      )
  }

  test("should plan btree node index when index hint has btree type") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.BTREE)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    planner.plan(nodeIndexHints.query("USING BTREE INDEX b:B(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("a:A")
          .expandAll("(b)<-[r:R]-(a)")
          .nodeIndexOperator("b:B(prop STARTS WITH 'x')", indexType = IndexType.BTREE)
          .build()
      )
  }

  test("should plan text node index when index hint has text type") {

    val planner = nodeIndexHints.config
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.BTREE)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    planner.plan(nodeIndexHints.query("USING TEXT INDEX b:B(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("a:A")
          .expandAll("(b)<-[r:R]-(a)")
          .nodeIndexOperator("b:B(prop STARTS WITH 'x')", indexType = IndexType.TEXT)
          .build()
      )
  }

  test("should warn when index hint has btree type but the only matching index is text") {

    val planner = nodeIndexHints.config
      .withSetting(GraphDatabaseSettings.cypher_hints_error, TRUE)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    the[IndexHintException]
      .thrownBy(planner.plan(nodeIndexHints.query("USING BTREE INDEX b:B(prop)")))
      .getMessage.should(include("No such index: BTREE INDEX FOR (`b`:`B`) ON (`b`.`prop`)"))
  }

  test("should warn when index hint has text type but the only matching index is btree") {
    val planner = nodeIndexHints.config
      .withSetting(GraphDatabaseSettings.cypher_hints_error, TRUE)
      .addNodeIndex("B", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.BTREE)
      .build()

    the[IndexHintException]
      .thrownBy(planner.plan(nodeIndexHints.query("USING TEXT INDEX b:B(prop)")))
      .getMessage.should(include("No such index: TEXT INDEX FOR (`b`:`B`) ON (`b`.`prop`)"))
  }

  private object relIndexHints {
    val config: StatisticsBackedLogicalPlanningConfigurationBuilder =
      plannerBuilder()
        .enablePlanningTextIndexes()
        .setAllNodesCardinality(10)
        .setAllRelationshipsCardinality(100000)
        .setRelationshipCardinality("()-[:R]->()", 100000)

    def query(hint: String): String =
      s"""MATCH (a)-[r:R]->(b) $hint
         |WHERE r.prop STARTS WITH ''
         |RETURN a""".stripMargin
  }

  test("should not plan relationship index for this query without hints") {

    val planner = relIndexHints.config
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.BTREE)
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    planner.plan(relIndexHints.query(""))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .filter("r.prop STARTS WITH ''")
          .expandAll("(b)<-[r:R]-(a)")
          .allNodeScan("b")
          .build()
      )
  }

  test("should warn when index hint has text type but the only matching index is btree rel index") {

    val planner = relIndexHints.config
      .withSetting(GraphDatabaseSettings.cypher_hints_error, TRUE)
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.BTREE)
      .build()

    the[IndexHintException]
      .thrownBy(planner.plan(relIndexHints.query("USING TEXT INDEX r:R(prop)")))
      .getMessage.should(include("No such index: TEXT INDEX FOR ()-[`r`:`R`]-() ON (`r`.`prop`)"))
  }

  test("should plan text relationship index when index hint has text type") {

    val planner = relIndexHints.config
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.BTREE)
      .addRelationshipIndex("R", Seq("prop"), existsSelectivity = 1.0, uniqueSelectivity = 1.0, indexType = IndexType.TEXT)
      .build()

    planner.plan(relIndexHints.query("USING TEXT INDEX r:R(prop)"))
      .shouldEqual(
        planner.planBuilder()
          .produceResults("a")
          .relationshipIndexOperator("(a)-[r:R(prop STARTS WITH '')]->(b)", indexType = IndexType.TEXT)
          .build()
      )
  }

  test("should plan node by ID seek based on a predicate with an id collection variable as the rhs") {
    val plan = new given {
      cost =  {
        case (_: AllNodesScan, _, _, _) => 1000.0
        case (_: NodeByIdSeek, _, _, _) => 2.0
        case (_: NodeByLabelScan, _, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "WITH [0,1,3] AS arr MATCH (n) WHERE id(n) IN arr return count(*)"

    plan._2 should equal(
      Aggregation(
        Apply(
          Projection(Argument(),Map("arr" -> listOfInt(0, 1, 3))),
          NodeByIdSeek("n", ManySeekableArgs(varFor("arr")),Set("arr"))
        ),
        Map(), Map("count(*)" -> CountStar()_)
      )
    )
  }

  test("should use index on label and property") {
    val plan = (new given {
      indexOn("Crew", "name")
    } getLogicalPlanFor "MATCH (n:Crew) WHERE n.name = 'Neo' RETURN n")._2

    plan shouldBe using[NodeIndexSeek]
  }

  test("should use index when there are multiple labels on the node") {
    val plan = (new given {
      indexOn("Crew", "name")
      indexOn("Matrix", "someOther")
    } getLogicalPlanFor "MATCH (n:Matrix:Crew) WHERE n.name = 'Neo' RETURN n")._2

    plan shouldBe using[NodeIndexSeek]
  }

  test("should be able to OR together two index seeks") {
    val plan = (new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 = 42 OR n.prop2 = 'apa' RETURN n")._2

    val seek1 = nodeIndexSeek("n:Awesome(prop1 = 42)", _ => DoNotGetValue)
    val seek2 = nodeIndexSeek("n:Awesome(prop2 = 'apa')", _ => DoNotGetValue, propIds = Some(Map("prop2" -> 1)))
    val alt1 = Distinct(Union(seek2, seek1), Map("n" -> varFor("n")))
    val alt2 = Distinct(Union(seek1, seek2), Map("n" -> varFor("n")))

    plan should (equal(alt1) or equal(alt2))
  }

  test("should be able to OR together two index range seeks") {
    val plan = (new given {
      indexOn("Awesome", "prop1")
      indexOn("Awesome", "prop2")
    } getLogicalPlanFor "MATCH (n:Awesome) WHERE n.prop1 >= 42 OR n.prop2 STARTS WITH 'apa' RETURN n")._2

    val seek1 = nodeIndexSeek("n:Awesome(prop1 >= 42)")
    val seek2 = nodeIndexSeek("n:Awesome(prop2 STARTS WITH 'apa')", propIds = Some(Map("prop2" -> 1)))
    val alt1 = Distinct(Union(seek1, seek2), Map("n" -> varFor("n")))
    val alt2 = Distinct(Union(seek2, seek1), Map("n" -> varFor("n")))

    plan should (equal(alt1) or equal(alt2))
  }

  test("should use transitive closure to figure out we can use index") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexSeekCost
    } getLogicalPlanFor "MATCH (a:Person)-->(b) WHERE a.name = b.prop AND b.prop = 42 RETURN b")._2 should beLike {
      case Selection(_, Expand(NodeIndexSeek("a", _, _, _, _, _, _), _, _, _, _, _, _)) => ()
    }
  }

  test("should use transitive closure to figure out emergent equalities") {
    (new given {
      indexOn("Person", "name")
      cost = nodeIndexSeekCost
    } getLogicalPlanFor "MATCH (a:Person)-->(b) WHERE b.prop = a.name AND b.prop = 42 RETURN b")._2 should beLike {
      case Selection(_, Expand(NodeIndexSeek("a", _, _, _, _, _, _), _, _, _, _, _, _)) => ()
    }
  }

  //---------------------------------------------------------------------------
  // Test expand order with multiple configurations and
  // unsupported.cypher.plan_with_minimum_cardinality_estimates setting
  //
  // To succeed this test assumes:
  // *  (:A) should have lower cardinality than (:B) and (:C) so it is selected as starting point
  // * (a)--(b) should have lower cardinality than (a)--(c) so that it is expanded first
  //
  // Ideally (and at the time of writing) the intrinsic order when the cardinalities are equal
  // is different from the assertion and would cause failure
  private def testAndAssertExpandOrder(config: StubbedLogicalPlanningConfiguration) {
    val query = "MATCH (b:B)-[rB]->(a:A)<-[rC]-(c:C) RETURN a, b, c"

    val plan = (config getLogicalPlanFor query)._2

    // Expected plan
    // Since (a)--(b) has a lower cardinality estimate than (a)--(c) it should be selected first
    val scanA = NodeByLabelScan("a", labelName("A"), Set.empty, IndexOrderNone)
    val expandB = Expand(scanA, "a", INCOMING, Seq.empty, "b", "rB", ExpandAll)
    val selectionB = Selection(Seq(hasLabels("b", "B")), expandB)
    val expandC = Expand(selectionB, "a", INCOMING, Seq.empty, "c", "rC", ExpandAll)
    val selectionC = Selection(Seq(hasLabels("c", "C"), not(equals(varFor("rB"), varFor("rC")))), expandC)
    val expected = selectionC

    plan should equal(expected)
  }

  test("should pick expands in an order that minimizes early cardinality increase") {
    val config = new given {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("c") => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 200.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 300.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 100.0
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test("should pick expands in an order that minimizes early cardinality increase (plan_with_minimum_cardinality_estimates enabled)") {
    val config = new givenPlanWithMinimumCardinalityEnabled {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") => 1000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("c") => 2000.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 200.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 300.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 100.0
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test("should pick expands in an order that minimizes early cardinality increase with estimates < 1.0") {
    val config = new given {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") =>  5.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("c") => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 0.4
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 0.5
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 0.1
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  test("should pick expands in an order that minimizes early cardinality increase with estimates < 1.0 (plan_with_minimum_cardinality_estimates enabled)") {
    val config = new givenPlanWithMinimumCardinalityEnabled {
      cardinality = mapCardinality {
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") =>  5.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("c") => 10.0
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 0.4
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "c") => 0.5
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 0.1
        case _ => throw new IllegalStateException("Unexpected PlannerQuery")
      }
      knownLabels = Set("A", "B", "C")
    }
    testAndAssertExpandOrder(config)
  }

  private val relationshipTypeScanConfig: StatisticsBackedLogicalPlanningConfiguration = plannerBuilder()
    .setAllNodesCardinality(100)
    .setRelationshipCardinality("()-[:REL]->()", 10)
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
      .sort(Seq(Ascending("a"), Ascending("r")))
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
      .partialSort(Seq(Descending("r")), Seq(Descending("a")))
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
      plan.treeExists {
        case _: DirectedRelationshipTypeScan => true
        case _: UndirectedRelationshipTypeScan => true
      } should be(false)
    }
  }

  test("should plan relationship type scan with filter for self loop with few relationships") {
    val query = "MATCH (a)-[r:REL]-(a) RETURN r"

    val plan = relationshipTypeScanConfig
      .plan(query)
      .stripProduceResults

    plan should equal(
      relationshipTypeScanConfig.subPlanBuilder()
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

  test("should not plan relationship type scan with filter for self loop with many relationships") {
    val query = "MATCH (a)-[r:REL]-(a) RETURN r"

    val plan = expandConfig
      .plan(query)
      .stripProduceResults

    withClue("Used relationshipTypeScan when not expected:") {
      plan.treeExists {
        case _: DirectedRelationshipTypeScan => true
        case _: UndirectedRelationshipTypeScan => true
      } should be(false)
    }
  }

  test("should plan additional filter after nodeIndexSeek with distance seekable predicate") {
    val pb = plannerBuilder()
      .setAllNodesCardinality(100)
      .setLabelCardinality("Person", 50)
      .addNodeIndex("Person", Seq("prop"), 0.5, 1)
      .build()

    val query =
        """MATCH (n:Person)
          |WHERE point.distance(n.prop, point({x: 1.1, y: 5.4})) < 0.5
          |RETURN n
          |""".stripMargin

    val plan = pb.plan(query).stripProduceResults

    val expectedPlan = pb.subPlanBuilder()
      .filter("point.distance(n.prop, point({x: 1.1, y: 5.4})) < 0.5")
      .pointDistanceNodeIndexSeek("n", "Person", "prop", "{x: 1.1, y: 5.4}", 0.5, indexOrder = IndexOrderNone, argumentIds = Set(), getValue = DoNotGetValue)
      .build()

    plan shouldEqual expectedPlan
  }
}
