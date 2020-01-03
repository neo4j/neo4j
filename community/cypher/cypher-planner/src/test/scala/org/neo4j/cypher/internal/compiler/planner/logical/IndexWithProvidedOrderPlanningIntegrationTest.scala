/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.{Limit => LimitPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.{ASC, BOTH, DESC}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class IndexWithProvidedOrderPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with PlanMatchHelp {

  case class TestOrder(indexOrder: IndexOrder, cypherToken: String, indexOrderCapability: IndexOrderCapability, sortOrder: String => ColumnOrder)
  val ASCENDING = TestOrder(IndexOrderAscending, "ASC", ASC, Ascending)
  val DESCENDING = TestOrder(IndexOrderDescending, "DESC", DESC, Descending)
  val DESCENDING_BOTH = TestOrder(IndexOrderDescending, "DESC", BOTH, Descending)

  override val pushdownPropertyReads: Boolean = false

  for (TestOrder(plannedOrder, cypherToken, orderCapability, sortOrder) <- List(ASCENDING, DESCENDING, DESCENDING_BOTH)) {

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan sort if index does not provide order") {
      val plan = new given {
        indexOn("Awesome", "prop")
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Sort(
          Projection(
            IndexSeek(
              "n:Awesome(prop > 'foo')", indexOrder =
                IndexOrderNone),
            Map("n.prop" -> prop("n", "prop"))),
          Seq(sortOrder("n.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan partial sort if index does partially provide order") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken, n.foo ASC"

      plan._2 should equal(
        PartialSort(
          Projection(
            Projection(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
              Map("n.prop" -> prop("n", "prop"))),
            Map("n.foo" -> prop("n", "foo"))),
          Seq(sortOrder("n.prop")), Seq(Ascending("n.foo")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan partial sort if index does partially provide order and the second column is more complicated") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken, n.foo + 1 ASC"

      plan._2 should equal(
          PartialSort(
            Projection(
              Projection(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
                Map("n.prop" -> prop("n", "prop"))),
              Map("n.foo + 1" -> add(prop("n", "foo"), literalInt(1)))),
            Seq(sortOrder("n.prop")), Seq(Ascending("n.foo + 1")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan multiple partial sorts") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' WITH n, n.prop AS p, n.foo AS f ORDER BY p $cypherToken, f ASC RETURN p ORDER BY p $cypherToken, f ASC, n.bar ASC"


      plan._2 should equal(
        PartialSort(
          Projection(
            PartialSort(
              Projection(
                IndexSeek(
                  "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
                Map("f" -> prop("n", "foo"), "p" -> prop("n", "prop"))),
              Seq(sortOrder("p")), Seq(Ascending("f"))),
            Map("n.bar" -> prop("n", "bar"))),
          Seq(sortOrder("p"), Ascending("f")), Seq(Ascending("n.bar")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property renamed in an earlier WITH") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor
        s"""MATCH (n:Awesome) WHERE n.prop > 'foo'
           |WITH n AS nnn
           |MATCH (m)-[r]->(nnn)
           |RETURN nnn.prop ORDER BY nnn.prop $cypherToken""".stripMargin

      plan._2 should equal(
        Projection(
          Expand(
            Projection(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
              Map("nnn" -> varFor("n"))),
            "nnn", SemanticDirection.INCOMING, Seq.empty, "m", "r"),
          Map("nnn.prop" -> prop("nnn", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property renamed in same return") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor
        s"""MATCH (n:Awesome) WHERE n.prop > 'foo'
           |RETURN n AS m ORDER BY m.prop $cypherToken""".stripMargin

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
          Map("m" -> varFor("n")))
      )
    }

    test(s"$cypherToken-$orderCapability: Cannot order by index when ordering is on same property name, but different node") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (m:Awesome), (n:Awesome) WHERE n.prop > 'foo' RETURN m.prop ORDER BY m.prop $cypherToken"

      val expectedIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        Sort(
          Projection(
            CartesianProduct(
              IndexSeek(
                "n:Awesome(prop > 'foo')", indexOrder = expectedIndexOrder),
              NodeByLabelScan("m", labelName("Awesome"), Set.empty)),
            Map("m.prop" -> prop("m", "prop"))),
          Seq(sortOrder("m.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (starts with scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop STARTS WITH 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "n:Awesome(prop STARTS WITH 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    // This is supported because internally all kernel indexes which support ordering will just scan and filter to serve contains
    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (contains scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop CONTAINS 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop CONTAINS 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    // This is supported because internally all kernel indexes which support ordering will just scan and filter to serve ends with
    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (ends with scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop ENDS WITH 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek("n:Awesome(prop ENDS WITH 'foo')", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (scan)") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE EXISTS(n.prop) RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "n:Awesome(prop)", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an Apply") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        indexOn("B", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop > 'foo' AND a.prop = b.prop RETURN a.prop ORDER BY a.prop $cypherToken"

      val expectedBIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        Projection(
          Apply(
            IndexSeek(
              "a:A(prop > 'foo')", indexOrder = plannedOrder),
            IndexSeek("b:B(prop = ???)",
              indexOrder = expectedBIndexOrder,
              paramExpr = Some(cachedNodeProp("a", "prop")),
              labelId = 1,
              argumentIds = Set("a"))
          ),
          Map("a.prop" -> cachedNodeProp("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed properties in a plan with an Apply needs Partial Sort if RHS order required") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        indexOn("B", "prop").providesOrder(orderCapability)
        // This query is very fragile in the sense that the slightest modification will result in a stupid plan
      } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop STARTS WITH 'foo' AND b.prop > a.prop RETURN a.prop, b.prop ORDER BY a.prop $cypherToken, b.prop $cypherToken"

      val expectedBIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        PartialSort(
          Projection(
            Apply(
              IndexSeek("a:A(prop STARTS WITH 'foo')", indexOrder = plannedOrder),
              IndexSeek("b:B(prop > ???)",
                indexOrder = expectedBIndexOrder,
                paramExpr = Some(cachedNodeProp("a", "prop")),
                labelId = 1,
                argumentIds = Set("a"))
            ),
            Map("a.prop" -> cachedNodeProp("a", "prop"), "b.prop" -> prop("b", "prop"))),
          Seq(sortOrder("a.prop")), Seq(sortOrder("b.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an renaming Projection") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (a:A) WHERE a.prop > 'foo' WITH a.prop AS theProp, 1 AS x RETURN theProp ORDER BY theProp $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "a:A(prop > 'foo')", indexOrder = plannedOrder),
          Map("theProp" -> prop("a", "prop"), "x" -> literalInt(1)))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an aggregation and an expand") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN a.prop, count(b) ORDER BY a.prop $cypherToken"

      plan._2 should equal(
        OrderedAggregation(
          Expand(
            IndexSeek(
              "a:A(prop > 'foo')", indexOrder = plannedOrder),
            "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
          Map("a.prop" -> cachedNodeProp("a", "prop")), Map("count(b)" -> count(varFor("b"))), Seq(cachedNodeProp("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with partial provided order and with an expand") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken, b.prop"

      plan._2 should equal(
        PartialSort(
          Projection(
            Projection(
              Expand(
                IndexSeek(
                  "a:A(prop > 'foo')", indexOrder = plannedOrder),
                "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
              Map("a.prop" -> prop("a", "prop"))),
            Map("b.prop" -> prop("b", "prop"))),
          Seq(sortOrder("a.prop")), Seq(Ascending("b.prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with partial provided order and with two expand - should plan partial sort in the middle") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("c") => 2000.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 50.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b", "c") => 500.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b", "c") => 1000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b)-[q]->(c) WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken, b.prop"

      plan._2 should equal(
        Selection(Seq(not(equals(varFor("q"), varFor("r")))),
          Expand(
            PartialSort(
              Projection(
                Projection(
                  Expand(
                    IndexSeek(
                      "a:A(prop > 'foo')", indexOrder = plannedOrder),
                    "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
                  Map("a.prop" -> prop("a", "prop"))),
                Map("b.prop" -> prop("b", "prop"))),
              Seq(sortOrder("a.prop")), Seq(Ascending("b.prop"))),
            "b", SemanticDirection.OUTGOING, Seq.empty, "c", "q"))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a distinct") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN DISTINCT a.prop ORDER BY a.prop $cypherToken"

      plan._2 should equal(
        OrderedDistinct(
          Expand(
            IndexSeek(
              "a:A(prop > 'foo')", indexOrder = plannedOrder),
            "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
          Map("a.prop" -> cachedNodeProp("a", "prop")),
          Seq(cachedNodeProp("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an outer join") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at b
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 100.0
          case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternNodes == Set("b") => 20.0
        }
      } getLogicalPlanFor s"MATCH (b) OPTIONAL MATCH (a:A)-[r]->(b) USING JOIN ON b WHERE a.prop > 'foo' RETURN a.prop ORDER BY a.prop $cypherToken"

      plan._2 should equal(
        Projection(
          LeftOuterHashJoin(Set("b"),
            AllNodesScan("b", Set.empty),
            Expand(
              IndexSeek(
                "a:A(prop > 'foo')", indexOrder = plannedOrder),
              "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r")),
          Map("a.prop" -> prop("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a tail apply") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor
        s"""MATCH (a:A) WHERE a.prop > 'foo' WITH a SKIP 0
           |MATCH (b)
           |RETURN a.prop, b ORDER BY a.prop $cypherToken""".stripMargin

      plan._2 should equal(
        Projection(
          Apply(
            SkipPlan(
              IndexSeek(
                "a:A(prop > 'foo')", indexOrder = plannedOrder),
              literalInt(0)),
            AllNodesScan("b", Set("a"))),
          Map("a.prop" -> prop("a", "prop")))
      )
    }

    // Given that we could get provided order for this query (by either a type constraint
    // or kernel support for ordering in point index), it should be possible to skip the sorting
    // for this case. Right now this only works on integration test level and not in production.
    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order (scan) in case of existence constraint") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
        existenceOrNodeKeyConstraintOn("Awesome", Set("prop"))
      } getLogicalPlanFor s"MATCH (n:Awesome) RETURN n.prop ORDER BY n.prop $cypherToken"

      plan._2 should equal(
        Projection(
          IndexSeek(
            "n:Awesome(prop)", indexOrder = plannedOrder),
          Map("n.prop" -> prop("n", "prop")))
      )
    }
  }

  // Composite index

  test("Order by index backed for composite index on range") {
    val projectionBoth = Map(
      "n.prop1" -> cachedNodeProp("n", "prop1"),
      "n.prop2" -> cachedNodeProp("n", "prop2")
    )
    val projectionProp1 = Map("n.prop1" -> cachedNodeProp("n", "prop1"))
    val projectionProp2 = Map("n.prop2" -> cachedNodeProp("n", "prop2"))

    val expr = ands(lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)))

    Seq(
      // Ascending index
      ("n.prop1 ASC", ASC, IndexOrderAscending, false, false, Seq.empty, false, Seq.empty),
      ("n.prop1 DESC", ASC, IndexOrderAscending, true, true, Seq(Descending("n.prop1")), false, Seq.empty),
      ("n.prop1 ASC, n.prop2 ASC", ASC, IndexOrderAscending, false, false, Seq.empty, false, Seq.empty),
      ("n.prop1 ASC, n.prop2 DESC", ASC, IndexOrderAscending, false, false, Seq(Descending("n.prop2")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC", ASC, IndexOrderAscending, true, false, Seq(Descending("n.prop1"), Ascending("n.prop2")), false, Seq.empty),
      ("n.prop1 DESC, n.prop2 DESC", ASC, IndexOrderAscending, true, false, Seq(Descending("n.prop1"), Descending("n.prop2")), false, Seq.empty),

      // Descending index
      ("n.prop1 ASC", DESC, IndexOrderDescending, true, true, Seq(Ascending("n.prop1")), false, Seq.empty),
      ("n.prop1 DESC", DESC, IndexOrderDescending, false, false, Seq.empty, false, Seq.empty),
      ("n.prop1 ASC, n.prop2 ASC", DESC, IndexOrderDescending, true, false, Seq(Ascending("n.prop1"), Ascending("n.prop2")), false, Seq.empty),
      ("n.prop1 ASC, n.prop2 DESC", DESC, IndexOrderDescending, true, false, Seq(Ascending("n.prop1"), Descending("n.prop2")), false, Seq.empty),
      ("n.prop1 DESC, n.prop2 ASC", DESC, IndexOrderDescending, false, false, Seq(Ascending("n.prop2")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 DESC", DESC, IndexOrderDescending, false, false, Seq.empty, false, Seq.empty),

      // Both index
      ("n.prop1 ASC", BOTH, IndexOrderAscending, false, false, Seq.empty, false, Seq.empty),
      ("n.prop1 DESC", BOTH, IndexOrderDescending, false, false, Seq.empty, false, Seq.empty),
      ("n.prop1 ASC, n.prop2 ASC", BOTH, IndexOrderAscending, false, false, Seq.empty, false, Seq.empty),
      ("n.prop1 ASC, n.prop2 DESC", BOTH, IndexOrderAscending, false, false, Seq(Descending("n.prop2")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC", BOTH, IndexOrderDescending, false, false, Seq(Ascending("n.prop2")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 DESC", BOTH, IndexOrderDescending, false, false, Seq.empty, false, Seq.empty)
    ).foreach {
      case (orderByString, orderCapability, indexOrder, shouldFullSort, sortOnOnlyOne, sortItems, shouldPartialSort, alreadySorted) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3
             |RETURN n.prop1, n.prop2
             |ORDER BY $orderByString""".stripMargin
        val plan = new given {
          indexOn("Label", "prop1", "prop2").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor query

        // Then
        val leafPlan = IndexSeek("n:Label(prop1 >= 42, prop2 <= 3)", indexOrder = indexOrder, getValue = GetValue)
        plan._2 should equal {
          if (shouldPartialSort)
            PartialSort(Projection(Selection(expr, leafPlan), projectionBoth), alreadySorted, sortItems)
          else if (shouldFullSort && sortOnOnlyOne)
            Projection(Sort(Projection(Selection(expr, leafPlan), projectionProp1), sortItems), projectionProp2)
          else if (shouldFullSort && !sortOnOnlyOne)
            Sort(Projection(Selection(expr, leafPlan), projectionBoth), sortItems)
          else
            Projection(Selection(expr, leafPlan), projectionBoth)
        }
    }
  }

  test("Order by partially index backed for composite index on part of the order by") {
    val asc = Seq(Ascending("n.prop1"), Ascending("n.prop2"))
    val ascProp3 = Seq(Ascending("n.prop3"))
    val desc = Seq(Descending("n.prop1"), Descending("n.prop2"))
    val descProp3 = Seq(Descending("n.prop3"))

    Seq(
      // Ascending index
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", ASC, IndexOrderAscending, asc, ascProp3),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", ASC, IndexOrderAscending, asc, descProp3),

      // Descending index
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", DESC, IndexOrderDescending, desc, ascProp3),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", DESC, IndexOrderDescending, desc, descProp3),

      // Both index
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", BOTH, IndexOrderAscending, asc, ascProp3),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", BOTH, IndexOrderAscending, asc, descProp3),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", BOTH, IndexOrderDescending, desc, ascProp3),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", BOTH, IndexOrderDescending, desc, descProp3)
    ).foreach {
      case (orderByString, orderCapability, indexOrder, alreadySorted, toBeSorted) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3
             |RETURN n.prop1, n.prop2, n.prop3
             |ORDER BY $orderByString""".stripMargin
        val plan = new given {
          indexOn("Label", "prop1", "prop2").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor query

        // Then
        plan._2 should equal(
          PartialSort(
            Projection(
              Selection(
                ands(lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3))),
                IndexSeek("n:Label(prop1 >= 42, prop2)", indexOrder = indexOrder, getValue = GetValue)
              ),
              Map(
                "n.prop1" -> cachedNodeProp("n", "prop1"),
                "n.prop2" -> cachedNodeProp("n", "prop2"),
                "n.prop3" -> prop("n", "prop3")
              )
            ),
            alreadySorted,
            toBeSorted
          )
        )
    }
  }

  test("Order by index backed for composite index on more properties") {
    val expr = ands(
      lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)),
      greaterThan(cachedNodeProp("n", "prop3"), literalString("a")),
      lessThan(cachedNodeProp("n", "prop4"), literalString("f"))
    )
    val projectionsAll = Map(
      "n.prop1" -> cachedNodeProp("n", "prop1"),
      "n.prop2" -> cachedNodeProp("n", "prop2"),
      "n.prop3" -> cachedNodeProp("n", "prop3"),
      "n.prop4" -> cachedNodeProp("n", "prop4")
    )

    val ascAll = Seq(Ascending("n.prop1"), Ascending("n.prop2"), Ascending("n.prop3"), Ascending("n.prop4"))
    val descAll = Seq(Descending("n.prop1"), Descending("n.prop2"), Descending("n.prop3"), Descending("n.prop4"))
    val asc1_2 = Seq(Ascending("n.prop1"), Ascending("n.prop2"))
    val desc1_2 = Seq(Descending("n.prop1"), Descending("n.prop2"))

    Seq(
      // Ascending index
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 ASC", ASC, IndexOrderAscending, false,
        Seq.empty, false, Seq.empty),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 DESC", ASC, IndexOrderAscending, false,
        Seq(Descending("n.prop4")), true, Seq(Ascending("n.prop1"), Ascending("n.prop2"), Ascending("n.prop3"))),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 ASC", ASC, IndexOrderAscending, false,
        Seq(Descending("n.prop3"), Ascending("n.prop4")), true, asc1_2),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 DESC", ASC, IndexOrderAscending, false,
        Seq(Descending("n.prop3"), Descending("n.prop4")), true, asc1_2),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 ASC, n.prop4 ASC", ASC, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Ascending("n.prop3"), Ascending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 ASC, n.prop4 DESC", ASC, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Ascending("n.prop3"), Descending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC", ASC, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Descending("n.prop3"), Ascending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC", ASC, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Descending("n.prop3"), Descending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC", ASC, IndexOrderAscending, true,
        descAll, false, Seq.empty),

      // Descending index
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC", DESC, IndexOrderDescending, false,
        Seq.empty, false, Seq.empty),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC", DESC, IndexOrderDescending, false,
        Seq(Ascending("n.prop4")), true, Seq(Descending("n.prop1"), Descending("n.prop2"), Descending("n.prop3"))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 DESC", DESC, IndexOrderDescending, false,
        Seq(Ascending("n.prop3"), Descending("n.prop4")), true, desc1_2),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 ASC", DESC, IndexOrderDescending, false,
        Seq(Ascending("n.prop3"), Ascending("n.prop4")), true, desc1_2),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 DESC, n.prop4 DESC", DESC, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Descending("n.prop3"), Descending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 DESC, n.prop4 ASC", DESC, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Descending("n.prop3"), Ascending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 ASC, n.prop4 DESC", DESC, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Ascending("n.prop3"), Descending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 ASC, n.prop4 ASC", DESC, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Ascending("n.prop3"), Ascending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 ASC", DESC, IndexOrderDescending, true,
        ascAll, false, Seq.empty),

      // Both index
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 ASC", BOTH, IndexOrderAscending, false,
        Seq.empty, false, Seq.empty),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC, n.prop4 DESC", BOTH, IndexOrderAscending, false,
        Seq(Descending("n.prop4")), true, Seq(Ascending("n.prop1"), Ascending("n.prop2"), Ascending("n.prop3"))),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 ASC", BOTH, IndexOrderAscending, false,
        Seq(Descending("n.prop3"), Ascending("n.prop4")), true, asc1_2),
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 DESC, n.prop4 DESC", BOTH, IndexOrderAscending, false,
        Seq(Descending("n.prop3"), Descending("n.prop4")), true, asc1_2),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 ASC, n.prop4 ASC", BOTH, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Ascending("n.prop3"), Ascending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 ASC, n.prop4 DESC", BOTH, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Ascending("n.prop3"), Descending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC", BOTH, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Descending("n.prop3"), Ascending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 ASC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC", BOTH, IndexOrderAscending, false,
        Seq(Descending("n.prop2"), Descending("n.prop3"), Descending("n.prop4")), true, Seq(Ascending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 ASC", BOTH, IndexOrderDescending, false,
        Seq(Ascending("n.prop4")), true, Seq(Descending("n.prop1"), Descending("n.prop2"), Descending("n.prop3"))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 DESC", BOTH, IndexOrderDescending, false,
        Seq(Ascending("n.prop3"), Descending("n.prop4")), true, desc1_2),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 ASC, n.prop4 ASC", BOTH, IndexOrderDescending, false,
        Seq(Ascending("n.prop3"), Ascending("n.prop4")), true, desc1_2),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 DESC, n.prop4 DESC", BOTH, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Descending("n.prop3"), Descending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 DESC, n.prop4 ASC", BOTH, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Descending("n.prop3"), Ascending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 ASC, n.prop4 DESC", BOTH, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Ascending("n.prop3"), Descending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 ASC, n.prop3 ASC, n.prop4 ASC", BOTH, IndexOrderDescending, false,
        Seq(Ascending("n.prop2"), Ascending("n.prop3"), Ascending("n.prop4")), true, Seq(Descending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC, n.prop4 DESC", BOTH, IndexOrderDescending, false,
        Seq.empty, false, Seq.empty)
    ).foreach {
      case (orderByString, orderCapability, indexOrder, fullSort, sortItems, partialSort, alreadySorted) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3 AND n.prop3 > 'a' AND n.prop4 < 'f'
             |RETURN n.prop1, n.prop2, n.prop3, n.prop4
             |ORDER BY $orderByString""".stripMargin
        val plan = new given {
          indexOn("Label", "prop1", "prop2", "prop3", "prop4").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor query

        // Then
        val leafPlan = IndexSeek("n:Label(prop1 >= 42, prop2 <= 3, prop3 > 'a', prop4 < 'f')", indexOrder = indexOrder, getValue = GetValue)
        plan._2 should equal {
          if (fullSort)
            Sort(Projection(Selection(expr, leafPlan), projectionsAll), sortItems)
          else if (partialSort)
            PartialSort(Projection(Selection(expr, leafPlan), projectionsAll), alreadySorted, sortItems)
          else
            Projection(Selection(expr, leafPlan), projectionsAll)
        }
    }
  }

  test("Order by index backed for composite index on more properties than is ordered on") {
    val expr = ands(
      lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)),
      greaterThan(cachedNodeProp("n", "prop3"), literalString("a")),
      lessThan(cachedNodeProp("n", "prop4"), literalString("f"))
    )
    val projectionsAll = Map(
      "n.prop1" -> cachedNodeProp("n", "prop1"),
      "n.prop2" -> cachedNodeProp("n", "prop2"),
      "n.prop3" -> cachedNodeProp("n", "prop3"),
      "n.prop4" -> cachedNodeProp("n", "prop4")
    )
    val projections_1_2_4 = Map(
      "n.prop1" -> cachedNodeProp("n", "prop1"),
      "n.prop2" -> cachedNodeProp("n", "prop2"),
      "n.prop4" -> cachedNodeProp("n", "prop4")
    )
    val projections_1_3_4 = Map(
      "n.prop1" -> cachedNodeProp("n", "prop1"),
      "n.prop3" -> cachedNodeProp("n", "prop3"),
      "n.prop4" -> cachedNodeProp("n", "prop4")
    )

    Seq(
      ("n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", BOTH, IndexOrderAscending, false,
        projectionsAll, Map.empty[String, Expression], Seq.empty, Seq.empty),
      ("n.prop1 ASC, n.prop2 ASC, n.prop4 ASC", BOTH, IndexOrderAscending, true,
        projections_1_2_4, Map("n.prop3" -> cachedNodeProp("n", "prop3")),
        Seq(Ascending("n.prop4")), Seq(Ascending("n.prop1"), Ascending("n.prop2"))),
      ("n.prop1 ASC, n.prop3 ASC, n.prop4 ASC", BOTH, IndexOrderAscending, true,
        projections_1_3_4, Map("n.prop2" -> cachedNodeProp("n", "prop2")),
        Seq(Ascending("n.prop3"), Ascending("n.prop4")), Seq(Ascending("n.prop1"))),
      ("n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", BOTH, IndexOrderDescending, false,
        projectionsAll, Map.empty[String, Expression], Seq.empty, Seq.empty),
      ("n.prop1 DESC, n.prop2 DESC, n.prop4 DESC", BOTH, IndexOrderDescending, true,
        projections_1_2_4, Map("n.prop3" -> cachedNodeProp("n", "prop3")),
        Seq(Descending("n.prop4")), Seq(Descending("n.prop1"), Descending("n.prop2"))),
      ("n.prop1 DESC, n.prop3 DESC, n.prop4 DESC", BOTH, IndexOrderDescending, true,
        projections_1_3_4, Map("n.prop2" -> cachedNodeProp("n", "prop2")),
        Seq(Descending("n.prop3"), Descending("n.prop4")), Seq(Descending("n.prop1"))),
    ).foreach {
      case (orderByString, orderCapability, indexOrder, sort, projections, projectionsAfterSort, toSort, alreadySorted) =>
        // When
        val query =
          s"""MATCH (n:Label)
             |WHERE n.prop1 >= 42 AND n.prop2 <= 3 AND n.prop3 > 'a' AND n.prop4 < 'f'
             |RETURN n.prop1, n.prop2, n.prop3, n.prop4
             |ORDER BY $orderByString""".stripMargin
        val plan = new given {
          indexOn("Label", "prop1", "prop2", "prop3", "prop4").providesOrder(orderCapability).providesValues()
        } getLogicalPlanFor query

        // Then
        val leafPlan = IndexSeek("n:Label(prop1 >= 42, prop2 <= 3, prop3 > 'a', prop4 < 'f')", indexOrder = indexOrder, getValue = GetValue)
        plan._2 should equal {
          if (sort)
            Projection(PartialSort(Projection(Selection(expr, leafPlan), projections), alreadySorted, toSort), projectionsAfterSort)
          else
            Projection(Selection(expr, leafPlan), projections)
        }
    }
  }

  test("Order by index backed for composite index when not returning same as order on") {
    val expr = ands(
      lessThanOrEqual(prop("n", "prop2"), literalInt(3)),
      greaterThan(prop("n", "prop3"), literalString(""))
    )
    val expr_2_3_cached = ands(
      lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)),
      greaterThan(cachedNodeProp("n", "prop3"), literalString(""))
    )
    val expr_2_cached = ands(
      lessThanOrEqual(cachedNodeProp("n", "prop2"), literalInt(3)),
      greaterThan(prop("n", "prop3"), literalString(""))
    )
    val expr_3_cached = ands(
      lessThanOrEqual(prop("n", "prop2"), literalInt(3)),
      greaterThan(cachedNodeProp("n", "prop3"), literalString(""))
    )
    val map_empty = Map.empty[String, Expression] // needed for correct type
    val map_1 = Map("n.prop1" -> prop("n", "prop1"))
    val map_2_cached = Map("n.prop2" -> cachedNodeProp("n", "prop2"))
    val map_3_cached = Map("n.prop3" -> cachedNodeProp("n", "prop3"))
    val map_2_3_cached = Map("n.prop2" -> cachedNodeProp("n", "prop2"), "n.prop3" -> cachedNodeProp("n", "prop3"))

    val asc_1 = Seq(Ascending("n.prop1"))
    val desc_1 = Seq(Descending("n.prop1"))
    val asc_3 = Seq(Ascending("n.prop3"))
    val desc_3 = Seq(Descending("n.prop3"))
    val asc_1_2 = Seq(Ascending("n.prop1"), Ascending("n.prop2"))
    val desc_1_2 = Seq(Descending("n.prop1"), Descending("n.prop2"))
    val asc_2_3 = Seq(Ascending("n.prop2"), Ascending("n.prop3"))
    val desc_2_asc_3 = Seq(Descending("n.prop2"), Ascending("n.prop3"))
    val asc_1_2_3 = Seq(Ascending("n.prop1"), Ascending("n.prop2"), Ascending("n.prop3"))
    val desc_1_2_3 = Seq(Descending("n.prop1"), Descending("n.prop2"), Descending("n.prop3"))

    Seq(
      // Ascending index
      ("n.prop1", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", ASC, IndexOrderAscending, false, false, map_1, map_empty, expr, Seq.empty, Seq.empty),
      ("n.prop1", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", ASC, IndexOrderAscending, false, true, map_1, map_2_3_cached, expr_2_3_cached, desc_3, asc_1_2),
      ("n.prop1", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", ASC, IndexOrderAscending, false, true, map_1, map_2_3_cached, expr_2_3_cached, desc_2_asc_3, asc_1),
      ("n.prop1", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", ASC, IndexOrderAscending, true, false, map_1, map_2_3_cached, expr_2_3_cached, desc_1_2_3, Seq.empty),
      ("n.prop2", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", ASC, IndexOrderAscending, false, false, map_2_cached, map_empty, expr_2_cached, Seq.empty, Seq.empty),
      ("n.prop2", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", ASC, IndexOrderAscending, true, false, map_2_cached, map_1 ++ map_3_cached, expr_2_3_cached, desc_1_2_3, Seq.empty),
      ("n.prop3", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", ASC, IndexOrderAscending, false, false, map_3_cached, map_empty, expr_3_cached, Seq.empty, Seq.empty),
      ("n.prop3", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", ASC, IndexOrderAscending, true, false, map_3_cached, map_1 ++ map_2_cached, expr_2_3_cached, desc_1_2_3, Seq.empty),

      ("n.prop1, n.prop2", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", ASC, IndexOrderAscending, false, false, map_1 ++ map_2_cached, map_empty, expr_2_cached, Seq.empty, Seq.empty),
      ("n.prop1, n.prop2", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", ASC, IndexOrderAscending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, desc_3, asc_1_2),
      ("n.prop1, n.prop2", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", ASC, IndexOrderAscending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, desc_2_asc_3, asc_1),
      ("n.prop1, n.prop2", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", ASC, IndexOrderAscending, true, false, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, desc_1_2_3, Seq.empty),
      ("n.prop1, n.prop3", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", ASC, IndexOrderAscending, false, false, map_1 ++ map_3_cached, map_empty, expr_3_cached, Seq.empty, Seq.empty),
      ("n.prop1, n.prop3", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", ASC, IndexOrderAscending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, desc_3, asc_1_2),
      ("n.prop1, n.prop3", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", ASC, IndexOrderAscending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, desc_2_asc_3, asc_1),
      ("n.prop1, n.prop3", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", ASC, IndexOrderAscending, true, false, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, desc_1_2_3, Seq.empty),


      // Descending index
      ("n.prop1", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, true, false, map_1, map_2_3_cached, expr_2_3_cached, asc_1_2_3, Seq.empty),
      ("n.prop1", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", DESC, IndexOrderDescending, false, true, map_1, map_2_3_cached, expr_2_3_cached, asc_3, desc_1_2),
      ("n.prop1", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, false, true, map_1, map_2_3_cached, expr_2_3_cached, asc_2_3, desc_1),
      ("n.prop1", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", DESC, IndexOrderDescending, false, false, map_1, map_empty, expr, Seq.empty, Seq.empty),
      ("n.prop2", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, true, false, map_2_cached, map_1 ++ map_3_cached, expr_2_3_cached, asc_1_2_3, Seq.empty),
      ("n.prop2", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", DESC, IndexOrderDescending, false, false, map_2_cached, map_empty, expr_2_cached, Seq.empty, Seq.empty),
      ("n.prop3", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, true, false, map_3_cached, map_1 ++ map_2_cached, expr_2_3_cached, asc_1_2_3, Seq.empty),
      ("n.prop3", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", DESC, IndexOrderDescending, false, false, map_3_cached, map_empty, expr_3_cached, Seq.empty, Seq.empty),

      ("n.prop1, n.prop2", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, true, false, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, asc_1_2_3, Seq.empty),
      ("n.prop1, n.prop2", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, asc_2_3, desc_1),
      ("n.prop1, n.prop2", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", DESC, IndexOrderDescending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, asc_3, desc_1_2),
      ("n.prop1, n.prop2", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", DESC, IndexOrderDescending, false, false, map_1 ++ map_2_cached, map_empty, expr_2_cached, Seq.empty, Seq.empty),
      ("n.prop1, n.prop3", "n.prop1 ASC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, true, false, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, asc_1_2_3, Seq.empty),
      ("n.prop1, n.prop3", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", DESC, IndexOrderDescending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, asc_2_3, desc_1),
      ("n.prop1, n.prop3", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", DESC, IndexOrderDescending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, asc_3, desc_1_2),
      ("n.prop1, n.prop3", "n.prop1 DESC, n.prop2 DESC, n.prop3 DESC", DESC, IndexOrderDescending, false, false, map_1 ++ map_3_cached, map_empty, expr_3_cached, Seq.empty, Seq.empty),

      // Both index
      ("n.prop1", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", BOTH, IndexOrderAscending, false, true, map_1, map_2_3_cached, expr_2_3_cached, desc_3, asc_1_2),
      ("n.prop1", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", BOTH, IndexOrderAscending, false, true, map_1, map_2_3_cached, expr_2_3_cached, desc_2_asc_3, asc_1),
      ("n.prop1", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", BOTH, IndexOrderDescending, false, true, map_1, map_2_3_cached, expr_2_3_cached, asc_2_3, desc_1),
      ("n.prop1", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", BOTH, IndexOrderDescending, false, true, map_1, map_2_3_cached, expr_2_3_cached, asc_3, desc_1_2),

      ("n.prop1, n.prop2", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", BOTH, IndexOrderAscending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, desc_3, asc_1_2),
      ("n.prop1, n.prop2", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", BOTH, IndexOrderAscending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, desc_2_asc_3, asc_1),
      ("n.prop1, n.prop2", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", BOTH, IndexOrderDescending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, asc_2_3, desc_1),
      ("n.prop1, n.prop2", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", BOTH, IndexOrderDescending, false, true, map_1 ++ map_2_cached, map_3_cached, expr_2_3_cached, asc_3, desc_1_2),
      ("n.prop1, n.prop3", "n.prop1 ASC, n.prop2 ASC, n.prop3 DESC", BOTH, IndexOrderAscending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, desc_3, asc_1_2),
      ("n.prop1, n.prop3", "n.prop1 ASC, n.prop2 DESC, n.prop3 ASC", BOTH, IndexOrderAscending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, desc_2_asc_3, asc_1),
      ("n.prop1, n.prop3", "n.prop1 DESC, n.prop2 ASC, n.prop3 ASC", BOTH, IndexOrderDescending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, asc_2_3, desc_1),
      ("n.prop1, n.prop3", "n.prop1 DESC, n.prop2 DESC, n.prop3 ASC", BOTH, IndexOrderDescending, false, true, map_1 ++ map_3_cached, map_2_cached, expr_2_3_cached, asc_3, desc_1_2)
    ).foreach {
      case (returnString, orderByString, orderCapability, indexOrder, fullSort, partialSort, returnProjections, sortProjections, selectionExpression, sortItems, alreadySorted) =>
        // When
        val query =
          s"""
            |MATCH (n:Label)
            |WHERE n.prop1 >= 42 AND n.prop2 <= 3 AND n.prop3 > ''
            |RETURN $returnString
            |ORDER BY $orderByString
          """.stripMargin
        val plan = new given {
          indexOn("Label", "prop1", "prop2", "prop3").providesOrder(orderCapability)
        } getLogicalPlanFor query

        // Then
        val leafPlan = IndexSeek("n:Label(prop1 >= 42, prop2 <= 3, prop3 > '')", indexOrder = indexOrder)
        plan._2 should equal {
          if (fullSort)
            Sort(Projection(Projection(Selection(selectionExpression, leafPlan), returnProjections), sortProjections), sortItems)
          else if (partialSort)
            PartialSort(Projection(Projection(Selection(selectionExpression, leafPlan), returnProjections), sortProjections), alreadySorted, sortItems)
          else
            Projection(Selection(selectionExpression, leafPlan), returnProjections)
        }
    }
  }

  // Min and Max

  // Tests (ASC, min), (DESC, max), (BOTH, min), (BOTH, max) -> interesting and provided order are the same
  val ASCENDING_BOTH = TestOrder(IndexOrderAscending, "ASC", BOTH, Ascending)
  for ((TestOrder(plannedOrder, cypherToken, orderCapability, _), functionName) <- List((ASCENDING, "min"), (DESCENDING, "max"), (ASCENDING_BOTH, "min"), (DESCENDING_BOTH, "max"))) {

    test(s"$orderCapability-$functionName: should use provided index order with range") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop)"

      plan._2 should equal(
        Optional(
          LimitPlan(
            Projection(
              IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
              Map(s"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
            ),
            literalInt(1),
            DoNotIncludeTies
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order with ORDER BY") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) ORDER BY $functionName(n.prop) $cypherToken"

      plan._2 should equal(
        Optional(
          LimitPlan(
            Projection(
              IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
              Map(s"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
            ),
            literalInt(1),
            DoNotIncludeTies
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order followed by sort for ORDER BY with reverse order") {
      val (inverseOrder, inverseSortOrder) = cypherToken match {
        case "ASC" => ("DESC", Descending)
        case "DESC" => ("ASC", Ascending)
      }

      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) ORDER BY $functionName(n.prop) $inverseOrder"

      plan._2 should equal(
        Sort(
          Optional(
            LimitPlan(
              Projection(
                IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
                Map(s"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
              ),
              literalInt(1),
              DoNotIncludeTies
            )),
          Seq(inverseSortOrder(s"$functionName(n.prop)"))
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order with additional Limit") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop) LIMIT 2"

      plan._2 should equal(
        LimitPlan(
          Optional(
            LimitPlan(
              Projection(
                IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
                Map(s"$functionName(n.prop)" -> cachedNodeProp("n", "prop"))
              ),
              literalInt(1),
              DoNotIncludeTies
            )),
          literalInt(2),
          DoNotIncludeTies
        )
      )
    }

    test(s"$orderCapability-$functionName: should use provided index order for multiple QueryGraphs") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor
        s"""MATCH (n:Awesome)
           |WHERE n.prop > 0
           |WITH $functionName(n.prop) AS agg
           |RETURN agg
           |ORDER BY agg $cypherToken""".stripMargin

      plan._2 should equal(
        Optional(
          LimitPlan(
            Projection(
              IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
              Map("agg" -> cachedNodeProp("n", "prop"))
            ),
            literalInt(1),
            DoNotIncludeTies
          )
        )
      )
    }

    test(s"$orderCapability-$functionName: cannot use provided index order for multiple aggregations") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop), count(n.prop)"

      val expectedIndexOrder = if (orderCapability.asc) IndexOrderAscending else IndexOrderDescending

      plan._2 should equal(
        Aggregation(
          IndexSeek("n:Awesome(prop > 0)", indexOrder = expectedIndexOrder, getValue = GetValue),
          Map.empty,
          Map(s"$functionName(n.prop)" -> function(functionName, cachedNodeProp("n", "prop")),
            "count(n.prop)" -> count(cachedNodeProp("n", "prop")))
        )
      )
    }

    test(s"should plan aggregation with exists and index for $functionName when there is no $orderCapability") {
      val plan = new given {
        indexOn("Awesome", "prop").providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE exists(n.prop) RETURN $functionName(n.prop)"

      plan._2 should equal(
        Aggregation(
          NodeIndexScan(
            "n",
            LabelToken("Awesome", LabelId(0)),
            Seq(indexedProperty("prop", 0, GetValue)),
            Set.empty,
            IndexOrderNone),
          Map.empty,
          Map(s"$functionName(n.prop)" -> function(functionName, cachedNodeProp("n", "prop")))
        )
      )
    }
  }

  // Tests (ASC, max), (DESC, min) -> interesting and provided order differs
  for ((TestOrder(plannedOrder, _, orderCapability, _), functionName) <- List((ASCENDING, "max"), (DESCENDING, "min"))) {

    test(s"$orderCapability-$functionName: cannot use provided index order with range") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability).providesValues()
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 0 RETURN $functionName(n.prop)"

      plan._2 should equal(
        Aggregation(
          IndexSeek("n:Awesome(prop > 0)", indexOrder = plannedOrder, getValue = GetValue),
          Map.empty,
          Map(s"$functionName(n.prop)" -> function(functionName, cachedNodeProp("n", "prop")))
        )
      )
    }
  }
}
