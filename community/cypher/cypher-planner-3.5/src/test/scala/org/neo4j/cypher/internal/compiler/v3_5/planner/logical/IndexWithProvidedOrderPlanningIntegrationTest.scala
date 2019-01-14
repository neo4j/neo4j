/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical

import org.neo4j.cypher.internal.compiler.v3_5.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.ir.v3_5.RegularPlannerQuery
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexOrderCapability.{ASC, DESC, BOTH}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.logical.plans.{Skip => SkipPlan}
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.util._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class IndexWithProvidedOrderPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 with AstConstructionTestSupport {

  case class TestOrder(indexOrder: IndexOrder, cypherToken: String, indexOrderCapability: IndexOrderCapability, sortOrder: String => ColumnOrder)
  val ASCENDING = TestOrder(IndexOrderAscending, "ASC", ASC, Ascending)
  val DESCENDING = TestOrder(IndexOrderDescending, "DESC", DESC, Descending)
  val DESCENDING_BOTH = TestOrder(IndexOrderDescending, "DESC", BOTH, Descending)

  for (TestOrder(plannedOrder, cypherToken, orderCapability, sortOrder) <- List(ASCENDING, DESCENDING, DESCENDING_BOTH)) {

    test(s"$cypherToken-$orderCapability: Order by index backed property should plan with provided order") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (n:Awesome) WHERE n.prop > 'foo' RETURN n.prop ORDER BY n.prop $cypherToken"

    plan._2 should equal(
      Projection(
        IndexSeek("n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
        Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)))
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
          Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos))),
        Seq(sortOrder("n.prop")))
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
            Map("nnn" -> Variable("n")(pos))),
          "nnn", SemanticDirection.INCOMING, Seq.empty, "m", "r"),
        Map("nnn.prop" -> Property(Variable("nnn")(pos), PropertyKeyName("prop")(pos))(pos)))
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
        Map("m" -> Variable("n")(pos)))
    )
  }

    test(s"$cypherToken-$orderCapability: Cannot order by index when ordering is on same property name, but different node") {
      val plan = new given {
        indexOn("Awesome", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (m:Awesome), (n:Awesome) WHERE n.prop > 'foo' RETURN m.prop ORDER BY m.prop $cypherToken"

      plan._2 should equal(
        Sort(
        Projection(
          CartesianProduct(
            IndexSeek(
              "n:Awesome(prop > 'foo')", indexOrder = plannedOrder),
            NodeByLabelScan("m", LabelName("Awesome")(pos), Set.empty)),
          Map("m.prop" -> Property(Variable("m")(pos), PropertyKeyName("prop")(pos))(pos))),
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
        Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)))
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
          Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)))
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
          Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)))
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
        Map("n.prop" -> Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos)))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an Apply") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        indexOn("B", "prop").providesOrder(orderCapability)
      } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop > 'foo' AND a.prop = b.prop RETURN a.prop ORDER BY a.prop $cypherToken"

    plan._2 should equal(
      Projection(
        Apply(
          IndexSeek(
            "a:A(prop > 'foo')", indexOrder = plannedOrder),
            IndexSeek("b:B(prop = ???)",
                indexOrder = plannedOrder,
            paramExpr = Some(prop("a", "prop")),
            labelId = 1,
                argumentIds = Set("a"))
        ),
        Map("a.prop" -> Property(Variable("a")(pos), PropertyKeyName("prop")(pos))(pos)))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed properties in a plan with an Apply needs Sort if RHS order required") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        indexOn("B", "prop").providesOrder(orderCapability)
        // This query is very fragile in the sense that the slightest modification will result in a stupid plan
      } getLogicalPlanFor s"MATCH (a:A), (b:B) WHERE a.prop STARTS WITH 'foo' AND b.prop > a.prop RETURN a.prop, b.prop ORDER BY a.prop $cypherToken, b.prop $cypherToken"

    plan._2 should equal(
      Sort(
        Projection(
          Apply(
            IndexSeek("a:A(prop STARTS WITH 'foo')", indexOrder = plannedOrder),
            IndexSeek("b:B(prop > ???)",
              indexOrder = plannedOrder,
              paramExpr = Some(prop("a", "prop")),
              labelId = 1,
              argumentIds = Set("a"))
          ),
          Map("a.prop" -> Property(Variable("a")(pos), PropertyKeyName("prop")(pos))(pos), "b.prop" -> Property(Variable("b")(pos), PropertyKeyName("prop")(pos))(pos))),
        Seq(sortOrder("a.prop"), sortOrder("b.prop")))
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
        Map("theProp" -> Property(Variable("a")(pos), PropertyKeyName("prop")(pos))(pos), "x" -> SignedDecimalIntegerLiteral("1")(pos)))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with an aggregation and an expand") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN a.prop, count(b) ORDER BY a.prop $cypherToken"

    plan._2 should equal(
      Aggregation(
        Expand(
          IndexSeek(
            "a:A(prop > 'foo')", indexOrder = plannedOrder),
          "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
        Map("a.prop" -> prop("a", "prop")), Map("count(b)" -> FunctionInvocation(Namespace(List())(pos), FunctionName("count")(pos), distinct = false, Vector(varFor("b")))(pos)))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a distinct") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at a
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 2000.0
        }
      } getLogicalPlanFor s"MATCH (a:A)-[r]->(b) WHERE a.prop > 'foo' RETURN DISTINCT a.prop ORDER BY a.prop $cypherToken"

    plan._2 should equal(
      Distinct(
        Expand(
          IndexSeek(
            "a:A(prop > 'foo')", indexOrder = plannedOrder),
          "a", SemanticDirection.OUTGOING, Seq.empty, "b", "r"),
        Map("a.prop" -> prop("a", "prop")))
      )
    }

    test(s"$cypherToken-$orderCapability: Order by index backed property in a plan with a outer join") {
      val plan = new given {
        indexOn("A", "prop").providesOrder(orderCapability)
        cardinality = mapCardinality {
          // Force the planner to start at b
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("a", "b") => 100.0
          case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternNodes == Set("b") => 20.0
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
            SignedDecimalIntegerLiteral("0")(pos)),
          AllNodesScan("b", Set("a"))),
        Map("a.prop" -> prop("a", "prop")))
      )
    }
  }
}
