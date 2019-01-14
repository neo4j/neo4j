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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.cypher.internal.ir.v3_4.SimplePatternLength
import org.neo4j.cypher.internal.planner.v3_4.spi.DelegatingGraphStatistics
import org.neo4j.cypher.internal.util.v3_4.{Cardinality, LabelId, RelTypeId}
import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans.{Limit, _}
import org.neo4j.kernel.impl.util.dbstructure.DbStructureLargeOptionalMatchStructure

class OptionalMatchPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing left outer joins") {
    (new given {
      cost = {
        case (_: AllNodesScan, _, _) => 2000000.0
        case (_: NodeByLabelScan, _, _) => 20.0
        case (p: Expand, _, _) if p.findByAllClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _) => 10.0
        case (_: LeftOuterHashJoin, _, _) => 20.0
        case (_: Argument, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      LeftOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")
      )
    )
  }

  test("should build plans containing right outer joins") {
    (new given {
      cost = {
        case (_: AllNodesScan, _, _) => 2000000.0
        case (_: NodeByLabelScan, _, _) => 20.0
        case (p: Expand, _, _) if p.findByAllClass[CartesianProduct].nonEmpty => Double.MaxValue
        case (_: Expand, _, _) => 10.0
        case (_: RightOuterHashJoin, _, _) => 20.0
        case (_: Argument, _, _) => 1.0
        case _ => Double.MaxValue
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      RightOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1")
      )
    )
  }

  test("should choose left outer join if lhs has small cardinality") {
    (new given {
      labelCardinality = Map("X" -> 1.0, "Y" -> 10.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = {
          // TODO proper lookup from semantic table somehow
          // X = 0, Y = 1
          if (fromLabel.exists(_.id == 0) && relTypeId.isEmpty && toLabel.isEmpty) {
            // low from a to b
            100.0
          } else if (fromLabel.isEmpty && relTypeId.isEmpty && toLabel.exists(_.id == 1)) {
            // high from b to c
            1000000000.0
          } else {
            super.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel)
          }
        }
      }
      cost = {
        case (_: Apply, _, _) => Double.MaxValue
        case x => parent.costModel()(x)
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      LeftOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")
      )
    )
  }

  test("should choose right outer join if rhs has small cardinality") {
    (new given {
      labelCardinality = Map("X" -> 10.0, "Y" -> 1.0)
      statistics = new DelegatingGraphStatistics(parent.graphStatistics) {
        override def cardinalityByLabelsAndRelationshipType(fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId]): Cardinality = {
          // TODO proper lookup from semantic table somehow
          // X = 0, Y = 1
          if (fromLabel.exists(_.id == 0) && relTypeId.isEmpty && toLabel.isEmpty) {
            // high from a to b
            1000000000.0
          } else if ( fromLabel.isEmpty && relTypeId.isEmpty && toLabel.exists(_.id == 1)) {
            // low from b to c
            100.0
          } else {
            super.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel)
          }
        }
      }
      cost = {
        case (_: Apply, _, _) => Double.MaxValue
        case x => parent.costModel()(x)
      }
    } getLogicalPlanFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b")._2 should equal(
      RightOuterHashJoin(Set("b"),
        Expand(NodeByLabelScan("c", lblName("Y"), Set.empty), "c", SemanticDirection.INCOMING, Seq(), "b", "r2"),
        Expand(NodeByLabelScan("a", lblName("X"), Set.empty), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1")
      )
    )
  }

  test("should build simple optional match plans") { // This should be built using plan rewriting
    planFor("OPTIONAL MATCH (a) RETURN a")._2 should equal(
      Optional(AllNodesScan("a", Set.empty)))
  }

  test("should build simple optional expand") {
    planFor("MATCH (n) OPTIONAL MATCH (n)-[:NOT_EXIST]->(x) RETURN n")._2.endoRewrite(unnestOptional) match {
      case OptionalExpand(
      AllNodesScan("n", _),
      "n",
      SemanticDirection.OUTGOING,
      _,
      "x",
      _,
      _,
      _
      ) => ()
    }
  }

  test("should build optional ProjectEndpoints") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2")._2 match {
      case
        Apply(
        Limit(
        Expand(
        AllNodesScan("b1", _), _, _, _, _, _, _), _, _),
        Optional(
        ProjectEndpoints(
        Argument(args), "r", "b2", false, "a1", true, None, true, SimplePatternLength
        ), _
        )
        ) =>
        args should equal(Set("r", "a1"))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2")._2 match {
      case Apply(
      Limit(Expand(AllNodesScan("b1", _), _, _, _, _, _, _), _, _),
      Optional(
      Selection(
      predicates,
      ProjectEndpoints(
      Argument(args),
      "r", "b2", false, "a2", false, None, true, SimplePatternLength
      )
      ), _
      )
      ) =>
        args should equal(Set("r", "a1"))
        val predicate: Expression = Equals(Variable("a1") _, Variable("a2") _) _
        predicates should equal(Seq(predicate))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates 2") {
    planFor("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2")._2 match {
      case Apply(
      Limit(Expand(AllNodesScan("b1", _), _, _, _, _, _, _), _, _),
      Optional(
      ProjectEndpoints(
      Argument(args),
      "r", "a2", false, "b2", false, None, true, SimplePatternLength
      ), _
      )
      ) =>
        args should equal(Set("r"))
    }
  }

  test("should solve multiple optional matches") {
    val plan = planFor("MATCH (a) OPTIONAL MATCH (a)-[:R1]->(x1) OPTIONAL MATCH (a)-[:R2]->(x2) RETURN a, x1, x2")._2.endoRewrite(unnestOptional)
    plan should equal(
      OptionalExpand(
        OptionalExpand(
          AllNodesScan("a", Set.empty),
          "a", SemanticDirection.OUTGOING, List(RelTypeName("R1") _), "x1", "  UNNAMED29", ExpandAll, Seq.empty),
        "a", SemanticDirection.OUTGOING, List(RelTypeName("R2") _), "x2", "  UNNAMED60", ExpandAll, Seq.empty)
    )
  }

  test("should solve optional matches with arguments and predicates") {
    val plan = new given {
      cost = {
        case (_: Expand, _, _) => 1000.0
      }
    }.getLogicalPlanFor(
      """MATCH (n:X)
        |OPTIONAL MATCH (n)-[r]-(m:Y)
        |WHERE m.prop = 42
        |RETURN m""".stripMargin)._2.endoRewrite(unnestOptional)
    val allNodesN: LogicalPlan = NodeByLabelScan("n", LabelName("X") _, Set.empty)
    val propEquality: Expression =
      In(Property(varFor("m"), PropertyKeyName("prop") _) _, ListLiteral(List(SignedDecimalIntegerLiteral("42") _)) _) _

    val labelCheck: Expression =
      HasLabels(varFor("m"), List(LabelName("Y") _)) _

    plan should equal(
      OptionalExpand(allNodesN, "n", SemanticDirection.BOTH, Seq.empty, "m", "r", ExpandAll,
        Seq(propEquality, labelCheck))
    )
  }

  test(
    "should plan for large number of optional matches without numerical overflow in estimatedRows") {

    val lom: LogicalPlanningEnvironment[_] = new fromDbStructure(DbStructureLargeOptionalMatchStructure.INSTANCE)
    val query =
      """
        |MATCH (me:Label1)-[rmeState:REL1]->(meState:Label2 {deleted: 0})
        |USING INDEX meState:Label2(id)
        |WHERE meState.id IN [63241]
        |WITH *
        |OPTIONAL MATCH(n1:Label3 {deleted: 0})<-[:REL1]-(:Label4)<-[:REL2]-(meState)
        |OPTIONAL MATCH(n2:Label5 {deleted: 0})<-[:REL1]-(:Label6)<-[:REL2]-(meState)
        |OPTIONAL MATCH(n3:Label7 {deleted: 0})<-[:REL1]-(:Label8)<-[:REL2]-(meState)
        |OPTIONAL MATCH(n4:Label9 {deleted: 0})<-[:REL1]-(:Label10) <-[:REL2]-(meState)
        |OPTIONAL MATCH p1 = (:Label2 {deleted: 0})<-[:REL1|:REL3*]-(meState)
        |OPTIONAL MATCH(:Label11 {deleted: 0})<-[r1:REL1]-(:Label12) <-[:REL5]-(meState)
        |OPTIONAL MATCH(:Label13 {deleted: 0})<-[r2:REL1]-(:Label14) <-[:REL6]-(meState)
        |OPTIONAL MATCH(:Label15 {deleted: 0})<-[r3:REL1]-(:Label16) <-[:REL7]-(meState)
        |OPTIONAL MATCH(:Label17 {deleted: 0})<-[r4:REL1]-(:Label18)<-[:REL8]-(meState)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r5:REL1]-(:Label20) <-[:REL2]-(n1)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r6:REL1]-(:Label20)<-[:REL2]-(n1)
        |
        |OPTIONAL MATCH(n5:Label21 {deleted: 0})<-[:REL1]-(:Label22)<-[:REL2]-(n2)
        |
        |OPTIONAL MATCH(n6:Label3 {deleted: 0})<-[:REL1]-(:Label4)<-[:REL2]-(n5)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r7:REL1]-(:Label20)<-[:REL2]-(n5)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r8:REL1]-(:Label20)<-[:REL2]-(n6)
        |
        |OPTIONAL MATCH(n7:Label23 {deleted: 0})<-[:REL1]-(:Label24)<-[:REL2]-(n3)
        |
        |OPTIONAL MATCH(n8:Label3 {deleted: 0})<-[:REL1]-(:Label4)<-[:REL2]-(n7)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r9:REL1]-(:Label20)<-[:REL2]-(n7)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r10:REL1]-(:Label20)<-[:REL2]-(n8)
        |
        |OPTIONAL MATCH(n9:Label25 {deleted: 0})<-[:REL1]-(:Label26)<-[:REL2]-(n4)
        |
        |OPTIONAL MATCH(n10:Label3 {deleted: 0})<-[:REL1]-(:Label4) <-[:REL2]-(n9)
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r11:REL1]-(:Label20) <-[:REL2]-(n9)
        |
        |OPTIONAL MATCH(:Label19 {deleted: 0})<-[r12:REL1]-(:Label20)<-[:REL2]-(n10)
        |OPTIONAL MATCH (me)-[:REL4]->(:Label2:Label27)
        |RETURN *
      """.stripMargin

    val (_, plan, _, _, cardinalities) = lom.getLogicalPlanFor(query)
    plan.treeExists {
      case plan: LogicalPlan =>
        cardinalities.get(plan.id) match {
          case Cardinality(amount) =>
            withClue("We should not get a NaN cardinality.") {
              amount.isNaN should not be true
            }
        }
        false // this is a "trick" to use treeExists to iterate over the whole tree
    }
  }
}
