/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_1.planner.logical

import org.neo4j.cypher.internal.compiler.v3_1.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.cypher.internal.compiler.v3_1.planner.logical.plans.{Limit, _}
import org.neo4j.cypher.internal.frontend.v3_1.Foldable._
import org.neo4j.cypher.internal.frontend.v3_1.SemanticDirection
import org.neo4j.cypher.internal.frontend.v3_1.ast._
import org.neo4j.cypher.internal.frontend.v3_1.test_helpers.CypherFunSuite
import org.neo4j.kernel.impl.util.dbstructure.DbStructureLargeOptionalMatchStructure

class OptionalMatchPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing joins") {
    (new given {
      cost = {
        case (_: AllNodesScan, _) => 2000000.0
        case (_: NodeByLabelScan, _) => 20.0
        case (_: Expand, _) => 10.0
        case (_: OuterHashJoin, _) => 20.0
        case (_: SingleRow, _) => 1.0
        case _ => Double.MaxValue
      }
    } planFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b").plan should equal(
        OuterHashJoin(Set("b"),
          Expand(NodeByLabelScan("a", lblName("X"), Set.empty)(solved), "a", SemanticDirection.OUTGOING, Seq(), "b", "r1")(solved),
          Expand(NodeByLabelScan("c", lblName("Y"), Set.empty)(solved), "c", SemanticDirection.INCOMING, Seq(), "b", "r2")(solved)
        )(solved)
    )
  }

  test("should build simple optional match plans") { // This should be built using plan rewriting
    planFor("OPTIONAL MATCH (a) RETURN a").plan should equal(
      Optional(AllNodesScan("a", Set.empty)(solved))(solved))
  }

  test("should build simple optional expand") {
    planFor("MATCH (n) OPTIONAL MATCH (n)-[:NOT_EXIST]->(x) RETURN n").plan.endoRewrite(unnestOptional) match {
      case OptionalExpand(
      AllNodesScan(IdName("n"), _),
      IdName("n"),
      SemanticDirection.OUTGOING,
      _,
      IdName("x"),
      _,
      _,
      _
      ) => ()
    }
  }

  test("should build optional ProjectEndpoints") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2").plan match {
      case
        Apply(
        Limit(
        Expand(
        AllNodesScan(IdName("b1"), _), _, _, _, _, _, _), _, _),
        Optional(
        ProjectEndpoints(
        Argument(args), IdName("r"), IdName("b2"), false, IdName("a1"), true, None, true, SimplePatternLength
        ), _
        )
        ) =>
        args should equal(Set(IdName("r"), IdName("a1")))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2").plan match {
      case Apply(
      Limit(Expand(AllNodesScan(IdName("b1"), _), _, _, _, _, _, _), _, _),
      Optional(
      Selection(
      predicates,
      ProjectEndpoints(
      Argument(args),
      IdName("r"), IdName("b2"), false, IdName("a2"), false, None, true, SimplePatternLength
      )
      ), _
      )
      ) =>
        args should equal(Set(IdName("r"), IdName("a1")))
        val predicate: Expression = Equals(Variable("a1")_, Variable("a2")_)_
        predicates should equal(Seq(predicate))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates 2") {
    planFor("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2").plan  match {
      case Apply(
      Limit(Expand(AllNodesScan(IdName("b1"), _), _, _, _, _, _, _), _, _),
      Optional(
      ProjectEndpoints(
      Argument(args),
      IdName("r"), IdName("a2"), false, IdName("b2"), false, None, true, SimplePatternLength
      ), _
      )
      ) =>
        args should equal(Set(IdName("r")))
    }
  }

  test("should solve multiple optional matches") {
    val plan = planFor("MATCH (a) OPTIONAL MATCH (a)-[:R1]->(x1) OPTIONAL MATCH (a)-[:R2]->(x2) RETURN a, x1, x2").plan.endoRewrite(unnestOptional)
    plan should equal(
      OptionalExpand(
        OptionalExpand(
          AllNodesScan(IdName("a"), Set.empty)(solved),
          IdName("a"), SemanticDirection.OUTGOING, List(RelTypeName("R1") _), IdName("x1"), IdName("  UNNAMED29"), ExpandAll, Seq.empty)(solved),
        IdName("a"), SemanticDirection.OUTGOING, List(RelTypeName("R2") _), IdName("x2"), IdName("  UNNAMED60"), ExpandAll, Seq.empty)(solved)
    )
  }

  test("should solve optional matches with arguments and predicates") {
    val plan = planFor("""MATCH (n)
                         |OPTIONAL MATCH (n)-[r]-(m)
                         |WHERE m.prop = 42
                         |RETURN m""".stripMargin).plan.endoRewrite(unnestOptional)
    val s = solved
    val allNodesN:LogicalPlan = AllNodesScan(IdName("n"),Set())(s)
    val predicate: Expression = In(Property(varFor("m"), PropertyKeyName("prop") _) _, ListLiteral(List(SignedDecimalIntegerLiteral("42") _)) _) _
    plan should equal(
      OptionalExpand(allNodesN, IdName("n"), SemanticDirection.BOTH, Seq.empty, IdName("m"), IdName("r"), ExpandAll,
                     Vector(predicate))(s)
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

   lom.planFor(query).treeExists {
      case plan:LogicalPlan =>
        plan.solved.estimatedCardinality match {
          case Cardinality(amount) =>
            withClue("We should not get a NaN cardinality.") {
              amount.isNaN should not be true
            }
        }
        false // this is a "trick" to use treeExists to iterate over the whole tree
    }
  }
}
