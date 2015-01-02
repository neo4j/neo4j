/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.pipes.LazyLabel
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.Limit
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.Limit
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.{PlannerQuery, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.{InputPosition}

class OptionalMatchPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should build plans containing joins") {
    (new given {
      cardinality = mapCardinality {
        case _: AllNodesScan => 2000000
        case _: NodeByLabelScan => 20
        case _: Expand => 10
        case _: OuterHashJoin => 20
        case _: SingleRow => 1
        case _ => Double.MaxValue
      }
    } planFor "MATCH (a:X)-[r1]->(b) OPTIONAL MATCH (b)-[r2]->(c:Y) RETURN b").plan should equal(
      Projection(
        OuterHashJoin(Set("b"),
          Expand(NodeByLabelScan("a", LazyLabel("X"), Set.empty)(PlannerQuery.empty), "a", Direction.OUTGOING, Seq(), "b", "r1")(PlannerQuery.empty),
          Expand(NodeByLabelScan("c", LazyLabel("Y"), Set.empty)(PlannerQuery.empty), "c", Direction.INCOMING, Seq(), "b", "r2")(PlannerQuery.empty)
        )(PlannerQuery.empty),
        expressions = Map("b" -> ident("b"))
      )(PlannerQuery.empty)
    )
  }

  test("should build simple optional match plans") { // This should be built using plan rewriting
    planFor("OPTIONAL MATCH a RETURN a").plan should equal(
      Optional(AllNodesScan("a", Set.empty)(PlannerQuery.empty))(PlannerQuery.empty)
    )
  }

  test("should build simple optional expand") {
    planFor("MATCH n OPTIONAL MATCH n-[:NOT_EXIST]->x RETURN n").plan.endoRewrite(unnestOptional) match {
      case Projection(OptionalExpand(
        AllNodesScan(IdName("n"), _),
        IdName("n"),
        Direction.OUTGOING,
        _,
        IdName("x"),
        _,
        _,
        _
      ), _) => ()
    }
  }

  test("should build optional ProjectEndpoints") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2").plan match {
      case Projection(
            Apply(
              Limit(
                Expand(
                  AllNodesScan(IdName("b1"), _), _, _, _, _, _, _), _),
              Optional(
                Selection(
                  predicates,
                  ProjectEndpoints(
                    Argument(_), IdName("r"), IdName("b2"), IdName("a1$$$_"), true, SimplePatternLength
                  )
                  )
                )
      ), _) =>
        val predicate: Expression = Equals(Identifier("a1")_, Identifier("a1$$$_")_)_
        predicates should equal(Seq(predicate))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2").plan match {
      case Projection(Apply(
        Limit(Expand(AllNodesScan(IdName("b1"), _), _, _, _, _, _, _), _),
        Optional(
          Selection(
            predicates1,
            ProjectEndpoints(
              Selection(predicates2, AllNodesScan(IdName("a2"), args)),
              IdName("r"), IdName("b2"), IdName("a2$$$_"), true, SimplePatternLength
            )
          )
        )
      ), _) =>
        args should equal(Set(IdName("r"), IdName("a1")))

        val predicate1: Expression = Equals(Identifier("a2")_, Identifier("a2$$$_")_)_
        predicates1 should equal(Seq(predicate1))

        val predicate2: Expression = Equals(Identifier("a1")_, Identifier("a2")_)_
        predicates2 should equal(Seq(predicate2))
    }
  }

  test("should build optional ProjectEndpoints with extra predicates 2") {
    planFor("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2").plan  match {
      case Projection(Apply(
        Limit(Expand(AllNodesScan(IdName("b1"), _), _, _, _, _, _, _), _),
        Optional(
          Selection(
            predicates,
            ProjectEndpoints(
              AllNodesScan(IdName("a2"), args),
              IdName("r"), IdName("a2$$$_"), IdName("b2"), true, SimplePatternLength
            )
          )
        )
      ), _) =>
        val predicate: Expression = Equals(Identifier("a2")_, Identifier("a2$$$_")_)_
        predicates should equal(Seq(predicate))
    }
  }

  test("should solve multiple optional matches") {
    val plan = planFor("MATCH a OPTIONAL MATCH (a)-[:R1]->(x1) OPTIONAL MATCH (a)-[:R2]->(x2) RETURN a, x1, x2").plan.endoRewrite(unnestOptional)
    plan should equal(
      Projection(
        OptionalExpand(
          OptionalExpand(
            AllNodesScan(IdName("a"), Set.empty)(PlannerQuery.empty),
            IdName("a"), Direction.OUTGOING, List(RelTypeName("R1") _), IdName("x1"), IdName("  UNNAMED27"), ExpandAll, Seq.empty)(PlannerQuery.empty),
          IdName("a"), Direction.OUTGOING, List(RelTypeName("R2") _), IdName("x2"), IdName("  UNNAMED58"), ExpandAll, Seq.empty)(PlannerQuery.empty),
        Map("a" -> ident("a"), "x1" -> ident("x1"), "x2" -> ident("x2"))
      )(PlannerQuery.empty)
    )
  }

  test("should solve optional matches with arguments and predicates") {
    val plan = planFor("""MATCH (n)
                         |OPTIONAL MATCH n-[r]-(m)
                         |WHERE m.prop = 42
                         |RETURN m""".stripMargin).plan.endoRewrite(unnestOptional)
    val s = PlannerQuery.empty
    val allNodesN:LogicalPlan = AllNodesScan(IdName("n"),Set())(s)
    val allNodesM:LogicalPlan = AllNodesScan(IdName("m"),Set())(s)
    val predicate: Expression = In(Property(ident("m"), PropertyKeyName("prop") _) _, Collection(List(SignedDecimalIntegerLiteral("42") _)) _) _
    val expand = Expand(Selection(Vector(predicate), allNodesM)(s), IdName("m"), Direction.BOTH, List(), IdName("n"), IdName("r"), ExpandAll)(s)
    plan should equal(
      Projection(
        OuterHashJoin(Set(IdName("n")), allNodesN, expand)(s),
        Map("m" -> ident("m"))
      )(s)
    )
  }
}
