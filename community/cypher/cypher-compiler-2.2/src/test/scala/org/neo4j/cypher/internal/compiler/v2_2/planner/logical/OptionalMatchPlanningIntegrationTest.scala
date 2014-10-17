/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.Limit
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.rewriter.unnestOptional
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner.{PlannerQuery, LogicalPlanningTestSupport2}
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.ast

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
          Expand(NodeByLabelScan("a", Left("X"), Set.empty)(PlannerQuery.empty), "a", Direction.OUTGOING, Direction.OUTGOING, Seq(), "b", "r1", SimplePatternLength)(PlannerQuery.empty),
          Expand(NodeByLabelScan("c", Left("Y"), Set.empty)(PlannerQuery.empty), "c", Direction.INCOMING, Direction.OUTGOING, Seq(), "b", "r2", SimplePatternLength)(PlannerQuery.empty)
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
        SimplePatternLength,
        _
      ), _) => ()
    }
  }

  test("should build optional ProjectEndpoints") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a1)<-[r]-(b2) RETURN a1, r, b2").plan match {
      case Projection(Apply(
        Limit(Expand(AllNodesScan(IdName("b1"), _), _, _, _, _, _, _, _, _), _),
        Apply(SingleRow(_), Optional(
          Selection(
            predicates,
            ProjectEndpoints(
              SingleRow(_), IdName("r"), IdName("b2"), IdName("a1$$$_"), true, SimplePatternLength
            )
          )
        ))
      ), _) => {
        val predicate: ast.Expression = ast.Equals(ast.Identifier("a1")_, ast.Identifier("a1$$$_")_)_
        predicates should equal(Seq(predicate))
      }
    }
  }

  test("should build optional ProjectEndpoints with extra predicates") {
    planFor("MATCH (a1)-[r]->(b1) WITH r, a1 LIMIT 1 OPTIONAL MATCH (a2)<-[r]-(b2) WHERE a1 = a2 RETURN a1, r, b2").plan match {
      case Projection(Apply(
        Limit(Expand(AllNodesScan(IdName("b1"), _), _, _, _, _, _, _, _,_), _),
        Apply(SingleRow(_), Optional(
          Selection(
            predicates1,
            ProjectEndpoints(
              Selection(predicates2, AllNodesScan(IdName("a2"), args)),
              IdName("r"), IdName("b2"), IdName("a2$$$_"), true, SimplePatternLength
            )
          )
        ))
      ), _) => {
        args should equal(Set(IdName("r"), IdName("a1")))

        val predicate1: ast.Expression = ast.Equals(ast.Identifier("a2")_, ast.Identifier("a2$$$_")_)_
        predicates1 should equal(Seq(predicate1))

        val predicate2: ast.Expression = ast.Equals(ast.Identifier("a1")_, ast.Identifier("a2")_)_
        predicates2 should equal(Seq(predicate2))
      }
    }
  }

  test("should build optional ProjectEndpoints with extra predicates 2") {
    planFor("MATCH (a1)-[r]->(b1) WITH r LIMIT 1 OPTIONAL MATCH (a2)-[r]->(b2) RETURN a2, r, b2").plan  match {
      case Projection(Apply(
        Limit(Expand(AllNodesScan(IdName("b1"), _), _, _, _, _, _, _, _, _), _),
        Apply(SingleRow(_), Optional(
          Selection(
            predicates,
            ProjectEndpoints(
              AllNodesScan(IdName("a2"), args),
              IdName("r"), IdName("a2$$$_"), IdName("b2"), true, SimplePatternLength
            )
          )
        ))
      ), _) => {
        val predicate: ast.Expression = ast.Equals(ast.Identifier("a2")_, ast.Identifier("a2$$$_")_)_
        predicates should equal(Seq(predicate))
      }
    }
  }

  test("should solve multiple optional matches") {
    val plan = planFor("MATCH a OPTIONAL MATCH (a)-[:R1]->(x1) OPTIONAL MATCH (a)-[:R2]->(x2) RETURN a, x1, x2").plan.endoRewrite(unnestOptional)
    plan should equal(
      Projection(
        OptionalExpand(
          OptionalExpand(
            AllNodesScan(IdName("a"), Set.empty)(PlannerQuery.empty),
            IdName("a"), Direction.OUTGOING, List(ast.RelTypeName("R1") _), IdName("x1"), IdName("  UNNAMED27"), SimplePatternLength, Seq.empty)(PlannerQuery.empty),
          IdName("a"), Direction.OUTGOING, List(ast.RelTypeName("R2") _), IdName("x2"), IdName("  UNNAMED58"), SimplePatternLength, Seq.empty)(PlannerQuery.empty),
        Map("a" -> ident("a"), "x1" -> ident("x1"), "x2" -> ident("x2"))
      )(PlannerQuery.empty)
    )
  }

  test("should solve optional matches with predicates ") {
    val plan = planFor("MATCH (n) OPTIONAL MATCH n-[r]-(m) WHERE m.prop = 42 RETURN m").plan.endoRewrite(unnestOptional)
    val predicates: Seq[ast.Expression] = List(ast.In(ast.Property(ast.Identifier("m")_, ast.PropertyKeyName("prop")_)_,ast.Collection(List(ast.SignedDecimalIntegerLiteral("42")_))_)_)
    plan should equal(
      Projection(
        OptionalExpand(
            AllNodesScan(IdName("n"), Set.empty)(PlannerQuery.empty),
          IdName("n"), Direction.BOTH, List.empty, IdName("m"), IdName("r"), SimplePatternLength, predicates)(PlannerQuery.empty),
        Map("m" -> ident("m"))
      )(PlannerQuery.empty)
    )
  }
}
