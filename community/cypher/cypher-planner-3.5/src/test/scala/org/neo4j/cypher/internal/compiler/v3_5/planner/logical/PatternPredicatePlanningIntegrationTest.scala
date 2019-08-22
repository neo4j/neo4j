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

import org.neo4j.cypher.internal.compiler.v3_5.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.ir.v3_5.{QueryGraph, RegularPlannerQuery}
import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans.{NestedPlanExpression, _}
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class PatternPredicatePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should consider variables introduced by outer list comprehensions when planning pattern predicates") {
    val plan = (new given {
      cardinality = mapCardinality {
        // expand
        case RegularPlannerQuery(queryGraph, _, _, _) if queryGraph.patternRelationships.size == 1 => 10
        // argument
        case RegularPlannerQuery(queryGraph, _, _, _) if containsArgumentOnly(queryGraph) => 1
        case _ => 4000000
      }
    } getLogicalPlanFor """MATCH (a:Person)-[:KNOWS]->(b:Person) WITH a, collect(b) AS friends RETURN a, [f IN friends WHERE (f)-[:WORKS_AT]->(:ComedyClub)] AS clowns""")._2

    plan match {
      case Projection(_, expressions) =>
        expressions("clowns") match {
          case ListComprehension(ExtractScope(_, Some(NestedPlanExpression(nestedPlan, _)), _), _, _) =>
            nestedPlan should equal(
              Selection(
                Ands(Set(HasLabels(varFor("  NODE116"), Seq(LabelName("ComedyClub")_))_))_,
                Expand(
                  Argument(Set("f")),
                  "f", SemanticDirection.OUTGOING, Seq(RelTypeName("WORKS_AT")_), "  NODE116", "  REL102", ExpandAll
                )
              )
            )
        }
    }
  }

  test("should build plans with getDegree for a single pattern predicate") {
    planFor("MATCH (a) WHERE (a)-[:X]->() RETURN a")._2 should equal(
      Selection(Ands(Set(GreaterThan(GetDegree(Variable("a") _,Some(RelTypeName("X") _), OUTGOING)_,SignedDecimalIntegerLiteral("0")_)_))_,
        AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a single negated pattern predicate") {
    planFor("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(Ands(Set(LessThanOrEqual(GetDegree(Variable("a") _,Some(RelTypeName("X") _), OUTGOING)_,SignedDecimalIntegerLiteral("0")_)_))_,
                AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two pattern predicates") {
    planFor("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a")._2 should equal(
      Selection(Ands(Set(
        GreaterThan(
          GetDegree(Variable("a")_, Some(RelTypeName("Y")_), OUTGOING)_,SignedDecimalIntegerLiteral("0")_)_,
        GreaterThan(
          GetDegree(Variable("a")_, Some(RelTypeName("X")_), OUTGOING)_, SignedDecimalIntegerLiteral("0")_)_))_,
                AllNodesScan("a", Set.empty)))
  }

  test("should build plans containing getDegree for a pattern predicate and an expression") {
    planFor("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a")._2 should equal(
      Selection(Ands(Set(Ors(Set(
        GreaterThan(
          GetDegree(Variable("a")_, Some(RelTypeName("X")_),OUTGOING)_,
          SignedDecimalIntegerLiteral("0")_)_,
        GreaterThan(Property(Variable("a") _, PropertyKeyName("prop") _) _, SignedDecimalIntegerLiteral("4") _) _))_))_,
                AllNodesScan("a", Set.empty)))
  }

  test("should build plans containing getDegree for a pattern predicate and multiple expressions") {
    planFor("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a")._2 should equal(
      Selection(Ands(Set(Ors(Set(
        GreaterThan(
          GetDegree(Variable("a")_, Some(RelTypeName("X")_),OUTGOING)_,
          SignedDecimalIntegerLiteral("0")_)_,
        GreaterThan(Property(Variable("a") _, PropertyKeyName("prop") _) _, SignedDecimalIntegerLiteral("4") _) _,
        In(Property(Variable("a") _, PropertyKeyName("prop2") _) _, ListLiteral(Seq(SignedDecimalIntegerLiteral("9") _)) _) _))_))_,
                AllNodesScan("a", Set.empty)))
  }

  test("should build plans containing getDegree for a single negated pattern predicate and an expression") {
    planFor("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(Ands(Set(Ors(Set(
        LessThanOrEqual(
          GetDegree(Variable("a")_, Some(RelTypeName("X")_),OUTGOING)_,
          SignedDecimalIntegerLiteral("0")_)_,
        In(Property(Variable("a") _, PropertyKeyName("prop") _) _, ListLiteral(Seq(SignedDecimalIntegerLiteral("9") _)) _) _))_))_,
                AllNodesScan("a", Set.empty)))
  }

  test("should build plans containing getDegree for two pattern predicates and expressions") {
    planFor("MATCH (a) WHERE a.prop = 9 OR (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(Ands(Set(Ors(Set(
        GreaterThan(
          GetDegree(Variable("a")_, Some(RelTypeName("Y")_), OUTGOING)_,
          SignedDecimalIntegerLiteral("0")_)_,
        LessThanOrEqual(
          GetDegree(Variable("a")_, Some(RelTypeName("X")_), OUTGOING)_,
          SignedDecimalIntegerLiteral("0")_)_,
        In(Property(Variable("a")_, PropertyKeyName("prop")_)_, ListLiteral(Seq(SignedDecimalIntegerLiteral("9")_))_)_))_))_,
                AllNodesScan("a", Set.empty)))
  }

  test("should build plans containing getDegree for two pattern predicates with one negation") {
    planFor("MATCH (a) WHERE (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(Ands(Set(Ors(Set(
        GreaterThan(
          GetDegree(Variable("a")_, Some(RelTypeName("Y")_), OUTGOING)_,
          SignedDecimalIntegerLiteral("0")_)_,
        LessThanOrEqual(
          GetDegree(Variable("a")_, Some(RelTypeName("X")_), OUTGOING)_,
          SignedDecimalIntegerLiteral("0")_)_))_))_,
                AllNodesScan("a", Set.empty))
    )
  }

  test("should build plans containing getDegree for two negated pattern predicates") {
    planFor("MATCH (a) WHERE NOT (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(Ands(Set(Ors(Set(
        LessThanOrEqual(GetDegree(Variable("a")_, Some(RelTypeName("Y")_), OUTGOING)_,
                        SignedDecimalIntegerLiteral("0")_)_,
        LessThanOrEqual(GetDegree(Variable("a")_, Some(RelTypeName("X")_), OUTGOING)_,
                        SignedDecimalIntegerLiteral("0")_)_))_))_,  AllNodesScan("a", Set.empty))
    )
  }

  test("should plan all predicates along with named varlength pattern") {
    planFor("MATCH p=(a)-[r*]->(b) WHERE all(n in nodes(p) WHERE n.prop = 1337) RETURN p")._2 should beLike {
      case Projection(
      VarExpand(_, _, _, _, _,_, _, _, _,_,_,_,_,
                     Seq((Variable("n"),
                     In(Property(Variable("n"), PropertyKeyName("prop") ), ListLiteral(List(SignedDecimalIntegerLiteral("1337"))))))), _) => ()

    }
  }

  test("should plan none predicates along with named varlength pattern") {
    planFor("MATCH p=(a)-[r*]->(b) WHERE none(n in nodes(p) WHERE n.prop = 1337) RETURN p")._2 should beLike {
      case Projection(
      VarExpand(_, _, _, _, _,_, _, _, _,_,_,_,_,
                Seq((Variable("n"),
                Not(In(Property(Variable("n"), PropertyKeyName("prop") ), ListLiteral(List(SignedDecimalIntegerLiteral("1337")))))))), _) => ()

    }
  }

  test("should solve pattern comprehensions as part of VarExpand") {
    val q =
      """
        |MATCH p= ( (b) -[:REL*0..]- (c) )
        |WHERE
        | ALL(n in nodes(p) where
        |   n.prop <= 1 < n.prop2
        |   AND coalesce(
        |     head( [ (n)<--(d) WHERE d.prop3 <= 1 < d.prop2 | d.prop4 = true ] ),
        |     head( [ (n)<--(e) WHERE e.prop3 <= 1 < e.prop2 | e.prop5 = '0'] ),
        |     true)
        | )
        | AND ALL(r in relationships(p) WHERE r.prop <= 1 < r.prop2)
        |RETURN c
      """.stripMargin

    planFor(q) // Should not fail
    // The plan that solves the predicates as part of VarExpand is not chosen, but considered, thus we cannot assert on _that_ plan here.
    // Nevertheless the assertion LogicalPlanProducer.assertNoBadExpressionsExists should never fail, even for plans that do not get chosen.

  }

  private def containsArgumentOnly(queryGraph: QueryGraph): Boolean =
    queryGraph.argumentIds.nonEmpty && queryGraph.patternNodes.isEmpty && queryGraph.patternRelationships.isEmpty
}
