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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical

import org.neo4j.cypher.internal.compiler.v4_0.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.compiler.v4_0.planner._
import org.neo4j.cypher.internal.ir.v4_0.{QueryGraph, RegularPlannerQuery}
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.logical.plans.{NestedPlanExpression, _}

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
          case ListComprehension(ExtractScope(_, Some(NestedPlanExpression(nestedPlan, _)), _), _) =>
            nestedPlan should equal(
              Selection(
                ands(hasLabels("  NODE116", "ComedyClub")),
                Expand(
                  Argument(Set("f")),
                  "f", OUTGOING, Seq(RelTypeName("WORKS_AT")_), "  NODE116", "  REL102", ExpandAll
                )
              )
            )
        }
    }
  }

  test("should build plans with getDegree for a single pattern predicate") {
    planFor("MATCH (a) WHERE (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X") _), OUTGOING)_, literalInt(0))
        ), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a single negated pattern predicate") {
    planFor("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(
          lessThanOrEqual(GetDegree(varFor("a"),Some(RelTypeName("X") _), OUTGOING)_, literalInt(0))
        ), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two pattern predicates") {
    planFor("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a")._2 should equal(
      Selection(
        ands(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0))
        ), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a pattern predicate and an expression") {
    planFor("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X")_),OUTGOING)_, literalInt(0)),
          propGreaterThan("a", "prop", 4)
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a pattern predicate and multiple expressions") {
    planFor("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("X")_),OUTGOING)_, literalInt(0)),
          propGreaterThan("a", "prop", 4),
          in(prop("a", "prop2"), listOfInt(9))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for a single negated pattern predicate and an expression") {
    planFor("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_),OUTGOING)_, literalInt(0)),
          in(prop("a", "prop"), listOfInt(9))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two pattern predicates and expressions") {
    planFor("MATCH (a) WHERE a.prop = 9 OR (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0)),
          in(prop("a", "prop"), listOfInt(9))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two pattern predicates with one negation") {
    planFor("MATCH (a) WHERE (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          greaterThan(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0))
        )), AllNodesScan("a", Set.empty)
      )
    )
  }

  test("should build plans containing getDegree for two negated pattern predicates") {
    planFor("MATCH (a) WHERE NOT (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a")._2 should equal(
      Selection(
        ands(ors(
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("Y")_), OUTGOING)_, literalInt(0)),
          lessThanOrEqual(GetDegree(varFor("a"), Some(RelTypeName("X")_), OUTGOING)_, literalInt(0))
        )),  AllNodesScan("a", Set.empty)
      )
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

  private def containsArgumentOnly(queryGraph: QueryGraph): Boolean =
    queryGraph.argumentIds.nonEmpty && queryGraph.patternNodes.isEmpty && queryGraph.patternRelationships.isEmpty
}
