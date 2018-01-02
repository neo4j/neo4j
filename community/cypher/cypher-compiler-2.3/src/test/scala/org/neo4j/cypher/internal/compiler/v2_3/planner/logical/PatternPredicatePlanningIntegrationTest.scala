/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import org.neo4j.cypher.internal.compiler.v2_3.ast.NestedPlanExpression
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class PatternPredicatePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("should consider identifiers introduced by outer list comprehensions when planning pattern predicates") {
    val plan = (new given {
      cardinality = mapCardinality {
        // expand
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 => 10
        // argument
        case PlannerQuery(queryGraph, _, _) if containsArgumentOnly(queryGraph) => 1
        case _ => 4000000
      }
    } planFor """MATCH (a:Person)-[:KNOWS]->(b:Person) WITH a, collect(b) AS friends RETURN a, [f IN friends WHERE (f)-[:WORKS_AT]->(:ComedyClub)] AS clowns""").plan

    plan match {
      case Projection(_, expressions) =>
        expressions("clowns") match {
          case ListComprehension(ExtractScope(_, Some(NestedPlanExpression(nestedPlan, _)), _), _) =>
            nestedPlan should equal(
              Selection(
                Seq(HasLabels(ident("  UNNAMED116"), Seq(LabelName("ComedyClub")_))_),
                Expand(
                  Argument(Set("f"))(solved)(),
                  "f", SemanticDirection.OUTGOING, Seq(RelTypeName("WORKS_AT")_), "  UNNAMED116", "  UNNAMED102", ExpandAll
                )(solved)
              )(solved)
            )
        }
    }
  }

  test("should build plans containing semi apply for a single pattern predicate") {
    planFor("MATCH (a) WHERE (a)-[:X]->() RETURN a").plan should equal(
      SemiApply(
        AllNodesScan("a", Set.empty)(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED27", "  UNNAMED20"
        )(solved)
      )(solved)
    )
  }

  test("should build plans containing anti semi apply for a single negated pattern predicate") {
    planFor("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a").plan should equal(
      AntiSemiApply(
        AllNodesScan("a", Set.empty)(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED31", "  UNNAMED24"
        )(solved)
      )(solved)
    )
  }

  test("should build plans containing semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a").plan should equal(
      SemiApply(
        SemiApply(
          AllNodesScan("a", Set.empty)(solved),
          Expand(
            Argument(Set("a"))(solved)(),
            "a", SemanticDirection.OUTGOING, Seq(RelTypeName("Y") _), "  UNNAMED44", "  UNNAMED37"
          )(solved)
        )(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED27", "  UNNAMED20"
        )(solved)
      )(solved)
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and an expression") {
    planFor("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a").plan should equal(
      SelectOrSemiApply(
        AllNodesScan("a", Set.empty)(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED27", "  UNNAMED20"
        )(solved),
        GreaterThan(Property(Identifier("a") _, PropertyKeyName("prop") _) _, SignedDecimalIntegerLiteral("4") _) _
      )(solved)
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and multiple expressions") {
    planFor("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a").plan should equal(
      SelectOrSemiApply(
        AllNodesScan("a", Set.empty)(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED42", "  UNNAMED35"
        )(solved),
        Ors(Set(
          In(Property(Identifier("a") _, PropertyKeyName("prop2") _) _, Collection(Seq(SignedDecimalIntegerLiteral("9") _)) _) _,
          GreaterThan(Property(Identifier("a") _, PropertyKeyName("prop") _) _, SignedDecimalIntegerLiteral("4") _) _
        )) _
      )(solved)
    )
  }

  test("should build plans containing select or anti semi apply for a single negated pattern predicate") {
    planFor("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a").plan should equal(
      SelectOrAntiSemiApply(
        AllNodesScan("a", Set.empty)(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED45", "  UNNAMED38"
        )(solved),
        In(Property(Identifier("a") _, PropertyKeyName("prop") _) _, Collection(Seq(SignedDecimalIntegerLiteral("9") _)) _) _
      )(solved)
    )
  }

  test("should build plans containing let select or semi apply and select or semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE a.prop = 9 OR (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a").plan should equal(
      SelectOrAntiSemiApply(
        LetSelectOrSemiApply(
          AllNodesScan("a", Set.empty)(solved),
          Expand(
            Argument(Set("a"))(solved)(),
            "a", SemanticDirection.OUTGOING, Seq(RelTypeName("Y") _), "  UNNAMED41", "  UNNAMED34"
          )(solved),
          "  FRESHID30",
          In(Property(Identifier("a") _, PropertyKeyName("prop") _) _, Collection(Seq(SignedDecimalIntegerLiteral("9") _)) _) _
        )(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED61", "  UNNAMED54"
        )(solved),
        ident("  FRESHID30")
      )(solved)
    )
  }

  test("should build plans containing let semi apply and select or semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a").plan should equal(
      SelectOrAntiSemiApply(
        LetSemiApply(
          AllNodesScan("a", Set.empty)(solved),
          Expand(
            Argument(Set("a"))(solved)(),
            "a", SemanticDirection.OUTGOING, Seq(RelTypeName("Y") _), "  UNNAMED27", "  UNNAMED20"
          )(solved),
          "  FRESHID16"
        )(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED47", "  UNNAMED40"
        )(solved),
        ident("  FRESHID16")
      )(solved)
    )
  }

  test("should build plans containing let anti semi apply and select or semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE NOT (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a").plan should equal(
      SelectOrAntiSemiApply(
        LetAntiSemiApply(
          AllNodesScan("a", Set.empty)(solved),
          Expand(
            Argument(Set("a"))(solved)(),
            "a", SemanticDirection.OUTGOING, Seq(RelTypeName("Y") _), "  UNNAMED31", "  UNNAMED24"
          )(solved),
          "  FRESHID20"
        )(solved),
        Expand(
          Argument(Set("a"))(solved)(),
          "a", SemanticDirection.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED51", "  UNNAMED44"
        )(solved),
        ident("  FRESHID20")
      )(solved)
    )
  }

  private def containsArgumentOnly(queryGraph: QueryGraph): Boolean =
    queryGraph.argumentIds.nonEmpty && queryGraph.patternNodes.size == 0 && queryGraph.patternRelationships.isEmpty
}
