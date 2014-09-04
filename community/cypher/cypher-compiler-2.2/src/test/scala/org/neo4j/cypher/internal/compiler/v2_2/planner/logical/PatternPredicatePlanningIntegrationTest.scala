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

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.planner._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_2.ast._

class PatternPredicatePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport2 {
  test("should build plans containing semi apply for a single pattern predicate") {
   planFor("MATCH (a) WHERE (a)-[:X]->() RETURN a").plan should equal(
      SemiApply(
        AllNodesScan("a", Set.empty)(PlannerQuery.empty),
        Expand(
          SingleRow(Set("a"))(PlannerQuery.empty)(),
          "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
        )(PlannerQuery.empty)
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing anti semi apply for a single negated pattern predicate") {
    planFor("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a").plan should equal(
      AntiSemiApply(
        AllNodesScan("a", Set.empty)(PlannerQuery.empty),
        Expand(
          SingleRow(Set("a"))(PlannerQuery.empty)(),
          "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED31", "  UNNAMED23", SimplePatternLength
        )(PlannerQuery.empty)
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a").plan should equal(
      SemiApply(
        SemiApply(
          AllNodesScan("a", Set.empty)(PlannerQuery.empty),
          Expand(
            SingleRow(Set("a"))(PlannerQuery.empty)(),
            "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("Y")_), "  UNNAMED44", "  UNNAMED36", SimplePatternLength
          )(PlannerQuery.empty)
        )(PlannerQuery.empty),
        Expand(
          SingleRow(Set("a"))(PlannerQuery.empty)(),
          "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
        )(PlannerQuery.empty)
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and an expression") {
    planFor("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a").plan should equal(
      SelectOrSemiApply(
        AllNodesScan("a", Set.empty)(PlannerQuery.empty),
        Expand(
          SingleRow(Set("a"))(PlannerQuery.empty)(),
          "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
        )(PlannerQuery.empty),
        GreaterThan(Property(Identifier("a")_, PropertyKeyName("prop")_)_, SignedDecimalIntegerLiteral("4")_)_
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and multiple expressions") {
    planFor("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a").plan should equal(
      SelectOrSemiApply(
        AllNodesScan("a", Set.empty)(PlannerQuery.empty),
        Expand(
          SingleRow(Set("a"))(PlannerQuery.empty)(),
          "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED42", "  UNNAMED34", SimplePatternLength
        )(PlannerQuery.empty),
        Ors(Set(
          In(Property(Identifier("a")_, PropertyKeyName("prop2")_)_, Collection(Seq(SignedDecimalIntegerLiteral("9")_))_)_,
          GreaterThan(Property(Identifier("a")_, PropertyKeyName("prop")_)_, SignedDecimalIntegerLiteral("4")_)_
        ))_
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing select or anti semi apply for a single negated pattern predicate") {
    planFor("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a").plan should equal(
      SelectOrAntiSemiApply(
        AllNodesScan("a", Set.empty)(PlannerQuery.empty),
        Expand(
          SingleRow(Set("a"))(PlannerQuery.empty)(),
          "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED45", "  UNNAMED37", SimplePatternLength
        )(PlannerQuery.empty),
        In(Property(Identifier("a")_, PropertyKeyName("prop")_)_, Collection(Seq(SignedDecimalIntegerLiteral("9")_))_)_
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing let select or semi apply and select or semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE a.prop = 9 OR (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a").plan should equal(
      Projection(
        SelectOrAntiSemiApply(
          LetSelectOrSemiApply(
            AllNodesScan("a", Set.empty)(PlannerQuery.empty),
            Expand(
              SingleRow(Set("a"))(PlannerQuery.empty)(),
              "a", Direction.OUTGOING, Direction.OUTGOING,  Seq(RelTypeName("Y") _), "  UNNAMED41", "  UNNAMED33", SimplePatternLength
            )(PlannerQuery.empty),
            "  FRESHID30",
            In(Property(Identifier("a") _, PropertyKeyName("prop") _) _, Collection(Seq(SignedDecimalIntegerLiteral("9")_))_)_
          )(PlannerQuery.empty),
          Expand(
            SingleRow(Set("a"))(PlannerQuery.empty)(),
            "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED61", "  UNNAMED53", SimplePatternLength
          )(PlannerQuery.empty),
          ident("  FRESHID30")
        )(PlannerQuery.empty),
        Map("a" -> ident("a"))
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing let semi apply and select or semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a").plan should equal(
      Projection(
        SelectOrAntiSemiApply(
          LetSemiApply(
            AllNodesScan("a", Set.empty)(PlannerQuery.empty),
            Expand(
              SingleRow(Set("a"))(PlannerQuery.empty)(),
              "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("Y") _), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
            )(PlannerQuery.empty),
            "  FRESHID16"
          )(PlannerQuery.empty),
          Expand(
            SingleRow(Set("a"))(PlannerQuery.empty)(),
            "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED47", "  UNNAMED39", SimplePatternLength
          )(PlannerQuery.empty),
          ident("  FRESHID16")
        )(PlannerQuery.empty),
        Map("a" -> ident("a"))
      )(PlannerQuery.empty)
    )
  }

  test("should build plans containing let anti semi apply and select or semi apply for two pattern predicates") {
    planFor("MATCH (a) WHERE NOT (a)-[:Y]->() OR NOT (a)-[:X]->() RETURN a").plan should equal(
      Projection(
        SelectOrAntiSemiApply(
          LetAntiSemiApply(
            AllNodesScan("a", Set.empty)(PlannerQuery.empty),
            Expand(
              SingleRow(Set("a"))(PlannerQuery.empty)(),
              "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("Y") _), "  UNNAMED31", "  UNNAMED23", SimplePatternLength
            )(PlannerQuery.empty),
            "  FRESHID20"
          )(PlannerQuery.empty),
          Expand(
            SingleRow(Set("a"))(PlannerQuery.empty)(),
            "a", Direction.OUTGOING, Direction.OUTGOING, Seq(RelTypeName("X") _), "  UNNAMED51", "  UNNAMED43", SimplePatternLength
          )(PlannerQuery.empty),
          ident("  FRESHID20")
        )(PlannerQuery.empty),
        Map("a" -> ident("a"))
      )(PlannerQuery.empty)
    )
  }
}
