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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical

import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.mockito.Mockito._
import org.mockito.Matchers._

class PatternPredicatePlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  private implicit val planContext = newMockedPlanContext
  when(planContext.getOptRelTypeId(any())).thenReturn(None)
  when(planContext.getOptPropertyKeyId(any())).thenReturn(None)

  private val factory = newMockedMetricsFactory
  when(factory.newCardinalityEstimator(any(), any(), any())).thenReturn((plan: LogicalPlan) => plan match {
    case _: AllNodesScan => 2000000
    case _: Expand => 10
    case _: SingleRow => 1
    case _ => Double.MaxValue
  })

  private implicit val planner = newPlanner(factory)

  test("should build plans containing semi apply for a single pattern predicate") {
    produceLogicalPlan("MATCH (a) WHERE (a)-[:X]->() RETURN a") should equal(
      SemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
        )
      )
    )
  }

  test("should build plans containing anti semi apply for a single negated pattern predicate") {
    produceLogicalPlan("MATCH (a) WHERE NOT (a)-[:X]->() RETURN a") should equal(
      AntiSemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED31", "  UNNAMED23", SimplePatternLength
        )
      )
    )
  }

  test("should build plans containing semi apply for two pattern predicates") {
    produceLogicalPlan("MATCH (a) WHERE (a)-[:X]->() AND (a)-[:Y]->() RETURN a") should equal(
      SemiApply(
        SemiApply(
          AllNodesScan("a"),
          Expand(
            SingleRow(Set("a")),
            "a", Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
          )
        ),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("Y")_), "  UNNAMED44", "  UNNAMED36", SimplePatternLength
        )
      )
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and an expression") {
    produceLogicalPlan("MATCH (a) WHERE (a)-[:X]->() OR a.prop > 4 RETURN a") should equal(
      SelectOrSemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED27", "  UNNAMED19", SimplePatternLength
        ),
        GreaterThan(Property(Identifier("a")_, PropertyKeyName("prop")_)_, SignedIntegerLiteral("4")_)_
      )
    )
  }

  test("should build plans containing select or semi apply for a pattern predicate and multiple expressions") {
    produceLogicalPlan("MATCH (a) WHERE a.prop2 = 9 OR (a)-[:X]->() OR a.prop > 4 RETURN a") should equal(
      SelectOrSemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED42", "  UNNAMED34", SimplePatternLength
        ),
        Ors(List(
          Equals(Property(Identifier("a")_, PropertyKeyName("prop2")_)_, SignedIntegerLiteral("9")_)_,
          GreaterThan(Property(Identifier("a")_, PropertyKeyName("prop")_)_, SignedIntegerLiteral("4")_)_
        ))_
      )
    )
  }

  test("should build plans containing select or anti semi apply for a single negated pattern predicate") {
    produceLogicalPlan("MATCH (a) WHERE a.prop = 9 OR NOT (a)-[:X]->() RETURN a") should equal(
      SelectOrAntiSemiApply(
        AllNodesScan("a"),
        Expand(
          SingleRow(Set("a")),
          "a", Direction.OUTGOING, Seq(RelTypeName("X")_), "  UNNAMED45", "  UNNAMED37", SimplePatternLength
        ),
        Equals(Property(Identifier("a")_, PropertyKeyName("prop")_)_, SignedIntegerLiteral("9")_)_
      )
    )
  }

}
