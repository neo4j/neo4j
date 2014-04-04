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

import org.neo4j.cypher.internal.compiler.v2_1.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.graphdb.Direction
import org.mockito.Mockito._
import org.mockito.Matchers._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Selection
import org.neo4j.cypher.internal.compiler.v2_1.ast.StringLiteral
import org.neo4j.cypher.internal.compiler.v2_1.ast.RelTypeName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Projection
import org.neo4j.cypher.internal.compiler.v2_1.ast.NotEquals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Equals
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.PropertyKeyName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.Expand
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.compiler.v2_1.ast.Property

class ExpandPlanningIntegrationTest extends CypherFunSuite with LogicalPlanningTestSupport {

  test("Should build plans containing expand for single relationship pattern") {
    implicit val statistics = newMockedStatistics
    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(newMetricsFactory)

    produceLogicalPlan("MATCH (a)-[r]->(b) RETURN r") should equal(
      Projection(
        Expand(
          AllNodesScan("a"),
          "a", Direction.OUTGOING, Seq.empty, "b", "r", SimplePatternLength
        )( null ),
        Map("r" -> Identifier("r") _)
      )
    )
  }

  test("Should build plans containing expand for two unrelated relationship patterns") {
    implicit val statistics = newMockedStatistics
    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(newMetricsFactory)

    produceLogicalPlan("MATCH (a)-[r1]->(b), (c)-[r2]->(d) RETURN r1, r2") should equal(
      Projection(
        Selection(
          Seq(NotEquals(Identifier("r1") _, Identifier("r2") _) _),
          CartesianProduct(
            Expand(
              AllNodesScan("a"),
              "a", Direction.OUTGOING, Seq.empty, "b", "r1", SimplePatternLength
            )( null ),
            Expand(
              AllNodesScan("c"),
              "c", Direction.OUTGOING, Seq.empty, "d", "r2", SimplePatternLength
            )( null )
          )
        ),
        Map(
          "r1" -> Identifier("r1") _,
          "r2" -> Identifier("r2") _
        )
      )
    )
  }

  test("Should build plans containing expand for self-referencing relationship patterns") {
    implicit val statistics = newMockedStatistics
    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(newMetricsFactory)

    produceLogicalPlan("MATCH (a)-[r]->(a) RETURN r") should equal(
      Projection(
        Selection(
          Seq(Equals(Identifier("a") _, Identifier("a$$$") _) _),
          Expand(
            AllNodesScan("a"),
            "a", Direction.OUTGOING, Seq.empty, "a$$$", "r", SimplePatternLength
          )( null )
        ),
        Map("r" -> Identifier("r") _)
      )
    )
  }

  test("Should build plans containing expand for looping relationship patterns") {
    implicit val statistics = newMockedStatistics
    implicit val planContext = newMockedPlanContext
    implicit val planner = newPlanner(newMetricsFactory)

    produceLogicalPlan("MATCH (a)-[r1]->(b)<-[r2]-(a) RETURN r1, r2") should equal(
      Projection(
        Selection(
          Seq(NotEquals(Identifier("r1") _, Identifier("r2") _) _),
          Selection(
            Seq(Equals(Identifier("b") _, Identifier("b$$$") _) _),
            Expand(
              Expand(AllNodesScan("a"), "a", Direction.OUTGOING, Seq.empty, "b", "r1", SimplePatternLength)(null),
              "a", Direction.OUTGOING, Seq.empty, "b$$$", "r2", SimplePatternLength
            )( null )
          )
        ),
        Map(
          "r1" -> Identifier("r1") _,
          "r2" -> Identifier("r2") _
        )
      )
    )
  }

  test("Should build plans expanding from the cheaper side for single relationship pattern") {
    implicit val planContext = newMockedPlanContext
    val factory: DummyMetricsFactory = newMockedMetricsFactory
    when(factory.newCardinalityEstimator(any(), any())).thenReturn((plan: LogicalPlan) => plan match {
      case _: NodeIndexSeek => 10.0
      case _: AllNodesScan  => 100.04
      case _                => Double.MaxValue
    })
    implicit val planner = newPlanner(factory)

    when(planContext.getOptRelTypeId("x")).thenReturn(None)
    when(planContext.getOptPropertyKeyId("name")).thenReturn(None)

    produceLogicalPlan("MATCH (start)-[rel:x]-(a) WHERE a.name = 'Andres' return a") should equal(
      Projection(
        Expand(
          Selection(
            Seq(Equals(Property(Identifier("a")_, PropertyKeyName("name")()_)_, StringLiteral("Andres")_)_),
            AllNodesScan("a")
          ),
          "a", Direction.BOTH, Seq(RelTypeName("x")() _), "start", "rel", SimplePatternLength
        )( null ),
        Map("a" -> Identifier("a") _)
      )
    )
  }
}
