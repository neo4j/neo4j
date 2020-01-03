/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.convert

import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath._
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v4_0.util.{DummyPosition, InputPosition}

class PathExpressionConversionTest extends CypherFunSuite {

  val converters = new ExpressionConverters(CommunityExpressionConverter(TokenContext.EMPTY))

  val pos = DummyPosition(0)

  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)


  test("p = (a)") {
    val expr = PathExpression(NodePathStep(Variable("a")_, NilPathStep))_

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("a"),
        singleNodeProjector("a", nilProjector)
      )
    )
  }

  test("p = (b)<-[r]-(a)") {
    val expr = PathExpression(NodePathStep(Variable("b")_, SingleRelationshipPathStep(Variable("r")_, SemanticDirection.INCOMING, Some(Variable("a")_), NilPathStep)))_

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("r", "b"),
        singleNodeProjector("b",
              singleRelationshipWithKnownTargetProjector("r", "a", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r]->(b)") {
    val expr = PathExpression(NodePathStep(Variable("a")_, SingleRelationshipPathStep(Variable("r")_, SemanticDirection.OUTGOING, Some(Variable("b")_), NilPathStep)))_

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("r", "a"),
        singleNodeProjector("a",
                singleRelationshipWithKnownTargetProjector("r", "b", nilProjector)
        )
      )
    )
  }

  test("p = (b)<-[r*1..]-(a)") {
    val expr = PathExpression(NodePathStep(Variable("b")_, MultiRelationshipPathStep(Variable("r")_, SemanticDirection.INCOMING, Some(Variable("a")_), NilPathStep)))_

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("r", "b"),
        singleNodeProjector("b",
          multiIncomingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r*1..]->(b)") {
    val expr = PathExpression(NodePathStep(Variable("a")_, MultiRelationshipPathStep(Variable("r")_, SemanticDirection.OUTGOING, Some(Variable("b")_), NilPathStep)))_

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("r", "a"),
        singleNodeProjector("a",
          multiOutgoingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r1*1..2]->(b)<-[r2]-c") {
    val expr = PathExpression(
      NodePathStep(Variable("a")_,
      MultiRelationshipPathStep(Variable("r1")_, SemanticDirection.OUTGOING, Some(Variable("b")_),
      SingleRelationshipPathStep(Variable("r2")_, SemanticDirection.INCOMING, Some(Variable("c")_),
      NilPathStep
    ))))_

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("a", "r1", "r2"),
        singleNodeProjector("a",
          multiOutgoingRelationshipProjector("r1",
            singleRelationshipWithKnownTargetProjector("r2", "c", nilProjector)
          )
        )
      )
    )
  }

  test("p = (a)-[r1]->(b)<-[r2*1..2]-c") {
    val expr = PathExpression(
      NodePathStep(Variable("a")_,
      MultiRelationshipPathStep(Variable("r1")_, SemanticDirection.OUTGOING, Some(Variable("b")_),
      SingleRelationshipPathStep(Variable("r2")_, SemanticDirection.INCOMING, Some(Variable("c")_),
      NilPathStep
    ))))_

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("a", "r1", "r2"),
        singleNodeProjector("a",
          multiOutgoingRelationshipProjector("r1",
            singleRelationshipWithKnownTargetProjector("r2", "c", nilProjector)
          )
        )
      )
    )
  }
}
