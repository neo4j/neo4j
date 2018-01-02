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
package org.neo4j.cypher.internal.compiler.v2_3.planner.execution.convert

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.ProjectedPath
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.ProjectedPath._
import org.neo4j.cypher.internal.frontend.v2_3.ast.{Identifier, _}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v2_3.{DummyPosition, InputPosition, SemanticDirection}

class PathExpressionConversionTest extends CypherFunSuite {


  val pos = DummyPosition(0)

  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)


  test("p = (a)") {
    val expr = PathExpression(NodePathStep(Identifier("a")_, NilPathStep))_

    toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("a"),
        singleNodeProjector("a", nilProjector)
      )
    )
  }

  test("p = (b)<-[r]-(a)") {
    val expr = PathExpression(NodePathStep(Identifier("b")_, SingleRelationshipPathStep(Identifier("r")_, SemanticDirection.INCOMING, NilPathStep)))_

    toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("r", "b"),
        singleNodeProjector("b",
          singleIncomingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r]->(b)") {
    val expr = PathExpression(NodePathStep(Identifier("a")_, SingleRelationshipPathStep(Identifier("r")_, SemanticDirection.OUTGOING, NilPathStep)))_

    toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("r", "a"),
        singleNodeProjector("a",
          singleOutgoingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (b)<-[r*1..]-(a)") {
    val expr = PathExpression(NodePathStep(Identifier("b")_, MultiRelationshipPathStep(Identifier("r")_, SemanticDirection.INCOMING, NilPathStep)))_

    toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("r", "b"),
        singleNodeProjector("b",
          multiIncomingRelationshipProjector("r", nilProjector)
        )
      )
    )
  }

  test("p = (a)-[r*1..]->(b)") {
    val expr = PathExpression(NodePathStep(Identifier("a")_, MultiRelationshipPathStep(Identifier("r")_, SemanticDirection.OUTGOING, NilPathStep)))_

    toCommandProjectedPath(expr) should equal(
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
      NodePathStep(Identifier("a")_,
      MultiRelationshipPathStep(Identifier("r1")_, SemanticDirection.OUTGOING,
      SingleRelationshipPathStep(Identifier("r2")_, SemanticDirection.INCOMING,
      NilPathStep
    ))))_

    toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("a", "r1", "r2"),
        singleNodeProjector("a",
          multiOutgoingRelationshipProjector("r1",
            singleIncomingRelationshipProjector("r2", nilProjector)
          )
        )
      )
    )
  }

  test("p = (a)-[r1]->(b)<-[r2*1..2]-c") {
    val expr = PathExpression(
      NodePathStep(Identifier("a")_,
      MultiRelationshipPathStep(Identifier("r1")_, SemanticDirection.OUTGOING,
      SingleRelationshipPathStep(Identifier("r2")_, SemanticDirection.INCOMING,
      NilPathStep
    ))))_

    toCommandProjectedPath(expr) should equal(
      ProjectedPath(
        Set("a", "r1", "r2"),
        singleNodeProjector("a",
          multiOutgoingRelationshipProjector("r1",
            singleIncomingRelationshipProjector("r2", nilProjector)
          )
        )
      )
    )
  }
}
