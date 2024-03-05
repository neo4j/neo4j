/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.convert

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.varFor
import org.neo4j.cypher.internal.expressions.MultiRelationshipPathStep
import org.neo4j.cypher.internal.expressions.NilPathStep
import org.neo4j.cypher.internal.expressions.NodePathStep
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.RepeatPathStep
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SingleRelationshipPathStep
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiIncomingRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.multiOutgoingRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.nilProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.quantifiedPathProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.singleNodeProjector
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath.singleRelationshipWithKnownTargetProjector
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.DummyPosition
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

import scala.language.implicitConversions

class PathExpressionConversionTest extends CypherFunSuite {

  val converters =
    new ExpressionConverters(
      None,
      CommunityExpressionConverter(
        ReadTokenContext.EMPTY,
        new AnonymousVariableNameGenerator(),
        new SelectivityTrackerRegistrator(),
        CypherRuntimeConfiguration.defaultConfiguration
      )
    )

  val pos = DummyPosition(0)

  implicit def withPos[T](expr: InputPosition => T): T = expr(pos)

  test("p = (a)") {
    val expr = PathExpression(NodePathStep(Variable("a") _, NilPathStep()(pos))(pos)) _

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector("a", nilProjector))
    )
  }

  test("p = (b)<-[r]-(a)") {
    val expr = PathExpression(NodePathStep(
      Variable("b") _,
      SingleRelationshipPathStep(
        Variable("r") _,
        SemanticDirection.INCOMING,
        Some(Variable("a") _),
        NilPathStep()(pos)
      )(pos)
    )(pos)) _

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector("b", singleRelationshipWithKnownTargetProjector("r", "a", nilProjector)))
    )
  }

  test("p = (a)-[r]->(b)") {
    val expr = PathExpression(NodePathStep(
      Variable("a") _,
      SingleRelationshipPathStep(
        Variable("r") _,
        SemanticDirection.OUTGOING,
        Some(Variable("b") _),
        NilPathStep()(pos)
      )(pos)
    )(pos)) _

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector("a", singleRelationshipWithKnownTargetProjector("r", "b", nilProjector)))
    )
  }

  test("p = (b)<-[r*1..]-(a)") {
    val expr =
      PathExpression(NodePathStep(
        Variable("b") _,
        MultiRelationshipPathStep(
          Variable("r") _,
          SemanticDirection.INCOMING,
          Some(Variable("a") _),
          NilPathStep()(pos)
        )(pos)
      )(pos)) _

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector("b", multiIncomingRelationshipWithKnownTargetProjector("r", "a", nilProjector)))
    )
  }

  test("p = (a)-[r*1..]->(b)") {
    val expr =
      PathExpression(NodePathStep(
        Variable("a") _,
        MultiRelationshipPathStep(
          Variable("r") _,
          SemanticDirection.OUTGOING,
          Some(Variable("b") _),
          NilPathStep()(pos)
        )(pos)
      )(pos)) _

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector("a", multiOutgoingRelationshipWithKnownTargetProjector("r", "b", nilProjector)))
    )
  }

  test("p = (a)-[r1*1..2]->(b)<-[r2]-c") {
    val expr = PathExpression(
      NodePathStep(
        Variable("a") _,
        MultiRelationshipPathStep(
          Variable("r1") _,
          SemanticDirection.OUTGOING,
          Some(Variable("b") _),
          SingleRelationshipPathStep(
            Variable("r2") _,
            SemanticDirection.INCOMING,
            Some(Variable("c") _),
            NilPathStep()(pos)
          )(pos)
        )(pos)
      )(pos)
    ) _

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector(
        "a",
        multiOutgoingRelationshipWithKnownTargetProjector(
          "r1",
          "b",
          singleRelationshipWithKnownTargetProjector("r2", "c", nilProjector)
        )
      ))
    )
  }

  test("p = (a)-[r1]->(b)<-[r2*1..2]-c") {
    val expr = PathExpression(
      NodePathStep(
        Variable("a") _,
        MultiRelationshipPathStep(
          Variable("r1") _,
          SemanticDirection.OUTGOING,
          Some(Variable("b") _),
          SingleRelationshipPathStep(
            Variable("r2") _,
            SemanticDirection.INCOMING,
            Some(Variable("c") _),
            NilPathStep()(pos)
          )(pos)
        )(pos)
      )(pos)
    ) _

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector(
        "a",
        multiOutgoingRelationshipWithKnownTargetProjector(
          "r1",
          "b",
          singleRelationshipWithKnownTargetProjector("r2", "c", nilProjector)
        )
      ))
    )
  }

  test("p = (a) ((n)-[r]->(m)-[q]->(o))+ (b)") {
    val expr = PathExpression(
      NodePathStep(
        varFor("a"),
        RepeatPathStep.asRepeatPathStep(
          List(
            varFor("n"),
            varFor("r"),
            varFor("m"),
            varFor("q")
          ),
          varFor("b"),
          NilPathStep()(pos)
        )(pos)
      )(pos)
    )(pos)

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector(
        "a",
        quantifiedPathProjector(List("n", "r", "m", "q"), "b", nilProjector)
      ))
    )
  }

  test("p = (a) ((n)-[r]-(m)-[q]-(n))* (x) ((b)-[r2]-(c))* (k)") {
    val expr = PathExpression(
      NodePathStep(
        varFor("a"),
        RepeatPathStep.asRepeatPathStep(
          List(varFor("n"), varFor("r"), varFor("m"), varFor("q")),
          varFor("x"),
          RepeatPathStep.asRepeatPathStep(
            List(varFor("b"), varFor("r2")),
            varFor("k"),
            NilPathStep()(pos)
          )(pos)
        )(pos)
      )(pos)
    )(pos)

    converters.toCommandProjectedPath(expr) should equal(
      ProjectedPath(singleNodeProjector(
        "a",
        quantifiedPathProjector(
          List("n", "r", "m", "q"),
          "x",
          quantifiedPathProjector(List("b", "r2"), "k", nilProjector)
        )
      ))
    )
  }
}
