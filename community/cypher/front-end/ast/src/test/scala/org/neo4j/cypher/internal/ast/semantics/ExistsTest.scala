/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.SemanticCheckInTest.SemanticCheckWithDefaultContext
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.symbols.CTBoolean

class ExistsTest extends SemanticFunSuite {

  val n: NodePattern = NodePattern(Some(variable("n")), None, None, None)(pos)
  val x: NodePattern = expressions.NodePattern(Some(variable("x")), None, None, None)(pos)
  val r: RelationshipPattern = RelationshipPattern(None, None, None, None, None, SemanticDirection.OUTGOING)(pos)
  val relChain: RelationshipChain = RelationshipChain(n, r, x)(pos)
  val pattern: Pattern.ForMatch = patternForMatch(relChain)
  val property: Property = Property(variable("x"), PropertyKeyName("prop")(pos))(pos)
  val failingProperty: Property = Property(variable("missing"), PropertyKeyName("prop")(pos))(pos)

  test("valid exists expression passes semantic check") {
    val expression = simpleExistsExpression(pattern, Some(where(property)))

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("multiple patterns in inner match should not report error") {
    val multiPattern = patternForMatch(x, n)
    val expression = simpleExistsExpression(multiPattern, Some(where(property)))

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("inner where using missing identifier reports error") {
    val expression = simpleExistsExpression(pattern, Some(where(failingProperty)))

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("EXISTS cannot reuse identifier with different type") {
    val expression = simpleExistsExpression(pattern, Some(where(property)))

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get

    val result =
      SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos)
    )
  }

  test("EXISTS works for a regular query") {
    val expression = ExistsExpression(
      singleQuery(match_(relChain), return_(varFor("n").as("n")))
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("EXISTS does not work for a regular query ending with FINISH") {
    val expression = ExistsExpression(
      singleQuery(match_(relChain), finish())
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(
      SemanticError("An Exists Expression cannot contain a query ending with FINISH.", pos)
    )
  }

  test("EXISTS does not work for an updating query") {
    val expression = ExistsExpression(
      singleQuery(create(nodePat(Some("n"))))
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(
      SemanticError("An Exists Expression cannot contain any updates", pos)
    )
  }

  test("inner where with regular query using missing identifier reports error") {
    val expression = ExistsExpression(
      singleQuery(match_(relChain, where = Some(where(failingProperty))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("EXISTS with a regular query cannot reuse identifier with different type") {
    val expression = ExistsExpression(
      singleQuery(match_(relChain), return_(varFor("n").as("n")))
    )(pos, None, None)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos)
    )
  }
}
