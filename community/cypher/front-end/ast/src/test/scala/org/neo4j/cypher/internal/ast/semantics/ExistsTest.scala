/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.cypher.internal.ast.FullExistsExpression
import org.neo4j.cypher.internal.ast.SimpleExistsExpression
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.EveryPath
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
  val pattern: Pattern = Pattern(Seq(EveryPath(relChain)))(pos)
  val property: Property = Property(variable("x"), PropertyKeyName("prop")(pos))(pos)
  val failingProperty: Property = Property(variable("missing"), PropertyKeyName("prop")(pos))(pos)

  test("valid exists expression passes semantic check") {
    val expression = SimpleExistsExpression(pattern, Some(where(property)))(pos, Set.empty, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("multiple patterns in inner match should not report error") {
    val multiPattern: Pattern = Pattern(Seq(EveryPath(x), EveryPath(n)))(pos)
    val expression = SimpleExistsExpression(multiPattern, Some(where(property)))(pos, Set.empty, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("inner where using missing identifier reports error") {
    val expression = SimpleExistsExpression(pattern, Some(where(failingProperty)))(pos, Set.empty, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("EXISTS cannot reuse identifier with different type") {
    val expression = SimpleExistsExpression(pattern, Some(where(property)))(pos, Set.empty, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos)
    )
  }

  test("EXISTS works for a regular query") {
    val expression = FullExistsExpression(
      query(singleQuery(match_(relChain), return_(varFor("n").as("n"))))
    )(pos, Set.empty, Set.empty)

    val result =
      SemanticExpressionCheck.simple(expression)(SemanticState.clean.withFeature(SemanticFeature.FullExistsSupport))

    result.errors shouldBe empty
  }

  test("EXISTS does not work for an updating query") {
    val expression = FullExistsExpression(
      query(singleQuery(create(nodePat(Some("n")))))
    )(pos, Set.empty, Set.empty)

    val result =
      SemanticExpressionCheck.simple(expression)(SemanticState.clean.withFeature(SemanticFeature.FullExistsSupport))

    result.errors shouldBe Seq(
      SemanticError("An Exists Expression cannot contain any updates", pos)
    )
  }

  test("inner where with regular query using missing identifier reports error") {
    val expression = FullExistsExpression(
      query(singleQuery(match_(relChain, Some(where(failingProperty))), return_(varFor("n").as("n"))))
    )(pos, Set.empty, Set.empty)

    val result =
      SemanticExpressionCheck.simple(expression)(SemanticState.clean.withFeature(SemanticFeature.FullExistsSupport))

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("EXISTS with a regular query cannot reuse identifier with different type") {
    val expression = FullExistsExpression(
      query(singleQuery(match_(relChain), return_(varFor("n").as("n"))))
    )(pos, Set.empty, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get.withFeature(
      SemanticFeature.FullExistsSupport
    )

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos)
    )
  }
}
