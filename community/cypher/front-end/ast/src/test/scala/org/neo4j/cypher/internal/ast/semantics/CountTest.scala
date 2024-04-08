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

import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.SemanticCheckInTest.SemanticCheckWithDefaultContext
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTNode

import scala.collection.compat.immutable.ArraySeq

class CountTest extends SemanticFunSuite {

  private val n: NodePattern = nodePat(Some("n"))
  private val x: NodePattern = nodePat(Some("x"))
  private val r: RelationshipPattern = relPat()
  private val label = Leaf(labelName("Label"))
  private val relChain = RelationshipChain(n, r, x)(pos)
  private val pattern: Pattern.ForMatch = patternForMatch(relChain)

  private val nodePredicate =
    Equals(
      Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos),
      StringLiteral("test")(pos.withInputLength(0))
    )(pos)
  private val property: Property = Property(variable("x"), PropertyKeyName("prop")(pos))(pos)
  private val failingProperty: Property = Property(variable("missing"), PropertyKeyName("prop")(pos))(pos)

  private val nodeProperties: Expression =
    MapExpression(ArraySeq((PropertyKeyName("name")(pos), StringLiteral("test")(pos.withInputLength(0)))))(pos)

  test("valid count expression passes semantic check") {
    val expression = simpleCountExpression(pattern, Some(where(property)))

    val result = SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("inner where using missing identifier reports error") {
    val expression = simpleCountExpression(pattern, Some(where(failingProperty)))

    val result = SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("count expression cannot reuse identifier with different type") {
    val expression = simpleCountExpression(pattern, Some(where(property)))

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos)
    )
  }

  // COUNT { (n: Label) } should succeed the semantic check
  test("count expression can contain a node pattern with label") {
    val p = patternForMatch(nodePat(Some("n"), labelExpression = Some(label)))
    val expression = simpleCountExpression(p, maybeWhere = None)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n) } should succeed the semantic check even if n is not recorded in the outer scope
  test("can contain a non declared variable in a single node pattern") {
    val p = patternForMatch(nodePat(Some("n")))
    val expression = simpleCountExpression(p, maybeWhere = None)

    val semanticState = SemanticState.clean.declareVariable(variable("x"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  test("COUNT { () } should succeed the semantic check") {
    val p = patternForMatch(nodePat())
    val expression = simpleCountExpression(p, maybeWhere = None)

    val semanticState = SemanticState.clean.declareVariable(variable("x"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  test("COUNT { (:Label) } should succeed the semantic check") {
    val p = patternForMatch(nodePat(None, labelExpression = Some(label)))
    val expression = simpleCountExpression(p, maybeWhere = None)

    val semanticState = SemanticState.clean.declareVariable(variable("x"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n{name:"test"}) } should succeed the semantic check
  test("count expression can contain node properties in a single node match") {
    val p = patternForMatch(nodePat(None, properties = Some(nodeProperties)))
    val expression = simpleCountExpression(p, maybeWhere = None)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n WHERE n.prop = "test") } should succeed the semantic check
  test("count expression can contain a node pattern with a where inside the node pattern") {
    val p = patternForMatch(nodePat(None, predicates = Some(nodePredicate)))
    val expression = simpleCountExpression(p, maybeWhere = None)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n) WHERE n.prop = "test" } should succeed the semantic check
  test("count expression can contain a node pattern with a where outside the node pattern") {
    val p = patternForMatch(nodePat())
    val expression = simpleCountExpression(p, maybeWhere = Some(where(nodePredicate)))

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  // COUNT {(n)} should pass the semantic check
  test("count expression with a standalone node pattern is valid") {
    val p = patternForMatch(nodePat(Some("n")))
    val expression = simpleCountExpression(p, maybeWhere = None)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe empty
  }

  test("COUNT works for a regular query") {
    val expression = CountExpression(
      singleQuery(match_(relChain), return_(varFor("n").as("n")))
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("COUNT does not work for a regular query ending with FINISH") {
    val expression = CountExpression(
      singleQuery(match_(relChain), finish())
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(
      SemanticError("A Count Expression cannot contain a query ending with FINISH.", pos)
    )
  }

  test("COUNT does not work for an updating query") {
    val expression = CountExpression(
      singleQuery(create(nodePat(Some("n"))))
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(
      SemanticError("A Count Expression cannot contain any updates", pos)
    )
  }

  test("inner where with regular query using missing identifier reports error") {
    val expression = CountExpression(
      singleQuery(match_(relChain, where = Some(where(failingProperty))), return_(varFor("n").as("n")))
    )(pos, None, None)

    val result =
      SemanticExpressionCheck.simple(expression).run(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("COUNT with a regular query cannot reuse identifier with different type") {
    val expression = CountExpression(
      singleQuery(match_(relChain), return_(varFor("n").as("n")))
    )(pos, None, None)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get

    val result = SemanticExpressionCheck.simple(expression).run(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos)
    )
  }
}
