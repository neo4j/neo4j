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

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.CountExpression
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTNode

import scala.collection.compat.immutable.ArraySeq

class CountTest extends SemanticFunSuite {

  val n: NodePattern = NodePattern(Some(variable("n")), None, None, None)(pos)
  val x: NodePattern = expressions.NodePattern(Some(variable("x")), None, None, None)(pos)
  val r: RelationshipPattern = RelationshipPattern(None, None, None, None, None, SemanticDirection.OUTGOING)(pos)
  val label = Leaf(LabelName("Label")(pos))
  val pattern = RelationshipChain(n, r, x)(pos)

  val nodePredicate =
    Equals(Property(Variable("n")(pos), PropertyKeyName("prop")(pos))(pos), StringLiteral("test")(pos))(pos)
  val property: Property = Property(variable("x"), PropertyKeyName("prop")(pos))(pos)
  val failingProperty: Property = Property(variable("missing"), PropertyKeyName("prop")(pos))(pos)

  val nodeProperties: Expression =
    MapExpression(ArraySeq((PropertyKeyName("name")(pos), StringLiteral("test")(pos))))(pos)

  test("valid count expression passes semantic check") {
    val expression = CountExpression(pattern, Some(property))(pos, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("inner where using missing identifier reports error") {
    val expression = CountExpression(pattern, Some(failingProperty))(pos, Set.empty)

    val result = SemanticExpressionCheck.simple(expression)(SemanticState.clean)

    result.errors shouldBe Seq(SemanticError("Variable `missing` not defined", pos))
  }

  test("count expression cannot reuse identifier with different type") {
    val expression = CountExpression(pattern, Some(property))(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTBoolean).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe Seq(
      SemanticError("Type mismatch: n defined with conflicting type Boolean (expected Node)", pos)
    )
  }

  // COUNT { (n: Label) } should succeed the semantic check
  test("count expression can contain a node pattern with label") {
    val expression =
      CountExpression(n.copy(labelExpression = Some(label))(pos), optionalWhereExpression = None)(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n) } should succeed the semantic check even if n is not recorded in the outer scope
  test("can contain a non declared variable in a single node pattern") {
    val expression =
      CountExpression(n, optionalWhereExpression = None)(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("x"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }

  test("COUNT { () } should succeed the semantic check") {
    val expression =
      CountExpression(NodePattern(None, None, None, None)(pos), optionalWhereExpression = None)(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("x"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }

  test("COUNT { (:Label) } should succeed the semantic check") {
    val expression =
      CountExpression(NodePattern(None, Some(label), None, None)(pos), optionalWhereExpression = None)(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("x"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n{name:"test"}) } should succeed the semantic check
  test("count expression can contain node properties in a single node match") {
    val expression =
      CountExpression(n.copy(properties = Some(nodeProperties))(pos), optionalWhereExpression = None)(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n WHERE n.prop = "test") } should succeed the semantic check
  test("count expression can contain a node pattern with a where inside the node pattern") {
    val expression =
      CountExpression(n.copy(predicate = Some(nodePredicate))(pos), optionalWhereExpression = None)(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }

  // COUNT { (n) WHERE n.prop = "test" } should succeed the semantic check
  test("count expression can contain a node pattern with a where outside the node pattern") {
    val expression = CountExpression(n, optionalWhereExpression = Some(nodePredicate))(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }

  // COUNT {(n)} should pass the semantic check
  test("count expression with a standalone node pattern is valid") {
    val expression = CountExpression(n, optionalWhereExpression = None)(pos, Set.empty)

    val semanticState = SemanticState.clean.declareVariable(variable("n"), CTNode).right.get

    val result = SemanticExpressionCheck.simple(expression)(semanticState)

    result.errors shouldBe empty
  }
}
