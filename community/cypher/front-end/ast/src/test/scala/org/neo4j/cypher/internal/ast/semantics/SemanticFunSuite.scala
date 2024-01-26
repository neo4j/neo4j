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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.DummyExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedHexIntegerLiteral
import org.neo4j.cypher.internal.expressions.SignedOctalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

trait SemanticFunSuite extends CypherFunSuite with SemanticAnalysisTooling with AstConstructionTestSupport {

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    SemanticExpressionCheck.semanticCheckFallback =
      (ctx, e) =>
        e match {
          case x: DummyExpression =>
            specifyType(x.possibleTypes, x)

          case x: ErrorExpression =>
            (s: SemanticState) => SemanticCheckResult.error(s, x.error)

          case x: CustomExpression =>
            x.semanticCheck(ctx, x)

          case x: Expression =>
            SemanticExpressionCheck.crashOnUnknownExpression(ctx, x)
        }
  }

  def literal(x: String) = StringLiteral(x)(pos, pos)
  def literal(x: Double) = DecimalDoubleLiteral(x.toString)(pos)
  def literal(x: Int) = SignedDecimalIntegerLiteral(x.toString)(pos)

  def unsignedDecimal(str: String) = UnsignedDecimalIntegerLiteral(str)(pos)
  def signedDecimal(str: String) = SignedDecimalIntegerLiteral(str)(pos)
  def decimalDouble(str: String) = DecimalDoubleLiteral(str)(pos)
  def signedOctal(str: String) = SignedOctalIntegerLiteral(str)(pos)
  def signedHex(str: String) = SignedHexIntegerLiteral(str)(pos)

  def variable(name: String): Variable = Variable(name)(pos)
  def propertyKeyName(name: String) = PropertyKeyName("prop")(pos)
  def property(variable: Variable, keyName: PropertyKeyName) = Property(variable, keyName)(pos)
}
