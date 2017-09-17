/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.apa.v3_4.DummyPosition
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_4.ast.{CustomExpression, DummyExpression, ErrorExpression}
import org.neo4j.cypher.internal.v3_4.expressions._

class SemanticFunSuite extends CypherFunSuite with SemanticAnalysisTooling {

  override def initTest(): Unit = {
    SemanticExpressionCheck.semanticCheckFallback =
      (ctx, e) =>
        e match {
          case x:DummyExpression =>
            specifyType(x.possibleTypes, x)

          case x:ErrorExpression =>
            s => SemanticCheckResult.error(s, x.error)

          case x:CustomExpression =>
            x.semanticCheck(ctx, x)

          case x:Expression =>
            SemanticExpressionCheck.crashOnUnknownExpression(ctx, x)
        }
  }

  val pos = DummyPosition(0)

  def literal(x:String) = StringLiteral(x)(pos)
  def literal(x:Double) = DecimalDoubleLiteral(x.toString)(pos)
  def literal(x:Int) = SignedDecimalIntegerLiteral(x.toString)(pos)

  def unsignedDecimal(str:String) = UnsignedDecimalIntegerLiteral(str)(pos)
  def signedDecimal(str:String) = SignedDecimalIntegerLiteral(str)(pos)
  def decimalDouble(str:String) = DecimalDoubleLiteral(str)(pos)
  def signedOctal(str:String) = SignedOctalIntegerLiteral(str)(pos)

  def variable(name: String): Variable = Variable(name)(pos)
  def propertyKeyName(name: String) = PropertyKeyName("prop")(pos)
  def property(variable:Variable, keyName:PropertyKeyName) = Property(variable, keyName)(pos)
}
