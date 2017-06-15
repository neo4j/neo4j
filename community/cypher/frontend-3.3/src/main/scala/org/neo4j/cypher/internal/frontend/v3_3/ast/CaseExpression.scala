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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck}
import org.neo4j.cypher.internal.frontend.v3_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_3.symbols._

case class CaseExpression(expression: Option[Expression], alternatives: IndexedSeq[(Expression, Expression)], default: Option[Expression])(val position: InputPosition) extends Expression {


  lazy val possibleExpressions = alternatives.map(_._2) ++ default

  def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    val possibleTypes = possibleExpressions.unionOfTypes

    expression.semanticCheck(ctx) chain
    alternatives.flatMap { a => Seq(a._1, a._2) }.semanticCheck(ctx) chain
    default.semanticCheck(ctx) chain
    when (expression.isEmpty) {
      alternatives.map(_._1).expectType(CTBoolean.covariant)
    } chain this.specifyType(possibleTypes)
  }
}

object CaseExpression {
  def apply(expression: Option[Expression], alternatives: List[(Expression, Expression)], default: Option[Expression])(position: InputPosition):CaseExpression =
    CaseExpression(expression, alternatives.toIndexedSeq, default)(position)

}
