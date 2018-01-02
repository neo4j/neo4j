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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, SemanticCheck}

case class CaseExpression(expression: Option[Expression], alternatives: Seq[(Expression, Expression)], default: Option[Expression])(val position: InputPosition) extends Expression {
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
