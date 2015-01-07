/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._

case class CaseExpression(expression: Option[Expression], alternatives: Seq[(Expression, Expression)], default: Option[Expression])(val position: InputPosition) extends Expression {
  def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    val possibleTypes: TypeGenerator = (alternatives.map(_._2) ++ default).mergeUpTypes

    expression.semanticCheck(ctx) then
    alternatives.flatMap { a => Seq(a._1, a._2) }.semanticCheck(ctx) then
    default.semanticCheck(ctx) then
    when (expression.isEmpty) {
      alternatives.map(_._1).expectType(CTBoolean.covariant)
    } then this.specifyType(possibleTypes)
  }
}
