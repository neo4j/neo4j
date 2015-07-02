/*
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
package org.neo4j.cypher.internal.compiler.v2_3.ast

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols._

sealed trait Extremum extends Expression {
  private val head = expressions.headOption.getOrElse(throw new InternalException("Cannot construct MaxValue for an empty sequence"))

  def expressions: Seq[Expression]
  def position = head.position

  def semanticCheck(ctx: SemanticContext): SemanticCheck = {
    val expressionChecks = expressions.map(_.expectType(CTNumber.covariant))
    val chainedCheck = expressionChecks.reduceOption(_ chain _).getOrElse(SemanticCheckResult.success)
    // TODO: Compute most specific upper type bound
    chainedCheck chain this.specifyType(CTNumber.covariant)
  }
}

final case class Minimum(expressions: Seq[Expression]) extends Extremum
final case class Maximum(expressions: Seq[Expression]) extends Extremum
