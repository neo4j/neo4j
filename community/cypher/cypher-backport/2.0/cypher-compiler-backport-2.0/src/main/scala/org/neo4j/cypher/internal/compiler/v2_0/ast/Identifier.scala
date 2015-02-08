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

case class Identifier(name: String)(val position: InputPosition) extends Expression {
  // check the identifier is defined and, if not, define it so that later errors are suppressed
  def semanticCheck(ctx: SemanticContext) = s => this.ensureDefined()(s) match {
    case Right(ss) => SemanticCheckResult.success(ss)
    case Left(error) => SemanticCheckResult.error(declare(CTAny.covariant)(s).right.get, error)
  }

  // double-dispatch helpers
  def declare(possibleTypes: TypeSpec) =
    (_: SemanticState).declareIdentifier(this, possibleTypes)
  def declare(typeGen: SemanticState => TypeSpec) =
    (s: SemanticState) => s.declareIdentifier(this, typeGen(s))
  def implicitDeclaration(possibleType: CypherType) =
    (_: SemanticState).implicitIdentifier(this, possibleType)
  def ensureDefined() =
    (_: SemanticState).ensureIdentifierDefined(this)
}
