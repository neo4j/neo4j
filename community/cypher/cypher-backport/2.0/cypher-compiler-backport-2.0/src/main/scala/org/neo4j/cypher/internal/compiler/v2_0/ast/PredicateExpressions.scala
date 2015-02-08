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

case class And(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Or(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Xor(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(Vector(CTBoolean, CTBoolean), outputType = CTBoolean)
  )
}

case class Not(rhs: Expression)(val position: InputPosition) extends Expression with PrefixFunctionTyping {
  val signatures = Vector(
    Signature(Vector(CTBoolean), outputType = CTBoolean)
  )
}

case class Equals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )
}

case class NotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny, CTAny), outputType = CTBoolean)
  )
}

case class InvalidNotEquals(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression {
  val arguments = Vector(lhs, rhs)
  def semanticCheck(ctx: SemanticContext): SemanticCheck =
    SemanticError("Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing)", position)
}

case class RegexMatch(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}

case class In(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression {
  def semanticCheck(ctx: ast.Expression.SemanticContext): SemanticCheck =
    lhs.semanticCheck(ctx) then
    lhs.expectType(CTAny.covariant) then
    rhs.semanticCheck(ctx) then
    rhs.expectType(lhs.types(_).wrapInCollection) then
    specifyType(CTBoolean)
}

case class IsNull(lhs: Expression)(val position: InputPosition) extends Expression with PostfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )
}

case class IsNotNull(lhs: Expression)(val position: InputPosition) extends Expression with PostfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTAny), outputType = CTBoolean)
  )
}

case class LessThan(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}

case class LessThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}

case class GreaterThan(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}

case class GreaterThanOrEqual(lhs: Expression, rhs: Expression)(val position: InputPosition) extends Expression with InfixFunctionTyping {
  val signatures = Vector(
    Signature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTString, CTString), outputType = CTBoolean)
  )
}
