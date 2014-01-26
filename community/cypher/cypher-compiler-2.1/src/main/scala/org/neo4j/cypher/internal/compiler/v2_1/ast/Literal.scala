/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_1._
import symbols._

trait Literal extends Expression {
  def value: Any
}

sealed abstract class IntegerLiteral(stringVal: String) extends Literal with SimpleTypedExpression {
  lazy val value = stringVal.toLong

  protected def possibleTypes = CTInteger

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(!(try {
      value.isInstanceOf[Any]
    } catch {
      case e:java.lang.NumberFormatException => false
    })) {
      SemanticError("integer is too large", token)
    } then super.semanticCheck(ctx)
}

case class SignedIntegerLiteral(stringVal: String)(val token: InputToken) extends IntegerLiteral(stringVal)
case class UnsignedIntegerLiteral(stringVal: String)(val token: InputToken) extends IntegerLiteral(stringVal)


case class DoubleLiteral(stringVal: String)(val token: InputToken) extends Literal with SimpleTypedExpression {
  val value = stringVal.toDouble

  protected def possibleTypes = CTDouble

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(value.isInfinite) {
      SemanticError("floating point number is too large", token)
    } then super.semanticCheck(ctx)
}


case class StringLiteral(value: String)(val token: InputToken) extends Literal with SimpleTypedExpression {
  protected def possibleTypes = CTString
}


case class Range(lower: Option[UnsignedIntegerLiteral], upper: Option[UnsignedIntegerLiteral])(val token: InputToken) extends AstNode {
  def isSingleLength = lower.isDefined && upper.isDefined && lower.get.value == 1 && upper.get.value == 1
}
