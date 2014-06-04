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
import java.net.URL

sealed trait Literal extends Expression {
  def value: AnyRef
}

sealed trait NumberLiteral extends Literal {
  def stringVal: String
}

sealed abstract class IntegerLiteral(stringVal: String) extends NumberLiteral with SimpleTyping {
  lazy val value: java.lang.Long = stringVal.toLong

  protected def possibleTypes = CTInteger

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(!(try {
      value.isInstanceOf[Any]
    } catch {
      case e:java.lang.NumberFormatException => false
    })) {
      SemanticError("integer is too large", position)
    } chain super.semanticCheck(ctx)
}

case class SignedIntegerLiteral(stringVal: String)(val position: InputPosition) extends IntegerLiteral(stringVal)
case class UnsignedIntegerLiteral(stringVal: String)(val position: InputPosition) extends IntegerLiteral(stringVal)

case class DoubleLiteral(stringVal: String)(val position: InputPosition) extends NumberLiteral with SimpleTyping {
  val value: java.lang.Double = stringVal.toDouble

  protected def possibleTypes = CTFloat

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(value.isInfinite) {
      SemanticError("floating point number is too large", position)
    } chain super.semanticCheck(ctx)
}

case class StringLiteral(value: String)(val position: InputPosition) extends Literal with SimpleTyping {
  protected def possibleTypes = CTString
}

case class Null()(val position: InputPosition) extends Literal with SimpleTyping {
  val value = null
  protected def possibleTypes = CTAny.covariant
}

sealed trait BooleanLiteral extends Literal

case class True()(val position: InputPosition) extends BooleanLiteral with SimpleTyping {
  val value: java.lang.Boolean = true
  protected def possibleTypes = CTBoolean
}

case class False()(val position: InputPosition) extends BooleanLiteral with SimpleTyping {
  val value: java.lang.Boolean = false
  protected def possibleTypes = CTBoolean
}
