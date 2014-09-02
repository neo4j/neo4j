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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.impl.quoteString
import symbols._
import java.net.URL
import Doc._
import Doc._

sealed trait Literal extends Expression {
  def value: AnyRef
}

sealed trait NumberLiteral extends Literal {
  def stringVal: String

  override def toDoc(pretty: FixedDocGenerator[ASTNode]): Doc = stringVal
}

sealed trait IntegerLiteral extends NumberLiteral {
  def value: java.lang.Long
}

sealed trait SignedIntegerLiteral extends IntegerLiteral
sealed trait UnsignedIntegerLiteral extends IntegerLiteral

sealed abstract class DecimalIntegerLiteral(stringVal: String) extends IntegerLiteral with SimpleTyping {
  lazy val value: java.lang.Long = java.lang.Long.parseLong(stringVal)

  protected def possibleTypes = CTInteger

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(!(try {
      value.isInstanceOf[Any]
    } catch {
      case e:java.lang.NumberFormatException => false
    })) {
      if (stringVal matches "^-?[1-9][0-9]*$")
        SemanticError("integer is too large", position)
      else
        SemanticError("invalid literal number", position)
    } chain super.semanticCheck(ctx)

}

case class SignedDecimalIntegerLiteral(stringVal: String)(val position: InputPosition) extends DecimalIntegerLiteral(stringVal) with SignedIntegerLiteral
case class UnsignedDecimalIntegerLiteral(stringVal: String)(val position: InputPosition) extends DecimalIntegerLiteral(stringVal) with UnsignedIntegerLiteral

sealed abstract class OctalIntegerLiteral(stringVal: String) extends IntegerLiteral with SimpleTyping {
  lazy val value: java.lang.Long = java.lang.Long.parseLong(stringVal, 8)

  protected def possibleTypes = CTInteger

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(!(try {
      value.isInstanceOf[Any]
    } catch {
      case e:java.lang.NumberFormatException => false
    })) {
      if (stringVal matches "^-?0[0-7]+$")
        SemanticError("integer is too large", position)
      else
        SemanticError("invalid literal number", position)
    } chain super.semanticCheck(ctx)
}

case class SignedOctalIntegerLiteral(stringVal: String)(val position: InputPosition) extends OctalIntegerLiteral(stringVal) with SignedIntegerLiteral

sealed abstract class HexIntegerLiteral(stringVal: String) extends IntegerLiteral with SimpleTyping {
  lazy val value: java.lang.Long =
    if (stringVal.charAt(0) == '-')
      -java.lang.Long.parseLong(stringVal.substring(3), 16)
    else
      java.lang.Long.parseLong(stringVal.substring(2), 16)

  protected def possibleTypes = CTInteger

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(!(try {
      value.isInstanceOf[Any]
    } catch {
      case e:java.lang.NumberFormatException => false
    })) {
      if (stringVal matches "^-?0x[0-9a-fA-F]+$")
        SemanticError("integer is too large", position)
      else
        SemanticError("invalid literal number", position)
    } chain super.semanticCheck(ctx)
}

case class SignedHexIntegerLiteral(stringVal: String)(val position: InputPosition) extends HexIntegerLiteral(stringVal) with SignedIntegerLiteral


sealed trait DoubleLiteral extends NumberLiteral {
  def value: java.lang.Double
}

case class DecimalDoubleLiteral(stringVal: String)(val position: InputPosition) extends DoubleLiteral with SimpleTyping {
  lazy val value: java.lang.Double = java.lang.Double.parseDouble(stringVal)

  protected def possibleTypes = CTFloat

  override def semanticCheck(ctx: SemanticContext): SemanticCheck =
    when(!(try {
      value.isInstanceOf[Any]
    } catch {
      case e:java.lang.NumberFormatException => false
    })) {
      SemanticError("invalid literal number", position)
    } ifOkChain when(value.isInfinite) {
      SemanticError("floating point number is too large", position)
    } chain super.semanticCheck(ctx)
}

case class StringLiteral(value: String)(val position: InputPosition) extends Literal with SimpleTyping {
  protected def possibleTypes = CTString

  override def toDoc(pretty: FixedDocGenerator[ASTNode]): Doc = quoteString(value)
}

case class Null()(val position: InputPosition) extends Literal with SimpleTyping {
  val value = null

  override def toDoc(pretty: FixedDocGenerator[ASTNode]): Doc = "NULL"

  protected def possibleTypes = CTAny.covariant
}

sealed trait BooleanLiteral extends Literal

case class True()(val position: InputPosition) extends BooleanLiteral with SimpleTyping {
  val value: java.lang.Boolean = true

  override def toDoc(pretty: FixedDocGenerator[ASTNode]): Doc = "true"

  protected def possibleTypes = CTBoolean
}

case class False()(val position: InputPosition) extends BooleanLiteral with SimpleTyping {
  val value: java.lang.Boolean = false

  override def toDoc(pretty: FixedDocGenerator[ASTNode]): Doc = "false"

  protected def possibleTypes = CTBoolean
}
