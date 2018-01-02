/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.InputPosition

sealed trait Literal extends Expression {
  def value: AnyRef
  def asCanonicalStringVal: String
}

sealed trait NumberLiteral extends Literal {
  def stringVal: String
  override def asCanonicalStringVal: String = stringVal
}

sealed trait IntegerLiteral extends NumberLiteral {
  def value: java.lang.Long
}

sealed trait SignedIntegerLiteral extends IntegerLiteral
sealed trait UnsignedIntegerLiteral extends IntegerLiteral

sealed abstract class DecimalIntegerLiteral(stringVal: String) extends IntegerLiteral {
  lazy val value: java.lang.Long = java.lang.Long.parseLong(stringVal)
}

case class SignedDecimalIntegerLiteral(stringVal: String)(val position: InputPosition) extends DecimalIntegerLiteral(stringVal) with SignedIntegerLiteral
case class UnsignedDecimalIntegerLiteral(stringVal: String)(val position: InputPosition) extends DecimalIntegerLiteral(stringVal) with UnsignedIntegerLiteral

sealed abstract class OctalIntegerLiteral(stringVal: String) extends IntegerLiteral {
  lazy val value: java.lang.Long = java.lang.Long.parseLong(stringVal, 8)
}

case class SignedOctalIntegerLiteral(stringVal: String)(val position: InputPosition) extends OctalIntegerLiteral(stringVal) with SignedIntegerLiteral

sealed abstract class HexIntegerLiteral(stringVal: String) extends IntegerLiteral {
  lazy val value: java.lang.Long =
    if (stringVal.charAt(0) == '-')
      -java.lang.Long.parseLong(stringVal.substring(3), 16)
    else
      java.lang.Long.parseLong(stringVal.substring(2), 16)
}

case class SignedHexIntegerLiteral(stringVal: String)(val position: InputPosition) extends HexIntegerLiteral(stringVal) with SignedIntegerLiteral


sealed trait DoubleLiteral extends NumberLiteral {
  def value: java.lang.Double
}

case class DecimalDoubleLiteral(stringVal: String)(val position: InputPosition) extends DoubleLiteral {
  lazy val value: java.lang.Double = java.lang.Double.parseDouble(stringVal)
}

case class StringLiteral(value: String)(val position: InputPosition) extends Literal {
  override def asCanonicalStringVal = value
}

case class Null()(val position: InputPosition) extends Literal {
  val value = null

  override def asCanonicalStringVal = "NULL"
}

sealed trait BooleanLiteral extends Literal

case class True()(val position: InputPosition) extends BooleanLiteral {
  val value: java.lang.Boolean = true

  override def asCanonicalStringVal = "true"
}

case class False()(val position: InputPosition) extends BooleanLiteral {
  val value: java.lang.Boolean = false

  override def asCanonicalStringVal = "false"
}
