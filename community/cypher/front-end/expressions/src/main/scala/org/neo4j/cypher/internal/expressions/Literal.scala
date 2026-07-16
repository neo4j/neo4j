/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.expressions

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.LazyVal

import java.util

import scala.language.postfixOps
import scala.util.matching.Regex

sealed trait Literal extends Expression {
  def value: AnyRef
  def asCanonicalStringVal: String
  def position: InputPosition.Range
  def asSensitiveLiteral: Literal with SensitiveLiteral
  override def isConstantForQuery: Boolean = true
}

sealed trait NumberLiteral extends Literal {
  def stringVal: String
  override def value: java.lang.Number
  override def asCanonicalStringVal: String = stringVal
}

sealed trait IntegerLiteral extends NumberLiteral {
  def value: java.lang.Long
}

sealed trait SignedIntegerLiteral extends IntegerLiteral
sealed trait UnsignedIntegerLiteral extends IntegerLiteral

case class SignedDecimalIntegerLiteral(stringVal: String)(override val position: InputPosition.Range)
    extends IntegerLiteral with SignedIntegerLiteral with StringDecimalInteger {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new SignedDecimalIntegerLiteral(stringVal)(position) with SensitiveLiteral
}

case class UnsignedDecimalIntegerLiteral(
  stringVal: String
)(override val position: InputPosition.Range)
    extends IntegerLiteral with UnsignedIntegerLiteral with StringDecimalInteger {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new UnsignedDecimalIntegerLiteral(stringVal)(position) with SensitiveLiteral
}

sealed abstract class OctalIntegerLiteral(stringVal: String) extends IntegerLiteral {
  override def value: java.lang.Long = lazyValue.value
  private val lazyValue: LazyVal[java.lang.Long] = LazyVal(OctalIntegerLiteral.octalToLong(stringVal))
}

object OctalIntegerLiteral {
  final private val octalMatcherWithUnderscore: Regex = """-?0o(_?[0-7]+)+""" r
  final private val octalMatcherWithoutUnderscore: Regex = """-?0o?[0-7]+""" r

  def octalToLong(stringValue: String): java.lang.Long = {
    if (stringValue.contains("_")) {
      if (!octalMatcherWithUnderscore.matches(stringValue)) {
        throw new NumberFormatException(s"Invalid octal integer literal: $stringValue")
      }
      java.lang.Long.decode(stringValue.replace("_", "").replace("o", ""))
    } else {
      // Requires at least one octal digit, otherwise e.g. "0o" would incorrectly decode as 0.
      if (!octalMatcherWithoutUnderscore.matches(stringValue)) {
        throw new NumberFormatException(s"Invalid octal integer literal: $stringValue")
      }
      java.lang.Long.decode(stringValue.replace("o", ""))
    }
  }
}

case class SignedOctalIntegerLiteral(stringVal: String)(override val position: InputPosition.Range)
    extends OctalIntegerLiteral(stringVal) with SignedIntegerLiteral {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new SignedOctalIntegerLiteral(stringVal)(position) with SensitiveLiteral
}

sealed abstract class HexIntegerLiteral(stringVal: String) extends IntegerLiteral {
  override def value: java.lang.Long = lazyValue.value
  private val lazyValue: LazyVal[java.lang.Long] = LazyVal(HexIntegerLiteral.hexStringToLong(stringVal))
}

object HexIntegerLiteral {
  final private val hexMatcher: Regex = """-?0x(_?[0-9a-fA-F]+)+""" r

  def hexStringToLong(stringValue: String): java.lang.Long = {
    if (stringValue.contains("_") && hexMatcher.matches(stringValue)) {
      java.lang.Long.decode(stringValue.replace("_", ""))
    } else {
      java.lang.Long.decode(stringValue)
    }
  }
}

case class SignedHexIntegerLiteral(stringVal: String)(override val position: InputPosition.Range)
    extends HexIntegerLiteral(stringVal)
    with SignedIntegerLiteral {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new SignedHexIntegerLiteral(stringVal)(position) with SensitiveLiteral
}

sealed trait DoubleLiteral extends NumberLiteral {
  def value: java.lang.Double
}

case class DecimalDoubleLiteral(stringVal: String)(override val position: InputPosition.Range) extends DoubleLiteral {
  override def value: java.lang.Double = lazyValue.value
  private val lazyValue: LazyVal[java.lang.Double] = LazyVal(DecimalDoubleLiteral.stringToDouble(stringVal))

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new DecimalDoubleLiteral(stringVal)(position) with SensitiveLiteral
}

object DecimalDoubleLiteral {
  private val doubleMatcher: Regex = """-?(\d+((_\d+)?)*)?(\.\d+((_\d+)?)*)?([eE]([+-])?\d+((_\d+)?)*)?""" r

  def stringToDouble(stringValue: String): java.lang.Double = {
    if (stringValue.contains("_") && doubleMatcher.matches(stringValue)) {
      java.lang.Double.parseDouble(stringValue.replace("_", ""))
    } else {
      java.lang.Double.parseDouble(stringValue)
    }
  }
}

// Note, the inputLength of the input position is not always equal to value.length because of escape characters.
case class StringLiteral(value: String)(val position: InputPosition.Range) extends Literal {

  override def asCanonicalStringVal: String = value

  override def dup(children: Seq[AnyRef]): this.type = {
    StringLiteral(children.head.asInstanceOf[String])(position).asInstanceOf[this.type]
  }

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new StringLiteral(value)(position) with SensitiveLiteral
}

final case class SensitiveStringLiteral(value: Array[Byte])(val position: InputPosition.Range)
    extends Expression
    with SensitiveLiteral {

  override def dup(children: Seq[AnyRef]): this.type = {
    SensitiveStringLiteral(children.head.asInstanceOf[Array[Byte]])(position).asInstanceOf[this.type]
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: SensitiveStringLiteral => util.Arrays.equals(o.value, value)
    case _                         => false
  }

  override def hashCode(): Int = util.Arrays.hashCode(value)

  override def isConstantForQuery: Boolean = true
}

trait SensitiveLiteral {
  val position: InputPosition.Range

  /**
   * Number of characters of the literal including quotes
   */
  final def literalLength: Int = position.inputLength
}

case class Null()(override val position: InputPosition.Range) extends Literal {
  override val value: AnyRef = null

  override def asCanonicalStringVal = "NULL"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new Null()(position) with SensitiveLiteral
}

object Null {
  val NULL: Null = Null()(InputPosition.NONE)
}

case class Infinity()(override val position: InputPosition.Range) extends Literal {
  val value: java.lang.Double = Double.PositiveInfinity

  override def asCanonicalStringVal = "Infinity"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new Infinity()(position) with SensitiveLiteral
}

case class NaN()(override val position: InputPosition.Range) extends Literal {
  val value: java.lang.Double = Double.NaN
  override def asCanonicalStringVal = "NaN"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new NaN()(position) with SensitiveLiteral
}

sealed trait BooleanLiteral extends Literal

case class True()(override val position: InputPosition.Range) extends BooleanLiteral {
  val value: java.lang.Boolean = true

  override def asCanonicalStringVal = "true"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new True()(position) with SensitiveLiteral
}

case class False()(override val position: InputPosition.Range) extends BooleanLiteral {
  val value: java.lang.Boolean = false
  override def asCanonicalStringVal = "false"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new False()(position) with SensitiveLiteral
}

case class ObfuscatedLiteral()(override val position: InputPosition.Range) extends Expression {
  val value: java.lang.Boolean = false
  override def asCanonicalStringVal = "******"

  override def isConstantForQuery: Boolean = true
}
