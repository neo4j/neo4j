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

import java.util

import scala.language.postfixOps
import scala.util.matching.Regex

sealed trait Literal extends Expression {
  def value: AnyRef
  def asCanonicalStringVal: String
  def asSensitiveLiteral: Literal with SensitiveLiteral
  override def isConstantForQuery: Boolean = true
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
  lazy val integerMatcher: Regex = """-?\d+((_\d+)?)*""" r

  lazy val value: java.lang.Long = stringVal match {
    case integerMatcher(_*) => java.lang.Long.parseLong(stringVal.toList.filter(c => c != '_').mkString)
    // pass along to keep the same error message
    case _ => java.lang.Long.parseLong(stringVal)
  }
}

case class SignedDecimalIntegerLiteral(stringVal: String)(val position: InputPosition)
    extends DecimalIntegerLiteral(stringVal) with SignedIntegerLiteral {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new SignedDecimalIntegerLiteral(stringVal)(position) with SensitiveLiteral {
      override def literalLength: Int = stringVal.length
    }
}

case class UnsignedDecimalIntegerLiteral(stringVal: String)(val position: InputPosition)
    extends DecimalIntegerLiteral(stringVal) with UnsignedIntegerLiteral {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new UnsignedDecimalIntegerLiteral(stringVal)(position) with SensitiveLiteral {
      override def literalLength: Int = stringVal.length
    }
}

sealed abstract class OctalIntegerLiteral(stringVal: String) extends IntegerLiteral {
  lazy val octalMatcher: Regex = """-?0o(_?[0-7]+)+""" r

  lazy val value: java.lang.Long = stringVal match {
    case octalMatcher(_*) =>
      java.lang.Long.decode(stringVal.toList.filter(c => c != '_').filter(c => c != 'o').mkString)
    case _ => java.lang.Long.decode(stringVal.toList.filter(c => c != 'o').mkString)
  }
}

case class SignedOctalIntegerLiteral(stringVal: String)(val position: InputPosition)
    extends OctalIntegerLiteral(stringVal) with SignedIntegerLiteral {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new SignedOctalIntegerLiteral(stringVal)(position) with SensitiveLiteral {
      override def literalLength: Int = stringVal.length
    }
}

sealed abstract class HexIntegerLiteral(stringVal: String) extends IntegerLiteral {
  lazy val hexMatcher: Regex = """-?0x(_?[0-9a-fA-F]+)+""" r

  lazy val value: java.lang.Long = stringVal match {
    case hexMatcher(_*) => java.lang.Long.decode(stringVal.toList.filter(c => c != '_').mkString)
    // pass along to keep the same error message
    case _ => java.lang.Long.decode(stringVal)
  }
}

case class SignedHexIntegerLiteral(stringVal: String)(val position: InputPosition) extends HexIntegerLiteral(stringVal)
    with SignedIntegerLiteral {

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new SignedHexIntegerLiteral(stringVal)(position) with SensitiveLiteral {
      override def literalLength: Int = stringVal.length
    }
}

sealed trait DoubleLiteral extends NumberLiteral {
  def value: java.lang.Double
}

case class DecimalDoubleLiteral(stringVal: String)(val position: InputPosition) extends DoubleLiteral {
  lazy val doubleMatcher: Regex = """-?(\d+((_\d+)?)*)?(\.\d+((_\d+)?)*)?([eE]([+-])?\d+((_\d+)?)*)?""" r

  lazy val value: java.lang.Double = stringVal match {
    case doubleMatcher(_*) => java.lang.Double.parseDouble(stringVal.replace("_", ""))
    // pass along to keep the same error message
    case _ => java.lang.Double.parseDouble(stringVal)
  }

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new DecimalDoubleLiteral(stringVal)(position) with SensitiveLiteral {
      override def literalLength: Int = stringVal.length
    }
}

case class StringLiteral(value: String)(val position: InputPosition, val endPosition: InputPosition) extends Literal {

  override def asCanonicalStringVal: String = value

  override def dup(children: Seq[AnyRef]): this.type = {
    StringLiteral(children.head.asInstanceOf[String])(position, endPosition).asInstanceOf[this.type]
  }

  override def asSensitiveLiteral: Literal with SensitiveLiteral =
    new StringLiteral(value)(position, endPosition) with SensitiveLiteral {
      override def literalLength: Int = endPosition.offset - position.offset + 1
    }
}

final case class SensitiveStringLiteral(value: Array[Byte])(val position: InputPosition, val endPosition: InputPosition)
    extends Expression
    with SensitiveLiteral {

  override def dup(children: Seq[AnyRef]): this.type = {
    SensitiveStringLiteral(children.head.asInstanceOf[Array[Byte]])(position, endPosition).asInstanceOf[this.type]
  }

  override def equals(obj: Any): Boolean = obj match {
    case o: SensitiveStringLiteral => util.Arrays.equals(o.value, value)
    case _                         => false
  }

  override def hashCode(): Int = util.Arrays.hashCode(value)

  override def literalLength: Int = endPosition.offset - position.offset + 1

  override def isConstantForQuery: Boolean = true
}

trait SensitiveLiteral {
  val position: InputPosition

  /**
   * Number of characters of the literal including quotes
   */
  def literalLength: Int
}

case class Null()(val position: InputPosition) extends Literal {
  val value = null

  override def asCanonicalStringVal = "NULL"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new Null()(position) with SensitiveLiteral {
    override def literalLength: Int = 4
  }
}

object Null {
  val NULL: Null = Null()(InputPosition.NONE)
}

case class Infinity()(val position: InputPosition) extends Literal {
  val value: java.lang.Double = Double.PositiveInfinity

  override def asCanonicalStringVal = "Infinity"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new Infinity()(position) with SensitiveLiteral {
    override def literalLength: Int = 8
  }
}

case class NaN()(val position: InputPosition) extends Literal {
  val value: java.lang.Double = Double.NaN
  override def asCanonicalStringVal = "NaN"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new NaN()(position) with SensitiveLiteral {
    override def literalLength: Int = 3
  }
}

sealed trait BooleanLiteral extends Literal

case class True()(val position: InputPosition) extends BooleanLiteral {
  val value: java.lang.Boolean = true

  override def asCanonicalStringVal = "true"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new True()(position) with SensitiveLiteral {
    override def literalLength: Int = 4
  }
}

case class False()(val position: InputPosition) extends BooleanLiteral {
  val value: java.lang.Boolean = false
  override def asCanonicalStringVal = "false"

  override def asSensitiveLiteral: Literal with SensitiveLiteral = new False()(position) with SensitiveLiteral {
    override def literalLength: Int = 5
  }
}
