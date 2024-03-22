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
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime

case class Add(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends Expression with BinaryOperatorExpression {

  override def canonicalOperatorSymbol = "+"
}

case class UnaryAdd(rhs: Expression)(val position: InputPosition)
    extends Expression with LeftUnaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTInteger), outputType = CTInteger),
    TypeSignature(argumentTypes = Vector(CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "+"
}

case class Subtract(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends Expression with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    TypeSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTInteger), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTDuration, CTDuration), outputType = CTDuration),
    TypeSignature(argumentTypes = Vector(CTLocalTime, CTDuration), outputType = CTLocalTime),
    TypeSignature(argumentTypes = Vector(CTTime, CTDuration), outputType = CTTime),
    TypeSignature(argumentTypes = Vector(CTDate, CTDuration), outputType = CTDate),
    TypeSignature(argumentTypes = Vector(CTLocalDateTime, CTDuration), outputType = CTLocalDateTime),
    TypeSignature(argumentTypes = Vector(CTDateTime, CTDuration), outputType = CTDateTime)
  )

  override def canonicalOperatorSymbol = "-"
}

case class Concatenate(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends Expression with BinaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTString, CTString), outputType = CTString),
    TypeSignature(argumentTypes = Vector(CTList(CTAny), CTList(CTAny)), outputType = CTList(CTAny))
  )

  override def canonicalOperatorSymbol = "||"
}

case class UnarySubtract(rhs: Expression)(val position: InputPosition)
    extends Expression with LeftUnaryOperatorExpression {

  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTInteger), outputType = CTInteger),
    TypeSignature(argumentTypes = Vector(CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "-"
}

case class Multiply(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends Expression with BinaryOperatorExpression {

  // 1 * 1 => 1
  // 1 * 1.1 => 1.1
  // 1.1 * 1 => 1.1
  // 1.1 * 1.1 => 1.21
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    TypeSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTInteger), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTDuration, CTFloat), outputType = CTDuration),
    TypeSignature(argumentTypes = Vector(CTDuration, CTInteger), outputType = CTDuration),
    TypeSignature(argumentTypes = Vector(CTFloat, CTDuration), outputType = CTDuration),
    TypeSignature(argumentTypes = Vector(CTInteger, CTDuration), outputType = CTDuration)
  )

  override def canonicalOperatorSymbol = "*"
}

case class Divide(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends Expression with BinaryOperatorExpression {

  // 1 / 1 => 1
  // 1 / 1.1 => 0.909
  // 1.1 / 1 => 1.1
  // 1.1 / 1.1 => 1.0
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    TypeSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTInteger), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTDuration, CTFloat), outputType = CTDuration),
    TypeSignature(argumentTypes = Vector(CTDuration, CTInteger), outputType = CTDuration)
  )

  override def canonicalOperatorSymbol = "/"
}

case class Modulo(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends Expression with BinaryOperatorExpression {

  // 1 % 1 => 0
  // 1 % 1.1 => 1.0
  // 1.1 % 1 => 0.1
  // 1.1 % 1.1 => 0.0
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTInteger, CTInteger), outputType = CTInteger),
    TypeSignature(argumentTypes = Vector(CTInteger, CTFloat), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTInteger), outputType = CTFloat),
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "%"
}

case class Pow(lhs: Expression, rhs: Expression)(val position: InputPosition)
    extends Expression with BinaryOperatorExpression {

  // 1 ^ 1 => 1.1
  // 1 ^ 1.1 => 1.0
  // 1.1 ^ 1 => 1.1
  // 1.1 ^ 1.1 => 1.1105
  override val signatures = Vector(
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "^"
}
