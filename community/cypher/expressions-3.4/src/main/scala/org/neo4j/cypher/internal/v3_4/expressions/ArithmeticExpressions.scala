/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.cypher.internal.util.v3_4.symbols._

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
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
  )

  override def canonicalOperatorSymbol = "-"
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
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
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
    TypeSignature(argumentTypes = Vector(CTFloat, CTFloat), outputType = CTFloat)
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
