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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1._
import ast._
import org.neo4j.cypher

object foldConstants extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] =
  try {
    bottomUp(instance).apply(that)
  } catch {
    case e:ArithmeticException => throw new cypher.ArithmeticException(e.getMessage, e)
  }
  private val instance: Rewriter = Rewriter.lift {
    case e@Add(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedIntegerLiteral((lhs.value + rhs.value).toString)(e.position)
    case e@Add(lhs: DoubleLiteral, rhs: SignedIntegerLiteral) =>
      DoubleLiteral((lhs.value + rhs.value).toString)(e.position)
    case e@Add(lhs: SignedIntegerLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value + rhs.value).toString)(e.position)
    case e@Add(lhs: DoubleLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value + rhs.value).toString)(e.position)

    case e@Subtract(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedIntegerLiteral((lhs.value - rhs.value).toString)(e.position)
    case e@Subtract(lhs: DoubleLiteral, rhs: SignedIntegerLiteral) =>
      DoubleLiteral((lhs.value - rhs.value).toString)(e.position)
    case e@Subtract(lhs: SignedIntegerLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value - rhs.value).toString)(e.position)
    case e@Subtract(lhs: DoubleLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value - rhs.value).toString)(e.position)

    case e@Multiply(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedIntegerLiteral((lhs.value * rhs.value).toString)(e.position)
    case e@Multiply(lhs: DoubleLiteral, rhs: SignedIntegerLiteral) =>
      DoubleLiteral((lhs.value * rhs.value).toString)(e.position)
    case e@Multiply(lhs: SignedIntegerLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value * rhs.value).toString)(e.position)
    case e@Multiply(lhs: DoubleLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value * rhs.value).toString)(e.position)

    case e@Multiply(lhs: NumberLiteral, rhs: NumberLiteral) =>
      e
    case e@Multiply(lhs: NumberLiteral, rhs) =>
      Multiply(rhs, lhs)(e.position).rewrite(bottomUp(this))
    case e@Multiply(lhs@Multiply(innerLhs, innerRhs: NumberLiteral), rhs: NumberLiteral) =>
      Multiply(Multiply(innerRhs, rhs)(lhs.position), innerLhs)(e.position).rewrite(bottomUp(this))
    case e@Multiply(lhs@Multiply(innerLhs: NumberLiteral, innerRhs), rhs: NumberLiteral) =>
      Multiply(Multiply(innerLhs, rhs)(lhs.position), innerRhs)(e.position).rewrite(bottomUp(this))

    case e@Divide(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedIntegerLiteral((lhs.value / rhs.value).toString)(e.position)
    case e@Divide(lhs: DoubleLiteral, rhs: SignedIntegerLiteral) =>
      DoubleLiteral((lhs.value / rhs.value).toString)(e.position)
    case e@Divide(lhs: SignedIntegerLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value / rhs.value).toString)(e.position)
    case e@Divide(lhs: DoubleLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value / rhs.value).toString)(e.position)

    case e@Modulo(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      SignedIntegerLiteral((lhs.value % rhs.value).toString)(e.position)
    case e@Modulo(lhs: DoubleLiteral, rhs: SignedIntegerLiteral) =>
      DoubleLiteral((lhs.value % rhs.value).toString)(e.position)
    case e@Modulo(lhs: SignedIntegerLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value % rhs.value).toString)(e.position)
    case e@Modulo(lhs: DoubleLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral((lhs.value % rhs.value).toString)(e.position)

    case e@Pow(lhs: SignedIntegerLiteral, rhs: SignedIntegerLiteral) =>
      DoubleLiteral(Math.pow(lhs.value.toDouble, rhs.value.toDouble).toString)(e.position)
    case e@Pow(lhs: DoubleLiteral, rhs: SignedIntegerLiteral) =>
      DoubleLiteral(Math.pow(lhs.value, rhs.value.toDouble).toString)(e.position)
    case e@Pow(lhs: SignedIntegerLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral(Math.pow(lhs.value.toDouble, rhs.value).toString)(e.position)
    case e@Pow(lhs: DoubleLiteral, rhs: DoubleLiteral) =>
      DoubleLiteral(Math.pow(lhs.value, rhs.value).toString)(e.position)

    case e: UnaryAdd =>
      e.rhs

    case e@UnarySubtract(rhs: SignedIntegerLiteral) =>
      SignedIntegerLiteral((-rhs.value).toString)(e.position)
    case e: UnarySubtract =>
      Subtract(SignedIntegerLiteral("0")(e.position), e.rhs)(e.position)
  }
}
