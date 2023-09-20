/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.ExactSize
import org.neo4j.cypher.internal.util.One
import org.neo4j.cypher.internal.util.ZeroOneOrMany
import org.neo4j.cypher.internal.util.symbols.ListType

sealed trait SeekableArgs {
  def expr: Expression
  def sizeHint: Option[Int]

  def dependencies: Set[LogicalVariable] = expr.dependencies

  def mapValues(f: Expression => Expression): SeekableArgs
  def asQueryExpression: QueryExpression[Expression]
}

case class SingleSeekableArg(expr: Expression) extends SeekableArgs {
  def sizeHint: Option[Int] = Some(1)

  override def mapValues(f: Expression => Expression): SingleSeekableArg = copy(f(expr))

  def asQueryExpression: SingleQueryExpression[Expression] = SingleQueryExpression(expr)
}

case class ManySeekableArgs(expr: Expression) extends SeekableArgs {

  val sizeHint: Option[Int] = expr match {
    case coll: ListLiteral => Some(coll.expressions.size)
    case param: Parameter  => param.sizeHint.toOption
    case _                 => None
  }

  override def mapValues(f: Expression => Expression): ManySeekableArgs = expr match {
    case coll: ListLiteral => copy(expr = coll.map(f))
    case _                 => copy(expr = f(expr))
  }

  def asQueryExpression: QueryExpression[Expression] = expr match {
    case coll: ListLiteral =>
      ZeroOneOrMany(coll.expressions) match {
        case One(value) => SingleQueryExpression(value)
        case _          => ManyQueryExpression(coll)
      }

    case p @ Parameter(_, ListType(_, _), ExactSize(1)) =>
      SingleQueryExpression(ContainerIndex(p, SignedDecimalIntegerLiteral("0")(p.position))(p.position))

    case _ =>
      ManyQueryExpression(expr)
  }
}

object WithSeekableArgs {

  def unapply(v: Any): Option[(Expression, SeekableArgs)] = v match {
    case In(lhs, rhs)     => Some(lhs -> ManySeekableArgs(rhs))
    case Equals(lhs, rhs) => Some(lhs -> SingleSeekableArg(rhs))
    case _                => None
  }
}
