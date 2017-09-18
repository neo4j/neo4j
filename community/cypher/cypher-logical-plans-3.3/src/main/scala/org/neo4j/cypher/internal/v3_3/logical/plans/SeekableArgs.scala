/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.v3_3.logical.plans

import org.neo4j.cypher.internal.frontend.v3_4.ast._

sealed trait SeekableArgs {
  def expr: Expression
  def sizeHint: Option[Int]

  def dependencies: Set[Variable] = expr.dependencies

  def mapValues(f: Expression => Expression): SeekableArgs
  def asQueryExpression: QueryExpression[Expression]
}

case class SingleSeekableArg(expr: Expression) extends SeekableArgs {
  def sizeHint = Some(1)

  override def mapValues(f: Expression => Expression): SingleSeekableArg = copy(f(expr))

  def asQueryExpression: SingleQueryExpression[Expression] = SingleQueryExpression(expr)
}

case class ManySeekableArgs(expr: Expression) extends SeekableArgs {
  val sizeHint: Option[Int] = expr match {
    case coll: ListLiteral => Some(coll.expressions.size)
    case _ => None
  }

  override def mapValues(f: Expression => Expression): ManySeekableArgs = expr match {
    case coll: ListLiteral => copy(expr = coll.map(f))
    case _ => copy(expr = f(expr))
  }

  def asQueryExpression: QueryExpression[Expression] = expr match {
    case coll: ListLiteral =>
      ZeroOneOrMany(coll.expressions) match {
        case One(value) => SingleQueryExpression(value)
        case _ => ManyQueryExpression(coll)
      }

    case _ =>
      ManyQueryExpression(expr)
  }
}

object WithSeekableArgs {
  def unapply(v: Any) = v match {
    case In(lhs, rhs) => Some(lhs -> ManySeekableArgs(rhs))
    case Equals(lhs, rhs) => Some(lhs -> SingleSeekableArg(rhs))
    case _ => None
  }
}
