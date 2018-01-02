/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.{ManyQueryExpression, QueryExpression, SingleQueryExpression}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{ManySeekArgs, SeekArgs}

object Seek {
  def unapply(v: Any) = v match {
    case Equals(lhs, rhs) => Some(lhs -> SingleSeekRhs(rhs))
    case In(lhs, rhs) => Some(lhs -> MultiSeekRhs(rhs))
    case _ => None
  }
}

trait SeekRhs {
  def expr: Expression
  def sizeHint: Option[Int]

  def map(f: Expression => Expression): SeekRhs

  def asQueryExpression: QueryExpression[Expression]
  def asCommandSeekArgs: SeekArgs
}

case class SingleSeekRhs(expr: Expression) extends SeekRhs {
  def sizeHint = None

  override def map(f: Expression => Expression) = copy(f(expr))

  def asQueryExpression: SingleQueryExpression[Expression] =
    SingleQueryExpression(expr)

  def asCommandSeekArgs: SeekArgs =
    ManySeekArgs(toCommandExpression(Collection(Seq(expr))(expr.position)))
}

case class MultiSeekRhs(expr: Expression) extends SeekRhs {
  val sizeHint = expr match {
    case coll: Collection => Some(coll.expressions.size)
    case _                => None
  }

  override def map(f: Expression => Expression) = expr match {
    case coll: Collection => copy(expr = coll.map(f))
    case _ => copy(expr = f(expr))
  }

  def asQueryExpression: ManyQueryExpression[Expression] =
    ManyQueryExpression(expr)

  def asCommandSeekArgs: SeekArgs =
    ManySeekArgs(toCommandExpression(expr))
}
