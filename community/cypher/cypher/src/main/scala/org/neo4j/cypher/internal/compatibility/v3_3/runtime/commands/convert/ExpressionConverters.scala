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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{predicates, expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.{ManySeekArgs, SeekArgs, SingleSeekArg}
import org.neo4j.cypher.internal.compiler.v3_3.helpers.{Many, One, Zero, ZeroOneOrMany}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{ManySeekableArgs, SeekableArgs, SingleSeekableArg}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticDirection, ast}
import org.neo4j.graphdb.Direction

trait ExpressionConverters {

  self =>

  def toCommandExpression(expression: ast.Expression): CommandExpression =
    toCommandExpression(expression, self)

  def toCommandProjectedPath(e: ast.PathExpression): ProjectedPath

  def toCommandPredicate(in: ast.Expression): Predicate = in match {
    case e: ast.PatternExpression => predicates.NonEmpty(toCommandExpression(e))
    case e: ast.FilterExpression => predicates.NonEmpty(toCommandExpression(e))
    case e: ast.ExtractExpression => predicates.NonEmpty(toCommandExpression(e))
    case e: ast.ListComprehension => predicates.NonEmpty(toCommandExpression(e))
    case e => toCommandExpression(e) match {
      case c: Predicate => c
      case c => predicates.CoercedPredicate(c)
    }
  }

  def toCommandSeekArgs(seek: SeekableArgs): SeekArgs = seek match {
    case SingleSeekableArg(expr) => SingleSeekArg(toCommandExpression(expr))
    case ManySeekableArgs(expr) => expr match {
      case coll: ListLiteral =>
        ZeroOneOrMany(coll.expressions) match {
          case Zero => SeekArgs.empty
          case One(value) => SingleSeekArg(toCommandExpression(value))
          case Many(_) => ManySeekArgs(toCommandExpression(coll))
        }

      case _ =>
        ManySeekArgs(toCommandExpression(expr))
    }
  }

  protected def toCommandExpression(expression: ast.Expression, self: ExpressionConverters): CommandExpression
}

object DirectionConverter {
  def toGraphDb(dir: SemanticDirection): Direction = dir match {
    case SemanticDirection.INCOMING => Direction.INCOMING
    case SemanticDirection.OUTGOING => Direction.OUTGOING
    case SemanticDirection.BOTH => Direction.BOTH
  }
}
