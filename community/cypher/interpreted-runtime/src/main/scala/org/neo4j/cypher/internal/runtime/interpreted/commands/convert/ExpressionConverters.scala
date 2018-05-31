/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.convert

import org.opencypher.v9_0.util._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ProjectedPath._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{ManySeekArgs, SeekArgs, SingleSeekArg}
import org.opencypher.v9_0.{expressions => ast}
import org.opencypher.v9_0.expressions.{SemanticDirection, Variable}
import org.neo4j.cypher.internal.v3_5.logical.plans.{ManySeekableArgs, SeekableArgs, SingleSeekableArg}
import org.neo4j.graphdb.Direction

trait ExpressionConverter {
  def toCommandExpression(expression: ast.Expression, self: ExpressionConverters): Option[CommandExpression]
}

class ExpressionConverters(converters: ExpressionConverter*) {

  self =>

  def toCommandExpression(expression: ast.Expression): CommandExpression = {
    converters foreach { c: ExpressionConverter =>
        c.toCommandExpression(expression, this) match {
          case Some(x) => return x
          case None =>
        }
    }

    throw new InternalException(s"Unknown expression type during transformation (${expression.getClass})")
  }

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

  def toCommandPredicate(e: Option[ast.Expression]): Predicate =
    e.map(self.toCommandPredicate).getOrElse(predicates.True())

  def toCommandSeekArgs(seek: SeekableArgs): SeekArgs = seek match {
    case SingleSeekableArg(expr) => SingleSeekArg(toCommandExpression(expr))
    case ManySeekableArgs(expr) => expr match {
      case coll: ast.ListLiteral =>
        ZeroOneOrMany(coll.expressions) match {
          case Zero => SeekArgs.empty
          case One(value) => SingleSeekArg(toCommandExpression(value))
          case Many(_) => ManySeekArgs(toCommandExpression(coll))
        }

      case _ =>
        ManySeekArgs(toCommandExpression(expr))
    }
  }

  def toCommandProjectedPath(e: ast.PathExpression): ProjectedPath = {
    def project(pathStep: ast.PathStep): Projector = pathStep match {

      case ast.NodePathStep(ast.Variable(node), next) =>
        singleNodeProjector(node, project(next))

      case ast.SingleRelationshipPathStep(ast.Variable(rel), SemanticDirection.INCOMING, next) =>
        singleIncomingRelationshipProjector(rel, project(next))

      case ast.SingleRelationshipPathStep(ast.Variable(rel), SemanticDirection.OUTGOING, next) =>
        singleOutgoingRelationshipProjector(rel, project(next))

      case ast.SingleRelationshipPathStep(ast.Variable(rel), SemanticDirection.BOTH, next) =>
        singleUndirectedRelationshipProjector(rel, project(next))

      case ast.MultiRelationshipPathStep(ast.Variable(rel), SemanticDirection.INCOMING, next) =>
        multiIncomingRelationshipProjector(rel, project(next))

      case ast.MultiRelationshipPathStep(ast.Variable(rel), SemanticDirection.OUTGOING, next) =>
        multiOutgoingRelationshipProjector(rel, project(next))

      case ast.MultiRelationshipPathStep(ast.Variable(rel), SemanticDirection.BOTH, next) =>
        multiUndirectedRelationshipProjector(rel, project(next))

      case ast.NilPathStep =>
        nilProjector

      case x =>
        throw new IllegalArgumentException(s"Unknown pattern part found in expression: $x")
    }

    val projector = project(e.step)
    val dependencies = e.step.dependencies.map(_.asInstanceOf[Variable].name)

    ProjectedPath(dependencies, projector)
  }
}

object DirectionConverter {
  def toGraphDb(dir: SemanticDirection): Direction = dir match {
    case SemanticDirection.INCOMING => Direction.INCOMING
    case SemanticDirection.OUTGOING => Direction.OUTGOING
    case SemanticDirection.BOTH => Direction.BOTH
  }
}
