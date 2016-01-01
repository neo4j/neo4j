/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.{bottomUp, Rewriter}

object collapseInCollectionsContainingConstants extends Rewriter {
  override def apply(that: AnyRef) = bottomUp(instance).apply(that)

  case class InValue(lhs: Expression, expr: Expression)

  private val instance: Rewriter = Rewriter.lift {
    case predicate@Ors(exprs) =>
      val (const, nonConst) = exprs.toList.partition {
        case in@In(_, rhs: Collection) if ConstantExpression.unapply(rhs).isDefined => true
        case _ => false
      }

      val ins = const.flatMap {
        case In(lhs, rhs: Collection) =>
          rhs.expressions.map(expr => InValue(lhs, expr))
      }

      val flattenConst = ins.groupBy(_.lhs).map {
        case (lhs, values) => {
          val pos = lhs.position
          In(lhs, Collection(values.map(_.expr).toSeq)(pos))(pos)
        }
      }

      nonConst ++ flattenConst match {
        case head :: Nil => head
        case l => Ors(l.toSet)(predicate.position)
      }
  }
}

