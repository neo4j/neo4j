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

object normalizeMatchPredicates extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = instance.apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case m@Match(_, pattern, _, where) =>
      val predicates = pattern.fold(Vector.empty[Expression]) {
        case NodePattern(Some(id), _, Some(props: MapExpression), _) =>
          acc => acc ++ props.items.map {
            case (propId, expression) =>
              Equals(Property(id, propId)(props.position), expression)(props.position)
          }
        case RelationshipPattern(Some(id), _, _, _, Some(props: MapExpression), _) =>
          acc => acc ++ props.items.map {
            case (propId, expression) =>
              Equals(Property(id, propId)(props.position), expression)(props.position)
          }
      }
      if (predicates.isEmpty)
        m
      else {
        val predicate = (predicates ++ where.map(_.expression)) reduceLeftOption {
          (left, right) => And(left, right)(m.position)
        }
        val rewrittenWhere = predicate.map(Where(_)(where.map(_.position).getOrElse(m.position)))
        val rewrittenPattern = pattern.rewrite(topDown(Rewriter.lift {
          case p@NodePattern(Some(id), _, Some(props: MapExpression), _) =>
            p.copy(properties = None)(p.position)
          case p@RelationshipPattern(Some(id), _, _, _, Some(props: MapExpression), _) =>
            p.copy(properties = None)(p.position)
        })).asInstanceOf[Pattern]
        m.copy(pattern = rewrittenPattern, where = rewrittenWhere)(m.position)
      }
  }
}
