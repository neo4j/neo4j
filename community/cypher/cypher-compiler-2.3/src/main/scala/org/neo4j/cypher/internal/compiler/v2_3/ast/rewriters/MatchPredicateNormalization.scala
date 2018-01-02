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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.{topDown, InputPosition, Rewriter, replace}

class MatchPredicateNormalization(normalizer: MatchPredicateNormalizer) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val instance = replace(replacer => {
    case expr: Expression =>
      replacer.stop(expr)

    case m@Match(_, pattern, _, where) =>
      val predicates = pattern.fold(Vector.empty[Expression]) {
        case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
        case _                                                          => identity
      }

      if (predicates.isEmpty)
        m
      else {
        val rewrittenPredicates: List[Expression] = (predicates ++ where.map(_.expression)).toList

        val predOpt: Option[Expression] = rewrittenPredicates match {
          case Nil => None
          case exp :: Nil => Some(exp)
          case list => Some(list.reduce(And(_, _)(m.position)))
        }

        val newWhere: Option[Where] = predOpt.map {
          exp =>
            val pos: InputPosition = where.fold(m.position)(_.position)
            Where(exp)(pos)
        }

        m.copy(
          pattern = pattern.endoRewrite(topDown(Rewriter.lift(normalizer.replace))),
          where = newWhere
        )(m.position)
      }

    case astNode =>
      replacer.expand(astNode)
  })
}
