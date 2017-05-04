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
package org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_3._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.helpers.calculateUsingGetDegree

abstract class MatchPredicateNormalization(normalizer: MatchPredicateNormalizer) extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case m@Match(_, pattern, _, where) =>
      val predicates = pattern.fold(Vector.empty[Expression]) {
        case pattern: AnyRef if normalizer.extract.isDefinedAt(pattern) => acc => acc ++ normalizer.extract(pattern)
        case _                                                          => identity
      }

      if (predicates.isEmpty)
        m.copy(where = m.where.map(w => {
          val pos: InputPosition = where.fold(m.position)(_.position)
          w.copy( expression = w.expression.endoRewrite(whereRewriter))(pos)
        }))(m.position)
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
            Where(exp.endoRewrite(whereRewriter))(pos)
        }

        m.copy(
          pattern = pattern.endoRewrite(topDown(Rewriter.lift(normalizer.replace))),
          where = newWhere
        )(m.position)
      }
  }

  private def whereRewriter: Rewriter = Rewriter.lift {
    // WHERE (a)-[:R]->() to WHERE GetDegree( (a)-[:R]->()) > 0
    case p@PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(Some(node), List(), None),
                                                                    RelationshipPattern(None, types, None, None, dir, _),
                                                                    NodePattern(None, List(), None)))) =>
      GreaterThan(calculateUsingGetDegree(p, node, types, dir), SignedDecimalIntegerLiteral("0")(p.position))(p.position)
    // WHERE ()-[:R]->(a) to WHERE GetDegree( (a)<-[:R]-()) > 0
    case p@PatternExpression(RelationshipsPattern(RelationshipChain(NodePattern(None, List(), None),
                                                                    RelationshipPattern(None, types, None, None, dir, _),
                                                                    NodePattern(Some(node), List(), None)))) =>
      GreaterThan(calculateUsingGetDegree(p, node, types, dir.reversed), SignedDecimalIntegerLiteral("0")(p.position))(p.position)

    case a@And(lhs, rhs) =>
      And(lhs.endoRewrite(whereRewriter), rhs.endoRewrite(whereRewriter))(a.position)

    case o@Or(lhs, rhs) => Or(lhs.endoRewrite(whereRewriter), rhs.endoRewrite(whereRewriter))(o.position)

    case n@Not(e) => Not(e.endoRewrite(whereRewriter))(n.position)
  }

  private val instance = topDown(rewriter, _.isInstanceOf[Expression])
}
