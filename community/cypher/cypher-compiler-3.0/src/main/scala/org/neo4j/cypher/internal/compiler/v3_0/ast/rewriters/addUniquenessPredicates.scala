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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.ast._

case object addUniquenessPredicates extends Rewriter {

  def apply(that: AnyRef): AnyRef = instance(that)

  private val rewriter = Rewriter.lift {
    case m@Match(_, pattern: Pattern, _, where: Option[Where]) =>
      val uniqueRels: Seq[UniqueRel] = collectUniqueRels(pattern)

      if (uniqueRels.size < 2) {
        m
      } else {
        val maybePredicate: Option[Expression] = createPredicateFor(uniqueRels, m.position)
        val newWhere: Option[Where] = (where, maybePredicate) match {
          case (Some(oldWhere), Some(newPredicate)) =>
            Some(oldWhere.copy(expression = And(oldWhere.expression, newPredicate)(m.position))(m.position))

          case (None, Some(newPredicate)) =>
            Some(Where(expression = newPredicate)(m.position))

          case (oldWhere, None) => oldWhere
        }
        m.copy(where = newWhere)(m.position)
      }
  }

  private val instance = bottomUp(rewriter, _.isInstanceOf[Expression])

  def collectUniqueRels(pattern: ASTNode): Seq[UniqueRel] =
    pattern.treeFold(Seq.empty[UniqueRel]) {
      case _: ShortestPaths =>
        acc => (acc, None)

      case RelationshipChain(_, patRel@RelationshipPattern(optIdent, _, types, _, _, _), _) =>
        acc => {
          val ident = optIdent.getOrElse(throw new InternalException("This rewriter cannot work with unnamed patterns"))
          (acc :+ UniqueRel(ident, types.toSet, patRel.isSingleLength), Some(identity))
        }
    }

  private def createPredicateFor(uniqueRels: Seq[UniqueRel], pos: InputPosition): Option[Expression] = {
    createPredicatesFor(uniqueRels, pos).reduceOption(And(_, _)(pos))
  }

  def createPredicatesFor(uniqueRels: Seq[UniqueRel], pos: InputPosition): Seq[Expression] =
    for {
      x <- uniqueRels
      y <- uniqueRels if x.name < y.name && !x.isAlwaysDifferentFrom(y)
    } yield {
      val equals = Equals(x.variable.copyId, y.variable.copyId)(pos)

      (x.singleLength, y.singleLength) match {
        case (true, true) =>
          Not(equals)(pos)

        case (true, false) =>
          NoneIterablePredicate(y.variable.copyId, y.variable.copyId, Some(equals))(pos)

        case (false, true) =>
          NoneIterablePredicate(x.variable.copyId, x.variable.copyId, Some(equals))(pos)

        case (false, false) =>
          NoneIterablePredicate(x.variable.copyId, x.variable.copyId, Some(AnyIterablePredicate(y.variable.copyId, y.variable.copyId, Some(equals))(pos)))(pos)
      }
    }

  case class UniqueRel(variable: Variable, types: Set[RelTypeName], singleLength: Boolean) {
    def name = variable.name

    def isAlwaysDifferentFrom(other: UniqueRel) =
      types.nonEmpty && other.types.nonEmpty && (types intersect other.types).isEmpty
  }
}
