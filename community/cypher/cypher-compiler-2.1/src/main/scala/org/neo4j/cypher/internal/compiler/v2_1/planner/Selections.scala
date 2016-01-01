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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_1.perty.GeneratedPretty
import org.neo4j.cypher.internal.compiler.v2_1.docbuilders.internalDocBuilder

case class Predicate(dependencies: Set[IdName], exp: Expression)
  extends internalDocBuilder.AsPrettyToString {

  def hasDependenciesMet(symbols: Set[IdName]): Boolean =
    (dependencies -- symbols).isEmpty
}

case class Selections(predicates: Set[Predicate] = Set.empty)
  extends internalDocBuilder.AsPrettyToString {

  def predicatesGiven(ids: Set[IdName]): Seq[Expression] = predicates.collect {
    case p@Predicate(_, predicate) if p.hasDependenciesMet(ids) => predicate
  }.toSeq

  def scalarPredicatesGiven(ids: Set[IdName]): Seq[Expression] = predicatesGiven(ids).filterNot(containsPatternPredicates)

  def patternPredicatesGiven(ids: Set[IdName]): Seq[Expression] = predicatesGiven(ids).filter(containsPatternPredicates)

  private def containsPatternPredicates(e: Expression): Boolean = e match {
    case _: PatternExpression      => true
    case Not(_: PatternExpression) => true
    case Ors(exprs)                => exprs.exists(containsPatternPredicates)
    case _                         => false
  }

  def flatPredicates: Seq[Expression] =
    predicates.map(_.exp).toSeq

  def labelPredicates: Map[IdName, Set[HasLabels]] =
    predicates.foldLeft(Map.empty[IdName, Set[HasLabels]]) {
      case (acc, Predicate(_, hasLabels@HasLabels(Identifier(name), labels))) =>
        // FIXME: remove when we have test for checking that we construct the expected plan
        if (labels.size > 1) {
          throw new ThisShouldNotHappenError("Davide", "Rewriting should introduce single label HasLabels predicates in the WHERE clause")
        }
        val idName = IdName(name)
        acc.updated(idName, acc.getOrElse(idName, Set.empty) + hasLabels)
      case (acc, _) => acc
    }

  def coveredBy(solvedPredicates: Seq[Expression]): Boolean =
    flatPredicates.forall( solvedPredicates.contains )

  def contains(e: Expression): Boolean = predicates.exists { _.exp == e }

  def ++(other: Selections): Selections = Selections(predicates ++ other.predicates)

  def ++(expressions: Expression*): Selections = Selections(predicates ++ expressions.flatMap(SelectionPredicates.extractPredicates))
}
