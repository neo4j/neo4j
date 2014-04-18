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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.helpers.ThisShouldNotHappenError
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.IdName

// TODO: Use Seq[SelectionPredicate] (with case class SelectionPredicate(expression, dependencies))
case class Selections(predicates: Seq[(Set[IdName], Expression)] = Seq.empty) {
  def predicatesGiven(ids: Set[IdName]): Seq[Expression] = predicates.collect {
    case (deps, predicate) if (deps -- ids).isEmpty => predicate
  }

  def predicatesAndDependenciesGiven(ids: Set[IdName]): Seq[(Set[IdName], Expression)] = predicates.collect {
    case (deps, predicate) if (deps -- ids).isEmpty => (deps, predicate)
  }

  def flatPredicates: Seq[Expression] =
    predicates.map(_._2)

  def labelPredicates: Map[IdName, Set[HasLabels]] = {
    predicates.foldLeft(Map.empty[IdName, Set[HasLabels]]) { case (m, pair) =>
      val (_, expr) = pair
      expr match {
        case hasLabels @ HasLabels(Identifier(name), labels) =>
          // FIXME: remove when we have test for checking that we construct the expected plan
          if (labels.size > 1) {
            throw new ThisShouldNotHappenError("Davide", "Rewriting should introduce single label HasLabels predicates in the WHERE clause")
          }
          val idName = IdName(name)
          m.updated(idName, m.getOrElse(idName, Set.empty) + hasLabels)
        case _ =>
          m
      }
    }
  }

  def coveredBy(solvedPredicates: Seq[Expression]): Boolean =
    flatPredicates.forall( solvedPredicates.contains )
}
