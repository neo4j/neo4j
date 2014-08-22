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
package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_2.ast.{Not, Ors, PatternExpression, Where}
import org.neo4j.cypher.internal.compiler.v2_2.helpers.UnNamedNameGenerator._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.compiler.v2_2.planner.{Predicate, Selections}
import ExpressionConverters._

object ClauseConverters {

  implicit class OptionalWhereConverter(val optWhere: Option[Where]) {
    def asSelections = Selections(optWhere.
      map(_.expression.asPredicates).
      getOrElse(Set.empty))
  }

  implicit class SelectionsSubQueryExtraction(val selections: Selections) {
    def getContainedPatternExpressions: Set[PatternExpression] = {
      val predicates = selections.predicates
      val predicatesWithCorrectDeps = predicates.map {
        case Predicate(deps, e: PatternExpression) =>
          Predicate(deps.filter(x => isNamed(x.name)), e)
        case Predicate(deps, ors@Ors(exprs)) =>
          val newDeps = exprs.foldLeft(Set.empty[IdName]) { (acc, exp) =>
            exp match {
              case exp: PatternExpression => acc ++ exp.idNames.filter(x => isNamed(x.name))
              case _                      => acc ++ exp.idNames
            }
          }
          Predicate(newDeps, ors)
        case p                               => p
      }

      val subQueries: Set[PatternExpression] = predicatesWithCorrectDeps.flatMap {
        case Predicate(_, Ors(orOperands)) =>
          orOperands.collect {
            case expr: PatternExpression      => expr
            case Not(expr: PatternExpression) => expr
          }

        case Predicate(_, Not(expr: PatternExpression)) =>
          Some(expr)

        case Predicate(_, expr: PatternExpression) =>
          Some(expr)

        case _ =>
          None
      }

      subQueries
    }
  }
}
