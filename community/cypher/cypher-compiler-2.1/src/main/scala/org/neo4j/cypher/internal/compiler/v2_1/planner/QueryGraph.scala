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

import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Where
import org.neo4j.cypher.internal.compiler.v2_1.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.helpers.ThisShouldNotHappenError

/*
An abstract representation of the query graph being solved at the current step
 */
case class QueryGraph(projections: Map[String, ast.Expression],
                      selections: Selections,
                      identifiers: Set[IdName])

object SelectionPredicates {
  def fromWhere(where: Where) = extractPredicates(where.expression)

  private def extractPredicates(predicate: ast.Expression): Seq[(Set[IdName], ast.Expression)] = predicate match {
    // n:Label
    case HasLabels(identifier@Identifier(name), labels) =>
      labels.map( (label: LabelName) => Set(IdName(name)) -> HasLabels(identifier, Seq(label))(predicate.position) )

    // id(n) = value
    case Equals(FunctionInvocation(Identifier("id"), _, IndexedSeq(Identifier(ident))), _) =>
      Seq(Set(IdName(ident)) -> predicate)

    // n.prop = value
    case Equals(Property(Identifier(name), PropertyKeyName(_)), ConstantExpression(_)) =>
      Seq(Set(IdName(name)) -> predicate)

    // and
    case And(predicateLhs, predicateRhs) =>
      extractPredicates(predicateLhs) ++ extractPredicates(predicateRhs)

    case _ =>
      throw new CantHandleQueryException
  }
}

case class Selections(predicates: Seq[(Set[IdName], ast.Expression)] = Seq.empty) {
  def apply(availableIds: Set[IdName]): Seq[ast.Expression] =
    predicates.collect { case (k, v) if k.subsetOf(availableIds) => v }

  def flatPredicates: Seq[ast.Expression] = {
    val flatPredicatesBuilder = Seq.newBuilder[Expression]
    predicates.foreach( flatPredicatesBuilder += _._2 )
    flatPredicatesBuilder.result()
  }

  def labelPredicates: Map[IdName, Set[ast.HasLabels]] = {
    predicates.foldLeft(Map.empty[IdName, Set[ast.HasLabels]]) { case (m, pair) =>
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
