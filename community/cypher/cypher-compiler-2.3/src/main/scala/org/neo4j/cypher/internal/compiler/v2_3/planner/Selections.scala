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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.perty.PageDocFormatting
import org.neo4j.helpers.ThisShouldNotHappenError

case class Predicate(dependencies: Set[IdName], expr: Expression) extends PageDocFormatting { // with ToPrettyString[Predicate] {

//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

  def hasDependenciesMet(symbols: Set[IdName]): Boolean =
    (dependencies -- symbols).isEmpty

  def hasDependenciesMetForRequiredSymbol(symbols: Set[IdName], required: IdName): Boolean =
    dependencies.contains(required) && hasDependenciesMet(symbols)
}


object Predicate {
  implicit val byPosition = Ordering.by { (predicate: Predicate) => predicate.expr.position }
}

object Selections {
  def from(expressions: Expression*): Selections = new Selections(expressions.flatMap(_.asPredicates).toSet)
}

case class Selections(predicates: Set[Predicate] = Set.empty) extends PageDocFormatting { // with ToPrettyString[Selections] {

//  def toDefaultPrettyString(formatter: DocFormatter) =
//    toPrettyString(formatter)(InternalDocHandler.docGen)

  def isEmpty = predicates.isEmpty

  def predicatesGiven(ids: Set[IdName]): Seq[Expression] = predicates.collect {
    case p@Predicate(_, predicate) if p.hasDependenciesMet(ids) => predicate
  }.toSeq

  def predicatesGivenForRequiredSymbol(allowed: Set[IdName], required: IdName): Seq[Expression] = predicates.collect {
    case p@Predicate(_, predicate) if p.hasDependenciesMetForRequiredSymbol(allowed, required) => predicate
  }.toSeq

  def unsolvedPredicates(plan: LogicalPlan): Seq[Expression] =
    scalarPredicatesGiven(plan.availableSymbols)
      .filterNot(predicate => plan.solved.exists(_.graph.selections.contains(predicate)))

  def scalarPredicatesGiven(ids: Set[IdName]): Seq[Expression] = predicatesGiven(ids).filterNot(containsPatternPredicates)

  def patternPredicatesGiven(ids: Set[IdName]): Seq[Expression] = predicatesGiven(ids).filter(containsPatternPredicates)

  private def containsPatternPredicates(e: Expression): Boolean = e match {
    case _: PatternExpression      => true
    case Not(_: PatternExpression) => true
    case Ors(exprs)                => exprs.exists(containsPatternPredicates)
    case _                         => false
  }

  def flatPredicates: Seq[Expression] =
    predicates.map(_.expr).toSeq

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

  def labelsOnNode(id: IdName): Set[LabelName] = labelInfo.getOrElse(id, Set.empty)

  lazy val labelInfo: Map[IdName, Set[LabelName]] =
    labelPredicates.mapValues(_.map(_.labels.head))

  def coveredBy(solvedPredicates: Seq[Expression]): Boolean =
    flatPredicates.forall( solvedPredicates.contains )

  def contains(e: Expression): Boolean = predicates.exists { _.expr == e }

  def ++(other: Selections): Selections = {
    val otherPredicates = other.predicates
    val keptPredicates  = predicates.filter {
      case pred@Predicate(_, expr: PartialPredicate[_]) =>
        !expr.coveringPredicate.asPredicates.forall(expr => otherPredicates.contains(expr) || predicates.contains(expr))

      case pred =>
        true
    }

    Selections(keptPredicates ++ other.predicates)
  }

  def ++(expressions: Expression*): Selections = Selections(predicates ++ expressions.flatMap(_.asPredicates))
}
