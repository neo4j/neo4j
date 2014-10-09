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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeDependence

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.Selectivity
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.cardinality.assumeDependence.groupPredicates.EstimatedPredicateCombination
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans.IdName
import org.neo4j.cypher.internal.helpers.Converge.iterateUntilConverged

object groupPredicates {
  type EstimatedPredicateCombination = (PredicateCombination, Selectivity)
}

case class groupPredicates(selectivityEstimator: PredicateCombination => Selectivity) extends (Set[Predicate] => Set[EstimatedPredicateCombination]) {
  def iteration(in: (Set[Predicate], Set[EstimatedPredicateCombination])) = {
    val (predicates, combinations) = in
    if (predicates.isEmpty) {
      (predicates, combinations)
    } else {
      val combination = findMostUsefulCombination(predicates)
      (predicates -- combination._1.containedPredicates, combinations + combination)
    }
  }

  def apply(predicates: Set[Predicate]): Set[EstimatedPredicateCombination] = {
    val (_, result) = iterateUntilConverged(iteration)((predicates, Set.empty))
    result
  }

  def findCombinations(predicates: Set[Predicate]): Set[EstimatedPredicateCombination] = {
    val combinations = predicates.flatMap {
      case expression@ExpressionPredicate(Not(In(Property(Identifier(name), propertyKey), Collection(expressions)))) =>
        val exists = ExistsPredicate(IdName(name))
        val labelPredicates: Set[(LabelName, ExpressionPredicate)] = predicates.labelsFor(name)
        val labelsCombinedWithExpression: Set[PredicateCombination] = labelPredicates.map {
          case (l, p) => PropertyNotEqualsAndLabelPredicate(propertyKey, expressions.length, l, Set(p, expression, exists))
        }
        labelsCombinedWithExpression + SingleExpression(expression.e)

      case expression@ExpressionPredicate(In(Property(Identifier(name), propertyKey), Collection(expressions))) =>
        val exists = ExistsPredicate(IdName(name))
        val labelPredicates: Set[(LabelName, ExpressionPredicate)] = predicates.labelsFor(name)
        val labelsCombinedWithExpression: Set[PredicateCombination] = labelPredicates.map {
          case (l, p) => PropertyEqualsAndLabelPredicate(propertyKey, expressions.length, l, Set(p, expression, exists))
        }
        labelsCombinedWithExpression + SingleExpression(expression.e)

      case patternPredicate@PatternPredicate(pattern) =>
        val lhsGroupings: Set[Option[(LabelName, ExpressionPredicate)]] = produceGroupings(predicates.labelsFor(pattern.nodes._1.name))
        val rhsGroupings: Set[Option[(LabelName, ExpressionPredicate)]] = produceGroupings(predicates.labelsFor(pattern.nodes._2.name))
        for {
          lhs <- lhsGroupings
          rhs <- rhsGroupings
        } yield {
          val lhsExists = ExistsPredicate(pattern.nodes._1)
          val rhsExists = ExistsPredicate(pattern.nodes._2)
          val lhsLabel = lhs.map(_._1)
          val rhsLabel = rhs.map(_._1)
          val containedPredicates: Set[Predicate] = Set[Predicate](patternPredicate) ++ lhs.map(_._2) ++ rhs.map(_._2) + lhsExists + rhsExists
          RelationshipWithLabels(lhsLabel, pattern, rhsLabel, containedPredicates)
        }

      case or@ExpressionPredicate(ors@Ors(operands)) =>
        val orPreds: Set[Predicate] = operands.map(ExpressionPredicate.apply).toSet
        val leastSelectivePredicateCombo: PredicateCombination = apply(orPreds).maxBy(_._2)._1

        Some(OrCombination(ors, leastSelectivePredicateCombo))

      case ExpressionPredicate(expression) =>
        Some(SingleExpression(expression))

      case ExistsPredicate(idName) =>
        Some(ExistsCombination(idName))
    }

    combinations.map(combination => combination -> selectivityEstimator(combination))
  }

  def findMostUsefulCombination(predicates: Set[Predicate]): EstimatedPredicateCombination =
    findCombinations(predicates).minBy(_._2)

  def produceGroupings(labels: Set[(LabelName, ExpressionPredicate)]): Set[Option[(LabelName, ExpressionPredicate)]] =
    if (labels.isEmpty)
      Set(None)
    else
      labels.map {
        l => Some(l)
      }

  implicit class RichPredicateSet(val predicates: Set[Predicate]) {
    def labelsFor(id: String): Set[(LabelName, ExpressionPredicate)] = predicates.collect {
      case p@ExpressionPredicate(HasLabels(Identifier(name), label :: Nil)) if id == name => (label, p)
    }
  }
}
