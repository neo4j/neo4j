/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.UsingScanHint
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.ResultOrdering
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability.BOTH
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.util.InputPosition

import scala.collection.mutable

case class subtractionLabelScanLeafPlanner(skipIDs: Set[LogicalVariable]) extends LeafPlanner {

  private def collectPositiveAndNegativeLabelPredicatesPerVariable(qg: QueryGraph)
    : Map[Variable, (Set[LabelName], Set[LabelName])] = {
    qg.selections.flatPredicatesSet.foldLeft(Map.empty[Variable, (Set[LabelName], Set[LabelName])]) {
      case (acc, current) => current match {
          // Positive labels
          case HasLabels(variable: Variable, labels)
            if !skipIDs.contains(variable) && (qg.patternNodes(variable) && !qg.argumentIds(variable)) =>
            val newValue =
              acc.get(variable).map(c => (c._1 ++ labels, c._2)).getOrElse((labels.toSet, Set.empty[LabelName]))
            acc + (variable -> newValue)
          // Negative labels
          case Not(HasLabels(variable: Variable, labels))
            if !skipIDs.contains(variable) && (qg.patternNodes(variable) && !qg.argumentIds(variable)) =>
            val newValue =
              acc.get(variable).map(c => (c._1, c._2 ++ labels)).getOrElse((Set.empty[LabelName], labels.toSet))
            acc + (variable -> newValue)
          case _ => acc
        }
    }
  }

  private def constructSubtractionNodeByLabelScan(
    variable: Variable,
    positiveLabels: Set[LabelName],
    negativeLabels: Set[LabelName],
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    nodeTokenIndex: TokenIndexDescriptor,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val providedOrder = ResultOrdering.providedOrderForLabelScan(
      interestingOrderConfig.orderToSolve,
      variable,
      nodeTokenIndex.orderCapability,
      context.providedOrderFactory
    )
    val hints = qg.hints.collect {
      case hint @ UsingScanHint(`variable`, LabelOrRelTypeName(name))
        if positiveLabels.exists(_.name == name) => hint
    }
    val negativeLabelPredicates =
      negativeLabels.map(label => Not(HasLabels(variable, Seq(label))(InputPosition.NONE))(InputPosition.NONE))
    val labelPredicates =
      negativeLabelPredicates.toSeq :+ HasLabels(variable, positiveLabels.toSeq)(InputPosition.NONE)
    val result = context.staticComponents.logicalPlanProducer.planSubtractionNodeByLabelsScan(
      variable,
      positiveLabels.toSeq,
      negativeLabels.toSeq,
      labelPredicates,
      hints.toSeq,
      qg.argumentIds,
      providedOrder,
      context
    )
    result
  }

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    context.staticComponents.planContext.nodeTokenIndex match {
      case Some(nodeTokenIndex) if nodeTokenIndex.orderCapability == BOTH =>
        // Combine the positive labels and the negative labels
        // For example HasLabels(n, Seq(A)), HasLabels(n, Seq(B)), Not(HasLabels(n, Seq(C))), NOT(HasLabels(n, Seq(D)))  to n -> (Set(A, B), Set(C, D))
        val combined: Map[Variable, (Set[LabelName], Set[LabelName])] =
          collectPositiveAndNegativeLabelPredicatesPerVariable(qg)

        // combined: Map from 'variable' to pair '(positiveLabels, negativeLabels)'
        combined
          .filter(variableToPositiveNegativeLabelsPair =>
            // At least one positive label and at least one negative label
            variableToPositiveNegativeLabelsPair._2._1.nonEmpty &&
              variableToPositiveNegativeLabelsPair._2._2.nonEmpty
          )
          .map(variableToPositiveNegativeLabelsPair => {
            val variable = variableToPositiveNegativeLabelsPair._1
            val positiveLabels = variableToPositiveNegativeLabelsPair._2._1
            val negativeLabels = variableToPositiveNegativeLabelsPair._2._2

            constructSubtractionNodeByLabelScan(
              variable,
              positiveLabels,
              negativeLabels,
              qg,
              interestingOrderConfig,
              nodeTokenIndex,
              context
            )
          }).toSet

      case _ => Set.empty
    }
  }
}
