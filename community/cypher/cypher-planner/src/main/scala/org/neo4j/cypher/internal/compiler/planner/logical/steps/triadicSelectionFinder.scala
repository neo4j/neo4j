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

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.Selections.containsExistsSubquery
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection

case object triadicSelectionFinder extends SelectionCandidateGenerator {

  override def apply(
    input: LogicalPlan,
    unsolvedPredicates: Set[Expression],
    queryGraph: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Iterator[SelectionCandidate] = {
    // The current runtime implementations of TriadicSelection relies on order being preserved and is not yet supported
    // by parallel runtime.
    if (context.settings.executionModel.providedOrderPreserving) {
      unsolvedPredicates.iterator.filter(containsExistsSubquery).collect {
        // WHERE NOT (a)-[:X]->(c)
        case predicate @ Not(subqueryExpression: ExistsIRExpression) =>
          findMatchingRelationshipPattern(
            positivePredicate = false,
            predicate,
            subqueryExpression,
            input,
            queryGraph,
            context
          )
            .map(SelectionCandidate(_, Set(predicate)))
        // WHERE (a)-[:X]->(c)
        case predicate: ExistsIRExpression =>
          findMatchingRelationshipPattern(
            positivePredicate = true,
            predicate,
            predicate,
            input,
            queryGraph,
            context
          )
            .map(SelectionCandidate(_, Set(predicate)))
      }.flatten
    } else {
      Iterator.empty[SelectionCandidate]
    }
  }

  private def findMatchingRelationshipPattern(
    positivePredicate: Boolean,
    triadicPredicate: Expression,
    subqueryExpression: ExistsIRExpression,
    in: LogicalPlan,
    qg: QueryGraph,
    context: LogicalPlanningContext
  ): Seq[LogicalPlan] = in match {

    // MATCH (a)-[:X]->(b)-[:X]->(c) WHERE (predicate involving (a)-[:X]->(c))
    case Selection(Ands(predicates), exp: Expand) => findMatchingOuterExpand(
        positivePredicate,
        triadicPredicate,
        subqueryExpression,
        predicates.toSeq,
        exp,
        qg,
        context
      )

    // MATCH (a)-[:X]->(b)-[:Y]->(c) WHERE (predicate involving (a)-[:X]->(c))
    case exp: Expand =>
      findMatchingOuterExpand(positivePredicate, triadicPredicate, subqueryExpression, Seq.empty, exp, qg, context)

    case _ => Seq.empty
  }

  private def findMatchingOuterExpand(
    positivePredicate: Boolean,
    triadicPredicate: Expression,
    subqueryExpression: ExistsIRExpression,
    incomingPredicates: Seq[Expression],
    expand: Expand,
    qg: QueryGraph,
    context: LogicalPlanningContext
  ): Seq[LogicalPlan] = expand match {
    case exp2 @ Expand(exp1: Expand, _, _, _, _, _, ExpandAll) =>
      findMatchingInnerExpand(
        positivePredicate,
        triadicPredicate,
        subqueryExpression,
        incomingPredicates,
        Seq.empty,
        exp1,
        exp2,
        qg,
        context
      )

    case exp2 @ Expand(Selection(Ands(innerPredicates), exp1: Expand), _, _, _, _, _, ExpandAll) =>
      findMatchingInnerExpand(
        positivePredicate,
        triadicPredicate,
        subqueryExpression,
        incomingPredicates,
        innerPredicates.toSeq,
        exp1,
        exp2,
        qg,
        context
      )

    case _ => Seq.empty
  }

  private def findMatchingInnerExpand(
    positivePredicate: Boolean,
    triadicPredicate: Expression,
    subqueryExpression: ExistsIRExpression,
    incomingPredicates: Seq[Expression],
    leftPredicates: Seq[Expression],
    exp1: Expand,
    exp2: Expand,
    qg: QueryGraph,
    context: LogicalPlanningContext
  ): Seq[LogicalPlan] =
    if (
      exp1.mode == ExpandAll && exp1.to == exp2.from &&
      matchingLabels(positivePredicate, exp1.to, exp2.to, qg) &&
      leftPredicatesAcceptable(exp1.to, leftPredicates) &&
      matchingIRExpression(subqueryExpression, exp1.from, exp2.to, exp1.types, exp1.dir)
    ) {

      val left =
        if (leftPredicates.nonEmpty)
          context.staticComponents.logicalPlanProducer.planSelection(exp1, leftPredicates, context)
        else
          exp1

      val argument = context.staticComponents.logicalPlanProducer.planArgument(
        patternNodes = Set(exp2.from),
        patternRels = Set(exp1.relName),
        other = Set.empty,
        context = context
      )
      val newExpand2 = {
        val from = exp2.from
        val to = exp2.to
        val expand2PR = qg.patternRelationships.find {
          case PatternRelationship(_, (`from`, `to`), _, _, _) => true
          case PatternRelationship(_, (`to`, `from`), _, _, _) => true
          case _                                               => false
        }.get
        context.staticComponents.logicalPlanProducer.planSimpleExpand(
          argument,
          exp2.from,
          exp2.to,
          expand2PR,
          ExpandAll,
          context
        )
      }
      val right =
        if (incomingPredicates.nonEmpty)
          context.staticComponents.logicalPlanProducer.planSelection(newExpand2, incomingPredicates, context)
        else
          newExpand2

      Seq(context.staticComponents.logicalPlanProducer.planTriadicSelection(
        positivePredicate,
        left,
        exp1.from,
        exp2.from,
        exp2.to,
        right,
        triadicPredicate,
        context
      ))
    } else
      Seq.empty

  private def leftPredicatesAcceptable(leftId: LogicalVariable, leftPredicates: Seq[Expression]) =
    leftPredicates.forall {
      case HasLabels(v: Variable, Seq(_)) if v == leftId => true
      case _                                             => false
    }

  private def matchingLabels(
    positivePredicate: Boolean,
    node1: LogicalVariable,
    node2: LogicalVariable,
    qg: QueryGraph
  ): Boolean = {
    val labels1 = qg.selections.labelsOnNode(node1)
    val labels2 = qg.selections.labelsOnNode(node2)
    if (positivePredicate)
      labels1 == labels2
    else
      labels1.isEmpty || labels2.nonEmpty && (labels2 subsetOf labels1)
  }

  private def matchingIRExpression(
    pattern: ExistsIRExpression,
    from: LogicalVariable,
    to: LogicalVariable,
    types: Seq[RelTypeName],
    dir: SemanticDirection
  ): Boolean = pattern match {
    // (a)-[:X]->(c)
    case ExistsIRExpression(
        RegularSinglePlannerQuery(
          QueryGraph(
            SetExtractor(PatternRelationship(
              rel,
              (predicateFrom, predicateTo),
              predicateDir,
              predicateTypes,
              SimplePatternLength
            )),
            SetExtractor(),
            patternNodes,
            _,
            Selections.empty,
            IndexedSeq(),
            SetExtractor(),
            SetExtractor(),
            IndexedSeq(),
            SetExtractor()
          ),
          InterestingOrder.empty,
          RegularQueryProjection(_, QueryPagination.empty, Selections.empty, _),
          None,
          None
        ),
        _,
        _
      )
      if patternNodes == Set(predicateFrom, predicateTo)
        && predicateFrom == from
        && predicateTo == to
        && predicateDir == dir
        && predicateTypes == types
        && !pattern.dependencies.contains(rel) => true
    case _ => false
  }
}
