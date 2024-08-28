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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.ConvertToNFA
import org.neo4j.cypher.internal.compiler.planner.logical.LimitRangesOnSelectivePathPattern
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.equalsPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.LogicalPlanWithIntoVsAllHeuristic
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSinglePatternSide
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSingleProjectEndpoints
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.preFilterCandidatesByIntoVsAllHeuristic
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelationshipUniquenessPredicate
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.ir.ExhaustiveNodeConnection
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.NodePathVariable
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.RelationshipPathVariable
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NFA.PathLength
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.options.CypherPlanVarExpandInto
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

import scala.collection.immutable.SortedMap
import scala.collection.mutable
import scala.language.implicitConversions

case class expandSolverStep(qg: QueryGraph, qppInnerPlanner: QPPInnerPlanner)
    extends IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext] {

  override def apply(
    registry: IdRegistry[NodeConnection],
    goal: Goal,
    table: IDPCache[LogicalPlan],
    context: LogicalPlanningContext
  ): Iterator[LogicalPlan] = {

    def plansForNodeConnection(source: LogicalPlan, nodeConnection: NodeConnection): Iterator[LogicalPlan] = {
      val candidates = nodeConnection match {
        case relationship: PatternRelationship if source.availableSymbols.contains(relationship.variable) =>
          Iterator[LogicalPlanWithIntoVsAllHeuristic](
            // we do not project endpoints for quantified path patterns
            planSingleProjectEndpoints(relationship, source, context)
          )
        case _ =>
          Iterator(
            planSinglePatternSide(
              qg,
              nodeConnection,
              source,
              nodeConnection.left,
              qppInnerPlanner,
              context
            ),
            planSinglePatternSide(
              qg,
              nodeConnection,
              source,
              nodeConnection.right,
              qppInnerPlanner,
              context
            )
          ).flatten
      }

      preFilterCandidatesByIntoVsAllHeuristic(candidates.toSeq, context).iterator
    }

    for {
      patternId <- goal.bitSet.iterator
      source <- table(Goal(goal.bitSet - patternId)).iterator
      nodeConnection <- registry.lookup(patternId).iterator
      plan <- plansForNodeConnection(source, nodeConnection)
    } yield {
      plan
    }
  }
}

object expandSolverStep {

  /**
   * Plan [[org.neo4j.cypher.internal.logical.plans.ProjectEndpoints]] on top of the given plan for the given [[PatternRelationship]].
   */
  def planSingleProjectEndpoints(
    patternRel: PatternRelationship,
    plan: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (start, end) = patternRel.boundaryNodes
    val isStartInScope = plan.availableSymbols(start)
    val isEndInScope = plan.availableSymbols(end)

    context.staticComponents.logicalPlanProducer.planProjectEndpoints(
      plan,
      start,
      isStartInScope,
      end,
      isEndInScope,
      patternRel,
      context
    )
  }

  /**
   * On top of the given source plan, plan the given [[NodeConnection]], if `nodeId` has been solved already.
   *
   * @param qg             the [[QueryGraph]] that is currently being planned.
   * @param nodeConnection the [[NodeConnection]] to plan
   * @param sourcePlan     the plan to plan on top of
   * @param nodeId         the node to start the expansion from.
   * @param qppInnerPlanner  the precomputed inner plans of [[QuantifiedPathPattern]]s
   */
  def planSinglePatternSide(
    qg: QueryGraph,
    nodeConnection: NodeConnection,
    sourcePlan: LogicalPlan,
    nodeId: LogicalVariable,
    qppInnerPlanner: QPPInnerPlanner,
    context: LogicalPlanningContext
  ): Option[LogicalPlanWithIntoVsAllHeuristic] = {
    val availableSymbols = sourcePlan.availableSymbols

    if (availableSymbols(nodeId)) {
      produceLogicalPlan(
        qg,
        nodeConnection,
        sourcePlan,
        nodeId,
        availableSymbols,
        context,
        qppInnerPlanner
      )
    } else {
      None
    }
  }

  private def produceLogicalPlan(
    qg: QueryGraph,
    patternRel: NodeConnection,
    sourcePlan: LogicalPlan,
    nodeId: LogicalVariable,
    availableSymbols: Set[LogicalVariable],
    context: LogicalPlanningContext,
    qppInnerPlanner: QPPInnerPlanner
  ): Option[LogicalPlanWithIntoVsAllHeuristic] = {
    patternRel match {
      case rel: PatternRelationship =>
        Some(produceExpandLogicalPlan(qg, rel, rel.variable, sourcePlan, nodeId, availableSymbols, context))
      case qpp: QuantifiedPathPattern =>
        Some(produceTrailLogicalPlan(
          qpp,
          sourcePlan,
          nodeId,
          availableSymbols,
          context,
          qppInnerPlanner,
          unsolvedPredicates(context.staticComponents.planningAttributes.solveds, qg.selections, sourcePlan),
          qg
        ))
      case spp: SelectivePathPattern =>
        produceStatefulShortestLogicalPlan(
          spp,
          sourcePlan,
          nodeId,
          availableSymbols,
          qg,
          context
        )
    }
  }

  private def unsolvedPredicates(solveds: Solveds, s: Selections, l: LogicalPlan): Seq[Expression] = {
    val alreadySolved = solveds.get(l.id).asSinglePlannerQuery.queryGraph.selections
    (s -- alreadySolved).flatPredicates
  }

  /**
   * On top of the given source plan, plan the given [[PatternRelationship]].
   *
   * @param qg                  the [[QueryGraph]] that is currently being planned.
   * @param patternRelationship the [[PatternRelationship]] to plan
   * @param sourcePlan          the plan to plan on top of
   * @param node              the node to start the expansion from.
   */
  def produceExpandLogicalPlan(
    qg: QueryGraph,
    patternRelationship: PatternRelationship,
    pattern: LogicalVariable,
    sourcePlan: LogicalPlan,
    node: LogicalVariable,
    availableSymbols: Set[LogicalVariable],
    context: LogicalPlanningContext
  ): LogicalPlanWithIntoVsAllHeuristic = {
    val otherSide = patternRelationship.otherSide(node)
    val overlapping = availableSymbols.contains(otherSide)
    val mode = if (overlapping) ExpandInto else ExpandAll

    patternRelationship match {
      case pr @ PatternRelationship(_, _, _, _, SimplePatternLength) =>
        context.staticComponents.logicalPlanProducer.planSimpleExpand(
          sourcePlan,
          node,
          otherSide,
          pr,
          mode,
          context
        )
      case PatternRelationship(_, _, _, _, varLength: VarPatternLength) =>
        val availablePredicates: collection.Seq[Expression] =
          qg.selections.predicatesGiven(availableSymbols + pattern + otherSide)
        val (
          nodePredicates: ListSet[VariablePredicate],
          relationshipPredicates: ListSet[VariablePredicate],
          solvedPredicates: ListSet[Expression]
        ) =
          extractPredicates(
            availablePredicates,
            originalRelationship = pattern,
            originalNode = node,
            targetNode = otherSide,
            targetNodeIsBound = mode.equals(ExpandInto),
            varLength = varLength
          )

        val plan =
          context.staticComponents.logicalPlanProducer.planVarExpand(
            source = sourcePlan,
            from = node,
            to = otherSide,
            patternRelationship = patternRelationship,
            nodePredicates = nodePredicates,
            relationshipPredicates = relationshipPredicates,
            solvedPredicates = solvedPredicates,
            mode = mode,
            context = context
          )

        heuristicForExpandIntoVsAll(
          plan,
          sourcePlan,
          mode,
          context
        )
    }
  }

  object VariableList {

    def unapply(arg: Any): Option[Set[LogicalVariable]] = arg match {
      case v: Variable       => Some(Set(v))
      case Add(part1, part2) => unapply(part1).map(_ ++ unapply(part2).getOrElse(Set.empty))
      case _                 => None
    }
  }

  private def produceTrailLogicalPlan(
    quantifiedPathPattern: QuantifiedPathPattern,
    sourcePlan: LogicalPlan,
    startNode: LogicalVariable,
    availableVars: Set[LogicalVariable],
    context: LogicalPlanningContext,
    qppInnerPlanner: QPPInnerPlanner,
    predicates: Seq[Expression],
    queryGraph: QueryGraph
  ): LogicalPlanWithIntoVsAllHeuristic = {
    val fromLeft = startNode == quantifiedPathPattern.left

    // Get the QPP inner plan
    val extractedPredicates = extractQPPPredicates(predicates, quantifiedPathPattern.variableGroupings, availableVars)

    val (updatedLabelInfo, updatedContext) = context.staticComponents.labelInferenceStrategy.inferLabels(
      context,
      queryGraph.patternNodeLabels,
      queryGraph.nodeConnections.toIndexedSeq
    )

    // We only retain the relevant label infos to get more cache hits.
    val filteredLabelInfo = updatedLabelInfo.view.filterKeys(
      extractedPredicates.requiredSymbols ++ Set(
        quantifiedPathPattern.leftBinding.outer,
        quantifiedPathPattern.rightBinding.outer
      )
    ).toMap

    val innerPlan = qppInnerPlanner.planQPP(quantifiedPathPattern, fromLeft, extractedPredicates, filteredLabelInfo)
    val innerPlanPredicates = extractedPredicates.predicates.map(_.original)

    // Update the QPP for Trail planning
    val updatedQpp = qppInnerPlanner.updateQpp(quantifiedPathPattern, fromLeft, availableVars)

    val startBinding = if (fromLeft) updatedQpp.leftBinding else updatedQpp.rightBinding
    val endBinding = if (fromLeft) updatedQpp.rightBinding else updatedQpp.leftBinding
    val originalEndBinding = if (fromLeft) quantifiedPathPattern.rightBinding else quantifiedPathPattern.leftBinding

    // If both the start and the end are already bound, we need to plan an extra filter to verify that we expanded to the right end nodes.
    val maybeHiddenFilter =
      if (originalEndBinding.outer != endBinding.outer) {
        Some(equalsPredicate(endBinding.outer, originalEndBinding.outer))
      } else {
        None
      }

    val groupingRelationships = quantifiedPathPattern.relationshipVariableGroupings.map(_.group)

    def isBound(variable: LogicalVariable): Boolean = {
      sourcePlan.availableSymbols.contains(variable)
    }

    /**
     * A solved predicate that expresses some relationship uniqueness.
     *
     * @param solvedPredicate                   the predicate to mark as solved.
     * @param previouslyBoundRelationships      previously bound relationship variables that are used by Trail to solve the predicate.
     * @param previouslyBoundRelationshipGroups previously bound relationship group variables that are used by Trail to solve the predicate.
     */
    case class SolvedUniquenessPredicate(
      solvedPredicate: Expression,
      previouslyBoundRelationships: Option[LogicalVariable] = None,
      previouslyBoundRelationshipGroups: Set[LogicalVariable] = Set.empty
    )

    val uniquenessPredicates = predicates.collect {
      case uniquePred @ Unique(VariableList(list)) if list.subsetOf(groupingRelationships) =>
        SolvedUniquenessPredicate(uniquePred)

      case disjointPred @ Disjoint(VariableList(list1), VariableList(list2))
        if list1.subsetOf(groupingRelationships) && list2.forall(isBound) =>
        SolvedUniquenessPredicate(disjointPred, previouslyBoundRelationshipGroups = list2)
      case disjointPred @ Disjoint(VariableList(list1), VariableList(list2))
        if list2.subsetOf(groupingRelationships) && list1.forall(isBound) =>
        SolvedUniquenessPredicate(disjointPred, previouslyBoundRelationshipGroups = list1)

      case noneOfPred @ NoneOfRelationships(singletonVariable: Variable, VariableList(groupedVariables))
        if groupedVariables.subsetOf(groupingRelationships) && isBound(singletonVariable) =>
        SolvedUniquenessPredicate(noneOfPred, previouslyBoundRelationships = Some(singletonVariable))
    }

    val solvedPredicates = uniquenessPredicates.map(_.solvedPredicate) ++ innerPlanPredicates
    val previouslyBoundRelationships = uniquenessPredicates.flatMap(_.previouslyBoundRelationships).toSet
    val previouslyBoundRelationshipGroups = uniquenessPredicates.flatMap(_.previouslyBoundRelationshipGroups).toSet

    val plan = updatedContext.staticComponents.logicalPlanProducer.planTrail(
      source = sourcePlan,
      pattern = quantifiedPathPattern,
      startBinding = startBinding,
      endBinding = endBinding,
      maybeHiddenFilter = maybeHiddenFilter,
      context = updatedContext,
      innerPlan = innerPlan,
      predicates = solvedPredicates,
      previouslyBoundRelationships,
      previouslyBoundRelationshipGroups,
      reverseGroupVariableProjections = !fromLeft
    )

    val bothEndpointsBoundInSourcePlan =
      availableVars.contains(quantifiedPathPattern.left) && availableVars.contains(quantifiedPathPattern.right)

    heuristicForExpandIntoVsAll(
      plan,
      sourcePlan,
      if (bothEndpointsBoundInSourcePlan) ExpandInto else ExpandAll,
      context
    )
  }

  private def rewritePredicatesToInlinableForm(
    spp: SelectivePathPattern,
    availableSymbols: Set[LogicalVariable]
  ): Selections = {
    spp.pathPattern.connections.foldLeft(spp.selections) {
      case (updatedSelections, nodeConnection) =>
        nodeConnection match {
          case qpp: QuantifiedPathPattern =>
            rewritePredicatesToInlinableForm(
              updatedSelections,
              qpp.leftBinding.outer,
              qpp.leftBinding.inner,
              qpp.rightBinding.inner,
              qpp.rightBinding.outer,
              availableSymbols
            )
          case _ => updatedSelections
        }
    }
  }

  /**
   * An equality predicate referring to the same property on the left-inner and right-inner node of a QPP
   * for example `li.prop=ri.prop` in: (lo) ((li)-->()-->()-->()-->(ri) WHERE li.prop=ri.prop)* (ro)
   * can be rewritten 
   * into:    `li.prop=ro.prop`
   * or into: `lo.prop=ri.prop`.
   * If `ro` is already bound, then `li.prop=ro.prop` can be inlined.
   * If `lo` is already bound, then `lo.prop=ri.prop` can be inlined.
   * Therefore, we will rewrite this predicate depending on what variable is already bound.
   *
   * Proof by induction that this rewrite is correct (li[i].prop=ri[i].prop into lo.prop=ri[i].prop):
   * - Iteration 0: lo and li[0] are juxtaposed, therefore li[0].prop=ri[0].prop is equivalent to lo.prop=ri[0].prop
   * Induction hypothesis: For iteration k, it holds that li[k].prop=ri[k].prop is equivalent to lo.prop=ri[k].prop
   * Iteration k+1: li[k+1] is juxtaposed with the ri[k], therefore li[k+1].prop=ri[k+1].prop is equivalent to ri[k].prop=ri[k+1].prop. Transitivity with the induction hypothesis (lo.prop=ri[k].prop) gives lo.prop=ri[k+1].prop.
   * This proves that for any number of iterations this rewrite is correct.
   * In case of zero iterations, the predicate has no effect, therefore the rewrite is also correct when the minimum number of iterations is zero, as is the case with Kleene Star.
   * Note: the property key on both sides of the equals operator must be the same!
   *
   * The other rewrite (li[i].prop=ri[i].prop into li[i].prop=ro.prop) is similar when reasoning from the other side.
   *
   * @param selections       The selections that might be rewritten
   * @param qppLeftOuter     The left-outer node of a qpp
   * @param qppLeftInner     The left-inner node of a qpp
   * @param qppRightInner    The right-inner node of a qpp
   * @param qppRightOuter    The right-outer node of a qpp
   * @param availableSymbols The variables that have already been bounded
   * @return The rewritten selections
   */
  private def rewritePredicatesToInlinableForm(
    selections: Selections,
    qppLeftOuter: LogicalVariable,
    qppLeftInner: LogicalVariable,
    qppRightInner: LogicalVariable,
    qppRightOuter: LogicalVariable,
    availableSymbols: Set[LogicalVariable]
  ): Selections = {
    val updatedExpression = selections.flatPredicates.map {
      case far @ ForAllRepetitions(
          _,
          variableGroupings,
          eq @ Equals(Property(lhsVar, lhsPropKey), Property(rhsVar, rhsPropKey))
        )
        if lhsPropKey == rhsPropKey &&
          ((lhsVar == qppLeftInner && rhsVar == qppRightInner)
            || (lhsVar == qppRightInner && rhsVar == qppLeftInner)) =>
        if (availableSymbols.contains(qppLeftOuter)) {
          // Translate (leftOuter)((leftInner)--()-...-()--(rightInner) WHERE leftInner.prop = rightInner.prop)*(rightOuter)
          // to        (leftOuter)((leftInner)--()-...-()--(rightInner) WHERE leftOuter.prop = rightInner.prop)*(rightOuter)
          val groupOfRightInnerVariable = variableGroupings.filter(_.singleton == qppRightInner).head.group
          ForAllRepetitions(
            groupOfRightInnerVariable,
            variableGroupings,
            eq.replaceAllOccurrencesBy(qppLeftInner, qppLeftOuter)
          )(far.position)
        } else if (availableSymbols.contains(qppRightOuter)) {
          // Translate (leftOuter)((leftInner)--()-...-()--(rightInner) WHERE leftInner.prop = rightInner.prop)*(rightOuter)
          // to        (leftOuter)((leftInner)--()-...-()--(rightInner) WHERE leftInner.prop = rightOuter.prop)*(rightOuter)
          val groupOfLeftInnerVariable = variableGroupings.filter(_.singleton == qppLeftInner).head.group
          ForAllRepetitions(
            groupOfLeftInnerVariable,
            variableGroupings,
            eq.replaceAllOccurrencesBy(qppRightInner, qppRightOuter)
          )(far.position)
        } else {
          far
        }
      case other => other
    }
    Selections.from(updatedExpression)
  }

  /**
   * Predicates that were extracted from Quantified Path Patterns within a Selective Path Pattern during
   * [[org.neo4j.cypher.internal.compiler.planner.logical.MoveQuantifiedPathPatternPredicates]] gets
   * inlined back into the QPP here and changed back into its original form.
   * @param spp SelectivePathPattern to update
   * @param availableSymbols Symbols available from source
   * @return Updated SelectivePathPattern with extracted QPP Predicates inlined and reverted back to its original form
   */
  def inlineQPPPredicates(spp: SelectivePathPattern, availableSymbols: Set[LogicalVariable]): SelectivePathPattern = {
    val rewrittenSppSelections = rewritePredicatesToInlinableForm(spp, availableSymbols)
    // We need to collect the updated node connections as well as the inlined predicates so we can remove them from the SPP Selections.
    val (newConnections, liftedPredicates) =
      spp.pathPattern.connections.foldLeft((Seq[ExhaustiveNodeConnection](), Set[Expression]())) {
        case ((updatedNodeConnections, inlinedQppPredicates), nodeConnection) => nodeConnection match {
            case pr: PatternRelationship => (updatedNodeConnections.appended(pr), inlinedQppPredicates)
            case qpp: QuantifiedPathPattern =>
              case class VariableGroupingSet(
                variableGroupings: Set[VariableGrouping],
                inlineCheck: (Expression, Set[LogicalVariable]) => Boolean
              )

              // All node variables, each in their own set
              val nodeVariableGrouping =
                qpp.nodeVariableGroupings.map(s => VariableGroupingSet(Set(s), ConvertToNFA.canBeInlined))
              // All relationship variables, each in their own set
              val relVariableGrouping =
                qpp.relationshipVariableGroupings.map(s => VariableGroupingSet(Set(s), ConvertToNFA.canBeInlined))
              // All directed relationships in a set together with their boundary nodes.
              // We do this because predicates using only these can also be inlined.
              // See `getExtraRelationshipPredicates` in ConvertToNFA.
              val extraRelVariableGroupings =
                qpp.patternRelationships.toSet
                  .filterNot(_.dir == BOTH)
                  .map(pr => pr.boundaryNodesSet + pr.variable)
                  .map { singletonVariables =>
                    val variableGroupings = qpp.variableGroupings
                      .filter(variableGrouping => singletonVariables.contains(variableGrouping.singleton))
                    // To be able to inline these extra predicates, we also need to know whether variables can
                    // safely be rewritten in the expression.
                    VariableGroupingSet(variableGroupings, ConvertToNFA.canBeInlinedAndVariablesRewritten)
                  }

              val variableGroupings: Set[VariableGroupingSet] =
                nodeVariableGrouping ++ relVariableGrouping ++ extraRelVariableGroupings

              val extractedPredicates = variableGroupings.map {
                case VariableGroupingSet(variableGrouping, inlineCheck) =>
                  val extracted = extractQPPPredicates(
                    rewrittenSppSelections.flatPredicates,
                    variableGrouping,
                    availableSymbols
                  )
                  val singletonVariables = variableGrouping.map(_.singleton)
                  val filteredPredicates = extracted.predicates
                    .filter(extractedPredicate =>
                      inlineCheck(extractedPredicate.extracted, singletonVariables)
                    )
                  extracted.copy(predicates = filteredPredicates)
              }

              val inlinedPredicates = extractedPredicates.flatMap(_.predicates.map(_.extracted))
              val originalPredicates = extractedPredicates.flatMap(_.predicates.map(_.original))

              (
                updatedNodeConnections.appended(
                  qpp.copy(selections = qpp.selections ++ Selections.from(inlinedPredicates))
                ),
                inlinedQppPredicates ++ originalPredicates
              )
          }
      }

    spp.copy(
      pathPattern = spp.pathPattern.copy(connections = NonEmptyList.from(newConnections)),
      selections = rewrittenSppSelections -- Selections.from(liftedPredicates)
    )
  }

  private def produceStatefulShortestLogicalPlan(
    originalSpp: SelectivePathPattern,
    sourcePlan: LogicalPlan,
    startNode: LogicalVariable,
    availableSymbols: Set[LogicalVariable],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): Option[LogicalPlanWithIntoVsAllHeuristic] = {
    val spp = inlineQPPPredicates(originalSpp, availableSymbols)
    val fromLeft = startNode == spp.left
    val endNode = if (fromLeft) spp.right else spp.left

    val (mode, matchingHints) = if (availableSymbols.contains(endNode)) {
      val matchingHints = queryGraph.statefulShortestPathIntoHints
        .filter(_.variables.toIndexedSeq == spp.pathVariables.map(_.variable))
      (ExpandInto, matchingHints)
    } else {
      val matchingHints = queryGraph.statefulShortestPathAllHints
        .filter(_.variables.toIndexedSeq == spp.pathVariables.map(_.variable))
      (ExpandAll, matchingHints)
    }

    if (!fromLeft && mode == ExpandInto) {
      // ExpandInto is symmetrical.
      // So there is no point considering it both from left and from right.
      // When coming from right, we stop here.
      return None
    }

    val unsolvedPredicatesOnEndNode = queryGraph.selections
      .predicatesGiven(availableSymbols + endNode)
      .filterNot(predicate =>
        context.staticComponents.planningAttributes.solveds
          .get(sourcePlan.id)
          .asSinglePlannerQuery
          .exists(_.queryGraph.selections.contains(predicate))
      )

    val rewriteLookup = mutable.Map.empty[LogicalVariable, LogicalVariable]
    val nonSingletons =
      spp.allQuantifiedPathPatterns.flatMap(_.groupVariables) ++
        spp.varLengthRelationships +
        startNode ++
        Option.when(mode == ExpandInto)(endNode)

    val singletonNodeVariables = Set.newBuilder[Mapping]
    val singletonRelVariables = Set.newBuilder[Mapping]
    spp.pathVariables.iterator
      .filterNot(pathVariable => nonSingletons.contains(pathVariable.variable))
      .foreach { pathVar =>
        val nfaName = Namespacer.genName(context.staticComponents.anonymousVariableNameGenerator, pathVar.variable.name)
        val mapping = Mapping(varFor(nfaName), pathVar.variable)
        rewriteLookup.addOne(mapping.rowVar -> mapping.nfaExprVar)
        pathVar match {
          case _: NodePathVariable         => singletonNodeVariables.addOne(mapping)
          case _: RelationshipPathVariable => singletonRelVariables.addOne(mapping)
        }
      }
    // Get minimum and maximum path lengths from the pattern.
    val pathLength = PathLength.from(spp.pathPattern)
    // Limit ranges for quantified patterns in SPP so we don't create NFAs that are too large.
    val rewrittenSpp =
      LimitRangesOnSelectivePathPattern(context.settings.statefulShortestPlanningRewriteQuantifiersAbove)(spp)

    val (nfa, nonInlinedSelections, syntheticVarLengthSingletons) = {
      ConvertToNFA.convertToNfa(
        rewrittenSpp,
        fromLeft,
        availableSymbols,
        unsolvedPredicatesOnEndNode,
        context.staticComponents.anonymousVariableNameGenerator
      )
    }
    val nonInlinedSelectionsWithoutUniqPreds = nonInlinedSelections.filter(_.expr match {
      case far: ForAllRepetitions =>
        far.originalInnerPredicate match {
          case _: RelationshipUniquenessPredicate => false
          case _                                  => true
        }
      case _: RelationshipUniquenessPredicate => false
      case _                                  => true
    })

    val rewrittenNfa = nfa.endoRewrite(bottomUp(Rewriter.lift {
      case variable: LogicalVariable => rewriteLookup.getOrElse(variable, variable)
    }))
    val selector = convertSelectorFromIr(rewrittenSpp.selector)
    val nodeVariableGroupings =
      rewrittenSpp.allQuantifiedPathPatterns.flatMap(_.nodeVariableGroupings)
    val relationshipVariableGroupings =
      rewrittenSpp.allQuantifiedPathPatterns.flatMap(_.relationshipVariableGroupings) ++
        syntheticVarLengthSingletons.map(entry =>
          VariableGrouping(entry._2, entry._1)(InputPosition.NONE)
        )
    val nonInlinedPreFilters =
      Option.when(nonInlinedSelectionsWithoutUniqPreds.nonEmpty)(
        Ands.create(nonInlinedSelectionsWithoutUniqPreds.flatPredicates.to(ListSet))
      )

    Some(
      heuristicForStatefulShortestInto(
        context.staticComponents.logicalPlanProducer.planStatefulShortest(
          sourcePlan,
          startNode,
          endNode,
          rewrittenNfa,
          mode,
          nonInlinedPreFilters,
          nodeVariableGroupings,
          relationshipVariableGroupings,
          singletonNodeVariables.result(),
          singletonRelVariables.result(),
          selector,
          rewrittenSpp.solvedString,
          originalSpp,
          unsolvedPredicatesOnEndNode,
          reverseGroupVariableProjections = !fromLeft,
          matchingHints,
          context,
          pathLength
        ),
        context
      )
    )
  }

  /**
   * A value that controls what expansion mode candidates should be produced.
   */
  sealed trait IntoVsAllHeuristic extends Ordered[IntoVsAllHeuristic] {
    private val inOrder = Seq(IntoVsAllHeuristic.Avoid, IntoVsAllHeuristic.Neutral, IntoVsAllHeuristic.Prefer)

    override def compare(that: IntoVsAllHeuristic): Int = inOrder.indexOf(this) - inOrder.indexOf(that)
  }

  object IntoVsAllHeuristic {

    /**
     * Prefer planning this plan, if possible.
     */
    case object Prefer extends IntoVsAllHeuristic

    /**
     * There is no preference whether this plan should be planned or not.
     */
    case object Neutral extends IntoVsAllHeuristic

    /**
     * Avoid planning this plan, if possible.
     */
    case object Avoid extends IntoVsAllHeuristic
  }

  case class LogicalPlanWithIntoVsAllHeuristic(plan: LogicalPlan, heuristic: IntoVsAllHeuristic)

  object LogicalPlanWithIntoVsAllHeuristic {

    implicit def neutralPlan(plan: LogicalPlan): LogicalPlanWithIntoVsAllHeuristic =
      LogicalPlanWithIntoVsAllHeuristic(plan, IntoVsAllHeuristic.Neutral)
  }

  /**
   * If we have `statefulShortestPlanningMode = cardinality_heuristic`,
   * then we want to heuristically prefer some plans over others:
   * - Favor INTO if the input cardinality is <= 1
   * - Do not prefer INTO if the input cardinality is > 1
   */
  private def heuristicForStatefulShortestInto(
    ssp: StatefulShortestPath,
    context: LogicalPlanningContext
  ): LogicalPlanWithIntoVsAllHeuristic = {
    ssp.mode match {
      case Expand.ExpandAll =>
        LogicalPlanWithIntoVsAllHeuristic(ssp, IntoVsAllHeuristic.Neutral)
      case Expand.ExpandInto =>
        val cardinalities = context.staticComponents.planningAttributes.cardinalities
        val inCardinality = cardinalities.get(ssp.source.id)
        // + 0.1 to accommodate very leniently for rounding errors.
        val single = inCardinality <= Cardinality.SINGLE + 0.1
        LogicalPlanWithIntoVsAllHeuristic(ssp, if (single) IntoVsAllHeuristic.Prefer else IntoVsAllHeuristic.Avoid)
    }
  }

  /**
   * This function can be used for VarExpand relationships and QPPs
   *
   * Avoid planning an expandInto when the child plan is estimated to have a cardinality larger than 1.
   * This should limit the planner into planning an expensive Cartesian Product followed by an expandInto.
   *
   * Consider the query: MATCH (a:A)-[r:R*]->(b:B) RETURN *
   *
   * The following query plan is avoided when the Cartesian Product of nodes with the label "A" and "B" is larger than 1
   * .expand("(a:A)-[r:R*]->(b:B)", expandMode = ExpandInto, projectedDir = OUTGOING)
   * .cartesianProduct
   * .|.nodeByLabelScan("b", "B")
   * .nodeByLabelScan("a", "A")
   *
   * Otherwise, it might get chosen when the variable length pattern is highly overestimated, which will make the final filter appear very expensive
   * .filter("b:B")                                                                 # cost will be highly overestimated due to overestimated cardinality estimate of its child
   * .expand("(a:A)-[r:R*]->(b)", expandMode = ExpandALL, projectedDir = OUTGOING)  # cardinality might be highly overestimated
   * .nodeByLabelScan("a", "A")
   *
   * @param plan          The original VarExpand or Trail logical plan
   * @param sourcePlan    The logical plan that is the source of `plan`
   * @param expansionMode The expansion mode.
   *                      Either ExpandInto, which means that both endpoint have been bound by the source plan.
   *                      Or ExpandAll, which means that only one endpoint has been bound by the source plan..
   * @param context       The logicalPlanningContext
   * @return The original logical plan together with an annotation whether it should be avoided or not
   */
  private def heuristicForExpandIntoVsAll(
    plan: LogicalPlan,
    sourcePlan: LogicalPlan,
    expansionMode: ExpansionMode,
    context: LogicalPlanningContext
  ): LogicalPlanWithIntoVsAllHeuristic = {
    if (context.settings.planVarExpandInto == CypherPlanVarExpandInto.singleRow) {
      if (expansionMode == ExpandInto) {
        val cardinalities = context.staticComponents.planningAttributes.cardinalities
        val inCardinality = cardinalities.get(sourcePlan.id)
        // + 0.1 to accommodate very leniently for rounding errors.
        val single = inCardinality <= Cardinality.SINGLE + 0.1
        LogicalPlanWithIntoVsAllHeuristic(
          plan,
          if (single) IntoVsAllHeuristic.Neutral else IntoVsAllHeuristic.Avoid
        )
      } else {
        LogicalPlanWithIntoVsAllHeuristic(plan, IntoVsAllHeuristic.Neutral)
      }
    } else {
      // context.settings.planVarExpandInto == CypherPlanVarExpandInto.minimum_cost
      LogicalPlanWithIntoVsAllHeuristic(plan, IntoVsAllHeuristic.Neutral)
    }
  }

  /**
   * Filter out candidates according to the IntoVsAllHeuristic.
   * This happens as a "pre-filter", before the candidates are given the pickBest.
   *
   * Thus this preference weights more than the cost estimated by the cost model.
   * This preference weights less than hints. This is why we include hints in the
   * filtering in this method.
   * This is necessary so that
   * - USING JOIN/SCAN hints can be fulfilled
   * - our internal hints when we don't use `statefulShortestPlanningMode = cardinality_heuristic` can be fulfilled
   */
  private[idp] def preFilterCandidatesByIntoVsAllHeuristic(
    candidates: Iterable[LogicalPlanWithIntoVsAllHeuristic],
    context: LogicalPlanningContext
  ): Iterable[LogicalPlan] = {
    candidates
      .map {
        case LogicalPlanWithIntoVsAllHeuristic(plan, heuristic) =>
          val numHints = context.staticComponents.planningAttributes.solveds.get(plan.id).numHints
          // Create a tuple (numHints, heuristic) for each candidate.
          (plan, (numHints, heuristic))
      }
      // Group by that tuple.
      .groupBy(_._2)
      .to(SortedMap)
      // Only keep the last group. This is either the group solving the most hints,
      // or if this is equal for all candidates, than this is the group with the highest
      // IntoVsAllHeuristic.
      .lastOption
      .map {
        _._2.map(_._1)
      }.getOrElse(Iterable.empty)
  }

  private val convertSelectorFromIr: SelectivePathPattern.Selector => StatefulShortestPath.Selector = {
    // for now we will implement ANY via SHORTEST.
    case SelectivePathPattern.Selector.Any(k)            => StatefulShortestPath.Selector.Shortest(k)
    case SelectivePathPattern.Selector.ShortestGroups(k) => StatefulShortestPath.Selector.ShortestGroups(k)
    case SelectivePathPattern.Selector.Shortest(k)       => StatefulShortestPath.Selector.Shortest(k)
  }
}
