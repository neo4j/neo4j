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
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.equalsPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.LogicalPlanWithSSPHeuristic
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSinglePatternSide
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSingleProjectEndpoints
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.preFilterCandidatesBySSPHeuristic
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
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
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
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
          Iterator[LogicalPlanWithSSPHeuristic](
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

      preFilterCandidatesBySSPHeuristic(candidates.toSeq, context).iterator
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
  ): Option[LogicalPlanWithSSPHeuristic] = {
    val availableSymbols = sourcePlan.availableSymbols

    if (availableSymbols(nodeId)) {
      Some(produceLogicalPlan(
        qg,
        nodeConnection,
        sourcePlan,
        nodeId,
        availableSymbols,
        context,
        qppInnerPlanner
      ))
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
  ): LogicalPlanWithSSPHeuristic = {
    patternRel match {
      case rel: PatternRelationship =>
        produceExpandLogicalPlan(qg, rel, rel.variable, sourcePlan, nodeId, availableSymbols, context)
      case qpp: QuantifiedPathPattern =>
        produceTrailLogicalPlan(
          qpp,
          sourcePlan,
          nodeId,
          availableSymbols,
          context,
          qppInnerPlanner,
          unsolvedPredicates(context.staticComponents.planningAttributes.solveds, qg.selections, sourcePlan),
          qg
        )
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
  ): LogicalPlan = {
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
  ): LogicalPlan = {
    val fromLeft = startNode == quantifiedPathPattern.left

    // Get the QPP inner plan
    val extractedPredicates = extractQPPPredicates(predicates, quantifiedPathPattern.variableGroupings, availableVars)

    val (updatedLabelInfo, updatedContext) = context.staticComponents.labelInferenceStrategy.inferLabels(
      context,
      queryGraph.patternNodeLabels,
      queryGraph.nodeConnections.toIndexedSeq
    )

    // We only retain the relevant label infos to get more cache hits.
    val filteredLabelInfo = updatedLabelInfo.view.filterKeys(Set(
      quantifiedPathPattern.leftBinding.outer,
      quantifiedPathPattern.rightBinding.outer
    )).toMap

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

    updatedContext.staticComponents.logicalPlanProducer.planTrail(
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
                    spp.selections.flatPredicates,
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
      selections = spp.selections -- Selections.from(liftedPredicates)
    )
  }

  private def produceStatefulShortestLogicalPlan(
    originalSpp: SelectivePathPattern,
    sourcePlan: LogicalPlan,
    startNode: LogicalVariable,
    availableSymbols: Set[LogicalVariable],
    queryGraph: QueryGraph,
    context: LogicalPlanningContext
  ): LogicalPlanWithSSPHeuristic = {
    val spp = inlineQPPPredicates(originalSpp, availableSymbols)
    val fromLeft = startNode == spp.left
    val endNode = if (fromLeft) spp.right else spp.left

    val unsolvedPredicatesOnEndNode = queryGraph.selections
      .predicatesGiven(availableSymbols + endNode)
      .filterNot(predicate =>
        context.staticComponents.planningAttributes.solveds
          .get(sourcePlan.id)
          .asSinglePlannerQuery
          .exists(_.queryGraph.selections.contains(predicate))
      )

    val (mode, matchingHints) = if (availableSymbols.contains(endNode)) {
      val matchingHints = queryGraph.statefulShortestPathIntoHints
        .filter(_.variables.toIndexedSeq == spp.pathVariables.map(_.variable))
      (ExpandInto, matchingHints)
    } else {
      val matchingHints = queryGraph.statefulShortestPathAllHints
        .filter(_.variables.toIndexedSeq == spp.pathVariables.map(_.variable))
      (ExpandAll, matchingHints)
    }

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

    val (nfa, nonInlinedSelections, syntheticVarLengthSingletons) = {
      ConvertToNFA.convertToNfa(
        spp,
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
    val selector = convertSelectorFromIr(spp.selector)
    val nodeVariableGroupings =
      spp.allQuantifiedPathPatterns.flatMap(_.nodeVariableGroupings)
    val relationshipVariableGroupings =
      spp.allQuantifiedPathPatterns.flatMap(_.relationshipVariableGroupings) ++
        syntheticVarLengthSingletons.map(entry =>
          VariableGrouping(entry._2, entry._1)(InputPosition.NONE)
        )
    val nonInlinedPreFilters =
      Option.when(nonInlinedSelectionsWithoutUniqPreds.nonEmpty)(
        Ands.create(nonInlinedSelectionsWithoutUniqPreds.flatPredicates.to(ListSet))
      )

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
        spp.solvedString,
        originalSpp,
        unsolvedPredicatesOnEndNode,
        reverseGroupVariableProjections = !fromLeft,
        matchingHints,
        context
      ),
      context
    )
  }

  /**
   * A value that controls what StatefulShortestPath candidates should be produced.
   */
  sealed trait SSPHeuristic extends Ordered[SSPHeuristic] {
    private val inOrder = Seq(SSPHeuristic.Avoid, SSPHeuristic.Neutral, SSPHeuristic.Prefer)

    override def compare(that: SSPHeuristic): Int = inOrder.indexOf(this) - inOrder.indexOf(that)
  }

  object SSPHeuristic {

    /**
     * Prefer planning this plan, if possible.
     */
    case object Prefer extends SSPHeuristic

    /**
     * There is no preference whether this plan should be planned or not.
     */
    case object Neutral extends SSPHeuristic

    /**
     * Avoid planning this plan, if possible.
     */
    case object Avoid extends SSPHeuristic
  }

  case class LogicalPlanWithSSPHeuristic(plan: LogicalPlan, heuristic: SSPHeuristic)

  object LogicalPlanWithSSPHeuristic {

    implicit def neutralPlan(plan: LogicalPlan): LogicalPlanWithSSPHeuristic =
      LogicalPlanWithSSPHeuristic(plan, SSPHeuristic.Neutral)
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
  ): LogicalPlanWithSSPHeuristic = {
    ssp.mode match {
      case Expand.ExpandAll =>
        LogicalPlanWithSSPHeuristic(ssp, SSPHeuristic.Neutral)
      case Expand.ExpandInto =>
        val cardinalities = context.staticComponents.planningAttributes.cardinalities
        val inCardinality = cardinalities.get(ssp.source.id)
        // + 0.1 to accommodate very leniently for rounding errors.
        val single = inCardinality <= Cardinality.SINGLE + 0.1
        LogicalPlanWithSSPHeuristic(ssp, if (single) SSPHeuristic.Prefer else SSPHeuristic.Avoid)
    }
  }

  /**
   * Filter out candidates according to the SSPHeuristic.
   * This happens as a "pre-filter", before the candidates are given the pickBest.
   *
   * Thus this preference weights more than the cost estimated by the cost model.
   * This preference weights less than hints. This is why we include hints in the
   * filtering in this method.
   * This is necessary so that
   * - USING JOIN/SCAN hints can be fulfilled
   * - our internal hints when we don't use `statefulShortestPlanningMode = cardinality_heuristic` can be fulfilled
   */
  private[idp] def preFilterCandidatesBySSPHeuristic(
    candidates: Iterable[LogicalPlanWithSSPHeuristic],
    context: LogicalPlanningContext
  ): Iterable[LogicalPlan] = {
    candidates
      .map {
        case LogicalPlanWithSSPHeuristic(plan, heuristic) =>
          val numHints = context.staticComponents.planningAttributes.solveds.get(plan.id).numHints
          // Create a tuple (numHints, heuristic) for each candidate.
          (plan, (numHints, heuristic))
      }
      // Group by that tuple.
      .groupBy(_._2)
      .to(SortedMap)
      // Only keep the last group. This is either the group solving the most hints,
      // or if this is equal for all candidates, than this is the group with the highest
      // SSPHeuristic.
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
