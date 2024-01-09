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
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSinglePatternSide
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSingleProjectEndpoints
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
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.immutable.ListSet
import scala.collection.mutable

case class expandSolverStep(qg: QueryGraph, qppInnerPlanner: QPPInnerPlanner)
    extends IDPSolverStep[NodeConnection, LogicalPlan, LogicalPlanningContext] {

  override def apply(
    registry: IdRegistry[NodeConnection],
    goal: Goal,
    table: IDPCache[LogicalPlan],
    context: LogicalPlanningContext
  ): Iterator[LogicalPlan] = {
    val result: Iterator[Iterator[LogicalPlan]] =
      for {
        patternId <- goal.bitSet.iterator
        plan <- table(Goal(goal.bitSet - patternId)).iterator
        pattern <- registry.lookup(patternId)
      } yield {
        pattern match {
          case relationship: PatternRelationship if plan.availableSymbols.contains(relationship.variable) =>
            Iterator(
              // we do not project endpoints for quantified path patterns
              planSingleProjectEndpoints(relationship, plan, context)
            )
          case _ =>
            Iterator(
              planSinglePatternSide(
                qg,
                pattern,
                plan,
                pattern.left,
                qppInnerPlanner,
                context
              ),
              planSinglePatternSide(
                qg,
                pattern,
                plan,
                pattern.right,
                qppInnerPlanner,
                context
              )
            ).flatten
        }
      }

    result.flatten
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
   * On top of the given source plan, plan  the given [[NodeConnection]], if `nodeId` has been solved already.
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
  ): Option[LogicalPlan] = {
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
  ): LogicalPlan = {
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
          unsolvedPredicates(context.staticComponents.planningAttributes.solveds, qg.selections, sourcePlan)
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
    predicates: Seq[Expression]
  ): LogicalPlan = {
    val fromLeft = startNode == quantifiedPathPattern.left

    // Get the QPP inner plan
    val extractedPredicates = extractQPPPredicates(predicates, quantifiedPathPattern.variableGroupings, availableVars)
    val innerPlan = qppInnerPlanner.planQPP(quantifiedPathPattern, fromLeft, extractedPredicates)
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

    context.staticComponents.logicalPlanProducer.planTrail(
      source = sourcePlan,
      pattern = quantifiedPathPattern,
      startBinding = startBinding,
      endBinding = endBinding,
      maybeHiddenFilter = maybeHiddenFilter,
      context = context,
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
            case pr: PatternRelationship    => (updatedNodeConnections.appended(pr), inlinedQppPredicates)
            case qpp: QuantifiedPathPattern =>
              // All node variables, each in their own set
              val nodeVariableGrouping = qpp.nodeVariableGroupings.map(Set(_))
              // All relationship variables, each in their own set
              val relVariableGrouping = qpp.relationshipVariableGroupings.map(Set(_))
              // All directed relationships in a set together with their boundary nodes.
              // We do this because predicates using only these can also be inlined.
              // See `getExtraRelationshipPredicates` in ConvertToNFA.
              val extraRelVariableGroupings =
                qpp.patternRelationships.toSet
                  .filterNot(_.dir == BOTH)
                  .map(pr => pr.boundaryNodesSet + pr.variable)
                  .map(singletonVariables =>
                    qpp.variableGroupings
                      .filter(variableGrouping => singletonVariables.contains(variableGrouping.singleton))
                  )

              val variableGroupings: Set[Set[VariableGrouping]] =
                nodeVariableGrouping ++ relVariableGrouping ++ extraRelVariableGroupings

              val extractedPredicates = variableGroupings.map { variableGrouping =>
                val extracted = extractQPPPredicates(
                  spp.selections.flatPredicates,
                  variableGrouping,
                  availableSymbols
                )
                val singletonVariables = variableGrouping.map(_.singleton)
                val filteredPredicates = extracted.predicates
                  .filter(extractedPredicate =>
                    ConvertToNFA.canBeInlined(extractedPredicate.extracted, singletonVariables)
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
  ) = {
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
    val solvedExpressionAsString =
      spp.copy(selections =
        selectionsWithOriginalPredicates(spp) ++ unsolvedPredicatesOnEndNode
      )
        .solvedString

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
      solvedExpressionAsString,
      originalSpp,
      unsolvedPredicatesOnEndNode,
      reverseGroupVariableProjections = !fromLeft,
      matchingHints,
      context
    )
  }

  private def selectionsWithOriginalPredicates(spp: SelectivePathPattern): Selections =
    Selections.from(spp.selections.flatPredicates.map {
      case far: ForAllRepetitions =>
        far.originalInnerPredicate
      case expr: Expression => expr
    })

  private val convertSelectorFromIr: SelectivePathPattern.Selector => StatefulShortestPath.Selector = {
    // for now we will implement ANY via SHORTEST.
    case SelectivePathPattern.Selector.Any(k)            => StatefulShortestPath.Selector.Shortest(k)
    case SelectivePathPattern.Selector.ShortestGroups(k) => StatefulShortestPath.Selector.ShortestGroups(k)
    case SelectivePathPattern.Selector.Shortest(k)       => StatefulShortestPath.Selector.Shortest(k)
  }
}
