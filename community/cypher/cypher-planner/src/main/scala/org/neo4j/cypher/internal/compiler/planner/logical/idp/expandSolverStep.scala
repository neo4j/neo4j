/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.ConvertToNFA
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.equalsPredicate
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.QPPInnerPlans
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSinglePatternSide
import org.neo4j.cypher.internal.compiler.planner.logical.idp.expandSolverStep.planSingleProjectEndpoints
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.Add
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NoneOfRelationships
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.NodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.VariableGrouping
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.exceptions.InternalException

import scala.collection.immutable.ListSet

case class expandSolverStep(qg: QueryGraph, qppInnerPlans: QPPInnerPlans)
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
          case relationship: PatternRelationship if plan.availableSymbols.contains(varFor(relationship.name)) =>
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
                qppInnerPlans,
                context
              ),
              planSinglePatternSide(
                qg,
                pattern,
                plan,
                pattern.right,
                qppInnerPlans,
                context
              )
            ).flatten
        }
      }

    result.flatten
  }
}

object expandSolverStep {

  case class TrailOption(qpp: QuantifiedPathPattern, fromLeft: Boolean)

  /**
   * Inner plans for [[QuantifiedPathPattern]]s.
   */
  trait QPPInnerPlans {

    /**
     * Get the precomputed RHS plan for a [[QuantifiedPathPattern]].
     *
     * @param trailOption the [[QuantifiedPathPattern]] and also whether we are "expanding" from left or from right.
     * @return the RHS plan.
     */
    def getPlan(trailOption: TrailOption): LogicalPlan
  }

  /**
   * This class precomputes inner plans for all [[QuantifiedPathPattern]]s in the given query graph,
   * in order to not recreate the same plan in multiple iterations.
   */
  class PrecomputedQPPInnerPlans(qg: QueryGraph, context: LogicalPlanningContext) extends QPPInnerPlans {

    private val plans: Map[TrailOption, LogicalPlan] = (for {
      qpp <- qg.quantifiedPathPatterns
      fromLeft <- Set(true, false)
    } yield {
      val plan = planQPPInner(qpp, fromLeft, context)
      TrailOption(qpp, fromLeft) -> plan
    }).toMap

    override def getPlan(trailOption: TrailOption): LogicalPlan = plans(trailOption)
  }

  /**
   * Add an argument to the query graph inside the given [[QuantifiedPathPattern]].
   * The argument is either the left or the right node.
   */
  private def updateQPPArguments(qpp: QuantifiedPathPattern, fromLeft: Boolean): QuantifiedPathPattern = {
    val bindingNodeArg = additionalQPPArgument(qpp, fromLeft)
    qpp.copy(argumentIds = qpp.argumentIds.incl(bindingNodeArg))
  }

  private def additionalQPPArgument(qpp: QuantifiedPathPattern, fromLeft: Boolean): String = {
    if (fromLeft) qpp.leftBinding.inner
    else qpp.rightBinding.inner
  }

  /**
   * Additional predicates to solve on RHS of Trail.
   */
  private def additionalTrailPredicates(qpp: QuantifiedPathPattern): Set[Expression] =
    qpp.patternRelationships.map(r =>
      IsRepeatTrailUnique(Variable(r.name)(InputPosition.NONE))(InputPosition.NONE)
    ).toSet

  /**
   * Plan the inner pattern of a [[QuantifiedPathPattern]].
   */
  private def planQPPInner(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val additionalArgument = additionalQPPArgument(qpp, fromLeft)
    val additionalPredicates = additionalTrailPredicates(qpp)
    val qg = qpp.asQueryGraph.addArgumentId(additionalArgument).addPredicates(additionalPredicates.toSeq: _*)

    // We use InterestingOrderConfig.empty because the order from a RHS of Trail is not propagated anyway
    val plan = context.staticComponents.queryGraphSolver.plan(qg, InterestingOrderConfig.empty, context).result

    context.staticComponents.logicalPlanProducer.fixupTrailRhsPlan(plan, additionalArgument, additionalPredicates)
  }

  /**
   * We currently don't have a trail into operator. Instead we need to create a new variable for the end node of the quantified path pattern, to not override
   * the existing variable for the end node.
   *
   * In `produceTrailLogicalPlan` we add a predicate to make sure the quantified path pattern ends on the correct node.
   */
  private def updateQppForTrailInto(
    qpp: QuantifiedPathPattern,
    fromLeft: Boolean,
    context: LogicalPlanningContext
  ): QuantifiedPathPattern = {
    val newName = context.staticComponents.anonymousVariableNameGenerator.nextName
    val qppWithNewEndBindingOuterName =
      if (fromLeft) {
        qpp.copy(rightBinding = qpp.rightBinding.copy(outer = newName))
      } else {
        qpp.copy(leftBinding = qpp.leftBinding.copy(outer = newName))
      }
    qppWithNewEndBindingOuterName
  }

  /**
   * Update a [[QuantifiedPathPattern]] so that it can be used to plan a [[org.neo4j.cypher.internal.logical.plans.Trail]] operator.
   */
  private def updateQpp(
    qpp: QuantifiedPathPattern,
    availableSymbols: Set[String],
    fromLeft: Boolean,
    context: LogicalPlanningContext
  ): QuantifiedPathPattern = {
    val qppWithUpdatedArguments = updateQPPArguments(qpp, fromLeft)

    val endNode = if (fromLeft) qpp.right else qpp.left
    val overlapping = availableSymbols.contains(endNode)
    if (overlapping) {
      updateQppForTrailInto(qppWithUpdatedArguments, fromLeft, context)
    } else {
      qppWithUpdatedArguments
    }
  }

  /**
   * Plan [[org.neo4j.cypher.internal.logical.plans.ProjectEndpoints]] on top of the given plan for the given [[PatternRelationship]].
   */
  def planSingleProjectEndpoints(
    patternRel: PatternRelationship,
    plan: LogicalPlan,
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (start, end) = patternRel.boundaryNodes
    val isStartInScope = plan.availableSymbols(varFor(start))
    val isEndInScope = plan.availableSymbols(varFor(end))

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
   * @param qppInnerPlans  the precomputed inner plans of [[QuantifiedPathPattern]]s
   */
  def planSinglePatternSide(
    qg: QueryGraph,
    nodeConnection: NodeConnection,
    sourcePlan: LogicalPlan,
    nodeId: String,
    qppInnerPlans: QPPInnerPlans,
    context: LogicalPlanningContext
  ): Option[LogicalPlan] = {
    val availableSymbols = sourcePlan.availableSymbols

    if (availableSymbols(varFor(nodeId))) {
      Some(produceLogicalPlan(
        qg,
        nodeConnection,
        sourcePlan,
        nodeId,
        availableSymbols.map(_.name),
        context,
        qppInnerPlans
      ))
    } else {
      None
    }
  }

  private def produceLogicalPlan(
    qg: QueryGraph,
    patternRel: NodeConnection,
    sourcePlan: LogicalPlan,
    nodeId: String,
    availableSymbols: Set[String],
    context: LogicalPlanningContext,
    qppInnerPlans: QPPInnerPlans
  ): LogicalPlan = {
    patternRel match {
      case rel: PatternRelationship =>
        produceExpandLogicalPlan(qg, rel, rel.name, sourcePlan, nodeId, availableSymbols, context)
      case qpp: QuantifiedPathPattern =>
        produceTrailLogicalPlan(
          qpp,
          sourcePlan,
          nodeId,
          availableSymbols,
          context,
          qppInnerPlans,
          qg.selections.flatPredicates
        )
      case spp: SelectivePathPattern =>
        produceStatefulShortestLogicalPlan(spp, sourcePlan, nodeId, availableSymbols, qg.selections, context)
    }
  }

  /**
   * On top of the given source plan, plan the given [[PatternRelationship]].
   *
   * @param qg                  the [[QueryGraph]] that is currently being planned.
   * @param patternRelationship the [[PatternRelationship]] to plan
   * @param sourcePlan          the plan to plan on top of
   * @param nodeId              the node to start the expansion from.
   */
  def produceExpandLogicalPlan(
    qg: QueryGraph,
    patternRelationship: PatternRelationship,
    patternName: String,
    sourcePlan: LogicalPlan,
    nodeId: String,
    availableSymbols: Set[String],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val otherSide = patternRelationship.otherSide(nodeId)
    val overlapping = availableSymbols.contains(otherSide)
    val mode = if (overlapping) ExpandInto else ExpandAll

    patternRelationship match {
      case pr @ PatternRelationship(_, _, _, _, SimplePatternLength) =>
        context.staticComponents.logicalPlanProducer.planSimpleExpand(
          sourcePlan,
          nodeId,
          otherSide,
          pr,
          mode,
          context
        )
      case PatternRelationship(_, _, _, _, varLength: VarPatternLength) =>
        val availablePredicates: collection.Seq[Expression] =
          qg.selections.predicatesGiven(availableSymbols + patternName + otherSide)
        val (
          nodePredicates: ListSet[VariablePredicate],
          relationshipPredicates: ListSet[VariablePredicate],
          solvedPredicates: ListSet[Expression]
        ) =
          extractPredicates(
            availablePredicates,
            originalRelationshipName = patternName,
            originalNodeName = nodeId,
            targetNodeName = otherSide,
            targetNodeIsBound = mode.equals(ExpandInto),
            varLength = varLength
          )

        context.staticComponents.logicalPlanProducer.planVarExpand(
          source = sourcePlan,
          from = nodeId,
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

    def unapply(arg: Any): Option[Set[String]] = arg match {
      case Variable(name)    => Some(Set(name))
      case Add(part1, part2) => unapply(part1).map(_ ++ unapply(part2).getOrElse(Set.empty))
      case _                 => None
    }
  }

  private def produceTrailLogicalPlan(
    quantifiedPathPattern: QuantifiedPathPattern,
    sourcePlan: LogicalPlan,
    startNode: String,
    availableSymbols: Set[String],
    context: LogicalPlanningContext,
    qppInnerPlans: QPPInnerPlans,
    predicates: Seq[Expression]
  ): LogicalPlan = {
    val fromLeft = startNode == quantifiedPathPattern.left
    val trailOption = TrailOption(quantifiedPathPattern, fromLeft)

    // Get the precomputed inner plan
    val innerPlan = qppInnerPlans.getPlan(trailOption)

    // Update the QPP for Trail planning
    val updatedQpp = updateQpp(quantifiedPathPattern, availableSymbols, fromLeft, context)

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

    val groupingRelationshipNames = quantifiedPathPattern.relationshipVariableGroupings.map(_.groupName)

    def isBound(variable: String): Boolean = {
      sourcePlan.availableSymbols.contains(varFor(variable))
    }

    /**
     * A solved predicate that expresses some relationship uniqueness.
     *
     * @param solvedPredicate                   the predicate to mark as solved.
     * @param previouslyBoundRelationships      previously bound relationship variable names that are used by Trail to solve the predicate.
     * @param previouslyBoundRelationshipGroups previously bound relationship group variable names that are used by Trail to solve the predicate.
     */
    case class SolvedUniquenessPredicate(
      solvedPredicate: Expression,
      previouslyBoundRelationships: Option[String] = None,
      previouslyBoundRelationshipGroups: Set[String] = Set.empty
    )

    val uniquenessPredicates = predicates.collect {
      case uniquePred @ Unique(VariableList(list)) if list.subsetOf(groupingRelationshipNames) =>
        SolvedUniquenessPredicate(uniquePred)

      case disjointPred @ Disjoint(VariableList(list1), VariableList(list2))
        if list1.subsetOf(groupingRelationshipNames) && list2.forall(isBound) =>
        SolvedUniquenessPredicate(disjointPred, previouslyBoundRelationshipGroups = list2)
      case disjointPred @ Disjoint(VariableList(list1), VariableList(list2))
        if list2.subsetOf(groupingRelationshipNames) && list1.forall(isBound) =>
        SolvedUniquenessPredicate(disjointPred, previouslyBoundRelationshipGroups = list1)

      case noneOfPred @ NoneOfRelationships(Variable(singletonVariable), VariableList(groupedVariables))
        if groupedVariables.subsetOf(groupingRelationshipNames) && isBound(singletonVariable) =>
        SolvedUniquenessPredicate(noneOfPred, previouslyBoundRelationships = Some(singletonVariable))
    }

    val solvedPredicates = uniquenessPredicates.map(_.solvedPredicate)
    val previouslyBoundRelationships = uniquenessPredicates.flatMap(_.previouslyBoundRelationships).toSet
    val previouslyBoundRelationshipGroups = uniquenessPredicates.flatMap(_.previouslyBoundRelationshipGroups).toSet

    context.staticComponents.logicalPlanProducer.planTrail(
      source = sourcePlan,
      pattern = quantifiedPathPattern,
      startBinding = startBinding,
      endBinding = endBinding,
      maybeHiddenFilter = maybeHiddenFilter,
      context = context,
      innerPlan,
      predicates = solvedPredicates,
      previouslyBoundRelationships,
      previouslyBoundRelationshipGroups,
      reverseGroupVariableProjections = !fromLeft
    )
  }

  private def produceStatefulShortestLogicalPlan(
    spp: SelectivePathPattern,
    sourcePlan: LogicalPlan,
    startNode: String,
    availableSymbols: Set[String],
    queryGraphSelections: Selections,
    context: LogicalPlanningContext
  ) = {
    val fromLeft = startNode == spp.left
    val endNode = if (fromLeft) spp.right else spp.left

    // Check unsupported cases
    val strictInteriorVariables = spp.pathVariables.init.tail
    val overlap = (availableSymbols + endNode).intersect(strictInteriorVariables.toSet)
    if (overlap.nonEmpty) {
      throw new InternalException(
        s"The selective path pattern overlaps in interior variables with previously bound symbols or end node: $overlap"
      )
    }
    val duplicateVariables = strictInteriorVariables
      .groupBy(identity)
      .filter(_._2.length > 1)
      .keys
    if (duplicateVariables.nonEmpty) {
      throw new InternalException(
        s"Repeating variables $duplicateVariables inside a selective path pattern is currently not supported."
      )
    }

    val unsolvedPredicatesOnEndNode = queryGraphSelections
      .predicatesGiven(availableSymbols + endNode)
      .filterNot(predicate =>
        context.staticComponents.planningAttributes.solveds
          .get(sourcePlan.id)
          .asSinglePlannerQuery
          .exists(_.queryGraph.selections.contains(predicate))
      )

    if (availableSymbols.contains(endNode)) {
      // If both the start and the end are already bound, we need to plan an extra filter to verify that we expanded to the right end nodes.
      val newEndNode = context.staticComponents.anonymousVariableNameGenerator.nextName
      val hiddenFilter = equalsPredicate(newEndNode, endNode)
      val rewriter = topDown(Rewriter.lift {
        case v @ Variable(`endNode`) => Variable(newEndNode)(v.position)
      })

      val newSpp = (if (fromLeft) spp.withRight(newEndNode) else spp.withLeft(newEndNode)).copy(
        selections = Selections.from(spp.selections.flatPredicatesSet.endoRewrite(rewriter))
      )
      val newUnsolvedPredicatesOnEndNode = unsolvedPredicatesOnEndNode.endoRewrite(rewriter)

      convertToNFAAndPlan(
        newSpp,
        sourcePlan,
        startNode,
        fromLeft,
        newEndNode,
        availableSymbols + newEndNode,
        spp,
        newUnsolvedPredicatesOnEndNode,
        Some(hiddenFilter),
        context
      )
    } else {
      convertToNFAAndPlan(
        spp,
        sourcePlan,
        startNode,
        fromLeft,
        endNode,
        availableSymbols,
        spp,
        unsolvedPredicatesOnEndNode,
        None,
        context
      )
    }
  }

  private def convertToNFAAndPlan(
    spp: SelectivePathPattern,
    sourcePlan: LogicalPlan,
    startNode: String,
    fromLeft: Boolean,
    newEndNode: String,
    availableSymbols: Set[String],
    solvedSpp: SelectivePathPattern,
    unsolvedPredicatesOnTargetNode: Seq[Expression],
    maybeHiddenFilter: Option[Expression],
    context: LogicalPlanningContext
  ): LogicalPlan = {
    val (nfa, nonInlinedSelections) =
      ConvertToNFA.convertToNfa(spp, fromLeft, availableSymbols, unsolvedPredicatesOnTargetNode)
    val solvedExpressionAsString =
      spp.copy(selections = spp.selections ++ unsolvedPredicatesOnTargetNode)
        .solvedString

    val selector = convertFromIr(spp.selector)
    val nodeVariableGroupings = spp.allQuantifiedPathPatterns.flatMap(_.nodeVariableGroupings.map(convertFromIr))
    val relationshipVariableGroupings =
      spp.allQuantifiedPathPatterns.flatMap(_.relationshipVariableGroupings.map(convertFromIr))
    val nonInlinablePreFilters =
      Option.when(nonInlinedSelections.nonEmpty)(Ands.create(nonInlinedSelections.flatPredicates.to(ListSet)))

    val singletonVariables =
      (spp.coveredIds -- spp.allQuantifiedPathPatterns.flatMap(_.groupings) - startNode).map(varFor)
        .toSet[LogicalVariable]

    context.staticComponents.logicalPlanProducer.planStatefulShortest(
      sourcePlan,
      startNode,
      newEndNode,
      nfa,
      nonInlinablePreFilters,
      nodeVariableGroupings,
      relationshipVariableGroupings,
      singletonVariables,
      selector,
      maybeHiddenFilter,
      solvedExpressionAsString,
      solvedSpp,
      unsolvedPredicatesOnTargetNode,
      context
    )
  }

  private val convertFromIr: SelectivePathPattern.Selector => StatefulShortestPath.Selector = {
    // for now we will implement ANY via SHORTEST.
    case SelectivePathPattern.Selector.Any(k)            => StatefulShortestPath.Selector.Shortest(k)
    case SelectivePathPattern.Selector.ShortestGroups(k) => StatefulShortestPath.Selector.ShortestGroups(k)
    case SelectivePathPattern.Selector.Shortest(k)       => StatefulShortestPath.Selector.Shortest(k)
  }

  // this should go eventually when we have Variables instead of Strings in IR
  private def convertFromIr(variableGrouping: VariableGrouping) =
    Trail.VariableGrouping(varFor(variableGrouping.singletonName), varFor(variableGrouping.groupName))

}
