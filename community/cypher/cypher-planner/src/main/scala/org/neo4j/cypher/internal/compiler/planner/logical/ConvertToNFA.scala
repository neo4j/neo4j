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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipUniquenessPredicate
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.VarLengthLowerBound
import org.neo4j.cypher.internal.expressions.VarLengthUpperBound
import org.neo4j.cypher.internal.expressions.functions.EndNode
import org.neo4j.cypher.internal.expressions.functions.StartNode
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.ir.ExhaustiveNodeConnection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ast.IRExpression
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NFABuilder
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.exceptions.InternalException

import scala.collection.immutable.ListSet

object ConvertToNFA {

  /**
   * @return (
   *           the NFA representing the selective path pattern,
   *           the selections from the spp that could not be inlined,
   *           map for each var-length relationship the synthetic singleton relationship
   *         )
   */
  def convertToNfa(
    spp: SelectivePathPattern,
    fromLeft: Boolean,
    availableSymbols: Set[LogicalVariable],
    predicatesOnTargetNode: Seq[Expression],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): (NFA, Selections, Map[LogicalVariable, LogicalVariable]) = {
    val firstNode = if (fromLeft) spp.left else spp.right

    val builder = new NFABuilder(firstNode)
    val connections = spp.pathPattern.connections
    val directedConnections = if (fromLeft) connections else connections.reverse

    val syntheticVarLengthSingletons = spp.varLengthRelationships.map { rel =>
      val singletonRelName = Namespacer.genName(
        anonymousVariableNameGenerator,
        rel.name
      )
      rel -> varFor(singletonRelName)
    }.toMap

    val nonInlinedSelections =
      convertToNfa(
        builder,
        directedConnections,
        spp.selections ++ predicatesOnTargetNode,
        fromLeft,
        availableSymbols,
        anonymousVariableNameGenerator,
        syntheticVarLengthSingletons
      )

    val lastNode = builder.getLastState
    builder.setFinalState(lastNode)
    (builder.build(), nonInlinedSelections, syntheticVarLengthSingletons)
  }

  /**
   * Return True if the given expression
   * - does depend on at least one of the given entities
   * - does not contain any IR expressions.
   */
  def canBeInlined(expression: Expression, entities: Set[LogicalVariable]): Boolean =
    expression.folder.treeFindByClass[IRExpression].isEmpty &&
      (expression.dependencies intersect entities).nonEmpty

  /**
   *
   * @return the selections that could not be inlined
   */
  private def convertToNfa(
    builder: NFABuilder,
    connections: NonEmptyList[ExhaustiveNodeConnection],
    selections: Selections,
    fromLeft: Boolean,
    availableSymbols: Set[LogicalVariable],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    syntheticVarLengthSingleton: Map[LogicalVariable, LogicalVariable]
  ): Selections = {
    // we cannot inline uniqueness predicates but we do not have to solve them as the algorithm for finding shortest paths will do that.
    val selectionsWithoutUniquenessPredicates = selections.filter(_.expr match {
      case _: RelationshipUniquenessPredicate => false
      case _                                  => true
    })

    /**
     * Return the top level predicates that only depend on availableSymbols and entities, but also use
     * at least one entity.
     */
    def getTopLevelPredicates(entities: Set[LogicalVariable]): ListSet[Expression] =
      getPredicates(selectionsWithoutUniquenessPredicates, entities)

    /**
     * Return the subset of the given predicates that only depend on availableSymbols and entities, but also use
     * at least one entity.
     */
    def getPredicates(selections: Selections, entities: Set[LogicalVariable]): ListSet[Expression] =
      selections.predicatesGiven(availableSymbols ++ entities)
        .filter(canBeInlined(_, entities))
        .to(ListSet)

    def getVariablePredicates(entity: LogicalVariable): (ListSet[Expression], Option[VariablePredicate]) = {
      val entityPredicates = getTopLevelPredicates(Set(entity))
      val entityVariablePredicates = toVariablePredicates(entity, entityPredicates)
      (entityPredicates, entityVariablePredicates)
    }

    /**
     * Get predicates that only depend on the relationship, its start and end node.
     * Rewrite these predicates to use `startNode` and `endNode`, so that they only
     * depend on the relationship.
     * 
     * @param dir the direction of the relationship.
     *            We cannot inline extra predicates for relationships with direction BOTH
     * @param sourceVariable the variable of the source NFA state
     * @param relationshipVariable the relationship variable
     * @param targetVariable the variable of the target NFA state
     * @param alreadySolvedPredicates predicates that are already solved otherwise.
     * @return a set of all extra predicates, in tuples: (original predicate, rewritten predicate)
     */
    def getExtraRelationshipPredicates(
      dir: SemanticDirection,
      sourceVariable: LogicalVariable,
      relationshipVariable: LogicalVariable,
      targetVariable: LogicalVariable,
      alreadySolvedPredicates: ListSet[Expression]
    ): ListSet[(Expression, Expression)] = {
      val (startNode, endNode) = (dir, fromLeft) match {
        case (SemanticDirection.BOTH, _) =>
          // We cannot inline extra predicates for relationships with direction BOTH
          return ListSet.empty
        case (SemanticDirection.OUTGOING, true)  => (sourceVariable, targetVariable)
        case (SemanticDirection.OUTGOING, false) => (targetVariable, sourceVariable)
        case (SemanticDirection.INCOMING, true)  => (targetVariable, sourceVariable)
        case (SemanticDirection.INCOMING, false) => (sourceVariable, targetVariable)
      }

      def rewrite(expression: Expression): Expression = expression.endoRewrite(topDown(Rewriter.lift {
        case `startNode` => StartNode(relationshipVariable)(InputPosition.NONE)
        case `endNode`   => EndNode(relationshipVariable)(InputPosition.NONE)
        case AndedPropertyInequalities(v, _, inequalities) if v == startNode || v == endNode =>
          Ands.create(inequalities.map(rewrite).toListSet)
      }))

      val allPredicatesGiven = getTopLevelPredicates(Set(
        sourceVariable,
        relationshipVariable,
        targetVariable
      ))
        // We cannot rewrite IRExpressions, since they contain Variables as strings
        .filter(_.folder.treeFindByClass[IRExpression].isEmpty)
        // DesugaredMapProjection cannot ge rewritten. They must have a variable as the first child, not an expression.
        .filter(_.folder.treeFindByClass[DesugaredMapProjection].isEmpty)

      val allApplicablePredicates = allPredicatesGiven -- alreadySolvedPredicates
      allApplicablePredicates.map(p => p -> rewrite(p))
    }

    // go over the node connections and keep track of selections we could inline
    val (_, inlinedSelections) = connections.foldLeft((builder, Selections.empty)) {
      case ((builder, inlinedSelections), nodeConnection) =>
        def addRelationshipBetweenStates(
          relationshipVariable: LogicalVariable,
          dir: SemanticDirection,
          types: Seq[RelTypeName],
          sourceState: NFABuilder.State,
          targetState: NFABuilder.State
        ): Selections = {
          val directionToPlan = if (fromLeft) dir else dir.reversed

          val target = targetState.variable

          val relPredicates = getTopLevelPredicates(Set(relationshipVariable))
          val nodePredicates = getTopLevelPredicates(Set(target))

          val extraRelPredicates = getExtraRelationshipPredicates(
            dir,
            sourceState.variable,
            relationshipVariable,
            targetState.variable,
            inlinedSelections.flatPredicates.to(ListSet) ++ relPredicates ++ nodePredicates
          )

          val relVariablePredicates =
            toVariablePredicates(relationshipVariable, relPredicates ++ extraRelPredicates.map(_._2))
          val nodeVariablePredicates = toVariablePredicates(target, nodePredicates)

          builder.addTransition(
            sourceState,
            targetState,
            NFA.RelationshipExpansionPredicate(
              relationshipVariable = relationshipVariable,
              relPred = relVariablePredicates,
              types = types,
              dir = directionToPlan,
              nodePred = nodeVariablePredicates
            )
          )
          Selections.from(relPredicates ++ nodePredicates ++ extraRelPredicates.map(_._1))
        }

        def addRelationship(
          relationship: LogicalVariable,
          target: LogicalVariable,
          dir: SemanticDirection,
          types: Seq[RelTypeName]
        ): Selections = {
          val sourceState = builder.getLastState
          val targetState = builder.addAndGetState(target)
          addRelationshipBetweenStates(relationship, dir, types, sourceState, targetState)
        }

        val newlyInlinedSelections = nodeConnection match {
          case PatternRelationship(relationship, (left, right), dir, types, SimplePatternLength) =>
            val target = if (fromLeft) right else left
            addRelationship(relationship, target, dir, types)

          case PatternRelationship(relationship, (left, right), dir, types, VarPatternLength(lowerBound, max)) =>
            /*
             * We introduce anonymous nodes for the nodes that we traverse while evaluating a var-length relationship.
             * Furthermore, we try to use a similar logic to that from QPPs also for var-length relationships.
             * That is, we
             * 1. Jump to the first node that is part of the var-length relationship via juxtaposing sourceState to innerState
             * 2. Reiterate the var-length relationship as many times as the lower bound needs us to.
             * Now we could potentially exit the var-length relationship. But we also need to make sure that the upper bound is observed by
             * 3. a) Either reiterating the pattern until we reach the upper bound or
             *    b) adding a relationship to the state itself, so that we can re-iterate the relationship indefinitely
             * 4. For all states which could potentially exit the var-length relationship, we juxtapose exitableState to targetState.
             *
             * E.g. For the pattern `(start)-[r*2..3]->(end)`
             *
             *
             *
             *                                                                                ┌───────────────────────────────────────────┐
             *                                                                                |                                   [4]────>|
             *                                                                                |                                    |      v
             * ┌──────────┐     ┌───────────┐  ()-[r]->()   ┌───────────┐  ()-[r]->()   ┌───────────┐  ()-[r]->()   ┌───────────┐  v  ╔════════╗
             * │ 0, start │ ──> │ 1, anon_1 │ ────────────> │ 2, anon_2 │ ────────────> │ 3, anon_3 │ ────────────> │ 4, anon_4 │ ──> ║ 5, end ║
             * └──────────┘     └───────────┘               └───────────┘               └───────────┘               └───────────┘     ╚════════╝
             *               ^  \__________________________________________________________________/ \__________________________/
             *               |                                   |                                               |
             *              [1]                                 [2]                                            [3.a]
             *
             *
             * (kudos to https://github.com/ggerganov/dot-to-ascii)
             */
            val singletonRelationship = syntheticVarLengthSingleton(relationship)
            val sourceState = builder.getLastState

            val innerState = builder.addAndGetState(varFor(anonymousVariableNameGenerator.nextName))
            builder.addTransition(
              sourceState,
              innerState,
              NFA.NodeJuxtapositionPredicate(None)
            )

            for (_ <- 1 to lowerBound) {
              addRelationship(singletonRelationship, varFor(anonymousVariableNameGenerator.nextName), dir, types)
            }

            val exitableState = builder.getLastState
            val furtherExitableStates = max match {
              case Some(upperBound) =>
                for (_ <- lowerBound until upperBound) yield {
                  addRelationship(
                    singletonRelationship,
                    varFor(anonymousVariableNameGenerator.nextName),
                    dir,
                    types
                  )
                  builder.getLastState
                }
              case None =>
                val targetState = exitableState
                addRelationshipBetweenStates(
                  singletonRelationship,
                  dir,
                  types,
                  exitableState,
                  targetState
                )
                Seq.empty
            }

            val target = if (fromLeft) right else left
            val targetState = builder.addAndGetState(target)
            val (predicatesOnTargetNode, variablePredicateOnTargetNode) = getVariablePredicates(target)
            (exitableState +: furtherExitableStates).foreach { exitableState =>
              builder.addTransition(
                exitableState,
                targetState,
                NFA.NodeJuxtapositionPredicate(variablePredicateOnTargetNode)
              )
            }

            // The part of the automaton that we created should make sure that we observe the lower and upper bound
            // of the var-length relationship. We therefore can claim to have solved these predicates.
            val varLengthPredicates = selections.flatPredicates.filter {
              case VarLengthLowerBound(`relationship`, `lowerBound`) => true
              case VarLengthUpperBound(`relationship`, upperBound) if max.contains(upperBound.intValue) =>
                true
              case _ => false
            }
            Selections.from(predicatesOnTargetNode ++ varLengthPredicates)

          case QuantifiedPathPattern(
              leftBinding,
              rightBinding,
              patternRelationships,
              _,
              qppSelections,
              repetition,
              _,
              _
            ) =>
            /*
             * When adding a QPP, we need to
             * 1. Jump into the pattern via juxtaposing sourceOuter to sourceInner
             * 2. Reiterate the pattern within the QPP as many times as the lower bound needs us to.
             * Now we could potentially exit the QPP. But we also need to make sure that the upper bound is observed by
             * 3. a) Either reiterating the pattern until we reach the UpperBound or
             *    b) adding a transition back, so that we can run the last iteration of the QPP again.
             * 4. For all states which could potentially exit the QPP, we juxtapose targetInner to targetOuter.
             * 5. If there is a lower bound of 0, we need to add an extra transition, skipping the whole inner pattern of the QPP.
             *
             * E.g. For the pattern `(start) ((a)-[r]->(b)) {2, 3} (end)`
             *
             *
             *
             *                                                                                ┌─────────────────────────────────────────────────┐
             *                                                                                |                                         [4]────>|
             *                                                                                |                                          |      v
             * ┌──────────┐     ┌──────┐  ()-[r]->()   ┌──────┐     ┌──────┐  ()-[r]->()   ┌──────┐     ┌──────┐  ()-[r]->()   ┌──────┐  v  ╔════════╗
             * │ 0, start │ ──> │ 1, a │ ────────────> │ 2, b │ ──> │ 3, a │ ────────────> │ 4, b │ ──> │ 5, a │ ────────────> │ 6, b │ ──> ║ 7, end ║
             * └──────────┘     └──────┘               └──────┘     └──────┘               └──────┘     └──────┘               └──────┘     ╚════════╝
             *               ^  \_________________________________________________________________/ \________________________________/
             *               |                                   |                                                  |
             *              [1]                                 [2]                                               [3.a]
             *
             *
             * (kudos to https://github.com/ggerganov/dot-to-ascii)
             */

            // === 1. Add entry juxtaposition ===
            val sourceBinding = if (fromLeft) leftBinding else rightBinding
            val sourceOuterState = builder.getLastState
            val sourceInner = sourceBinding.inner
            // var because it will get overwritten if the lower bound is > 1
            var lastSourceInnerState = builder.addAndGetState(sourceInner)
            val predicatesOnSourceInner =
              getPredicates(qppSelections, availableSymbols + sourceInner)
            val variablePredicateOnSourceInner =
              toVariablePredicates(sourceInner, predicatesOnSourceInner.to(ListSet))
            builder.addTransition(
              sourceOuterState,
              lastSourceInnerState,
              NFA.NodeJuxtapositionPredicate(variablePredicateOnSourceInner)
            )

            // === 2.a) Add inner transitions ===
            val relsInOrder = if (fromLeft) patternRelationships else patternRelationships.reverse

            def addQppInnerTransitions(): Selections =
              convertToNfa(
                builder,
                relsInOrder,
                qppSelections -- predicatesOnSourceInner,
                fromLeft,
                availableSymbols,
                anonymousVariableNameGenerator,
                syntheticVarLengthSingleton
              )

            val nonInlinedQppSelections = addQppInnerTransitions()
            if (nonInlinedQppSelections.nonEmpty) {
              throw new InternalException(s"$nonInlinedQppSelections could not be inlined into NFA")
            }
            // === 2.b) Unrolling for lower bound ===
            // If the lower bound is larger than 1, repeat the inner steps of the QPP (min - 1) times.
            for (_ <- 1L to (repetition.min - 1)) {
              val targetInnerState = builder.getLastState
              lastSourceInnerState = builder.addAndGetState(sourceInner)
              builder.addTransition(
                targetInnerState,
                lastSourceInnerState,
                NFA.NodeJuxtapositionPredicate(variablePredicateOnSourceInner)
              )
              addQppInnerTransitions()
            }

            // 3. By unrolling, we have reached the first target inner state from which we can exit the QPP.
            val exitableTargetInnerState = builder.getLastState
            val furtherExitableTargetInnerStates = repetition.max match {
              case UpperBound.Unlimited =>
                builder.addTransition(
                  exitableTargetInnerState,
                  lastSourceInnerState,
                  NFA.NodeJuxtapositionPredicate(variablePredicateOnSourceInner)
                )
                Seq.empty
              case UpperBound.Limited(max) =>
                for (_ <- Math.max(repetition.min, 1) until max) yield {
                  val targetInnerState = builder.getLastState
                  val sourceInnerState = builder.addAndGetState(sourceInner)
                  builder.addTransition(
                    targetInnerState,
                    sourceInnerState,
                    NFA.NodeJuxtapositionPredicate(variablePredicateOnSourceInner)
                  )
                  addQppInnerTransitions()
                  builder.getLastState
                }
            }
            val exitableTargetInnerStates = exitableTargetInnerState +: furtherExitableTargetInnerStates

            // === 4. Add exit juxtapositions ===
            // Connect all exitableTargetInnerStates with the targetOuterState
            val targetBinding = if (fromLeft) rightBinding else leftBinding
            val targetOuter = targetBinding.outer
            val targetOuterState = builder.addAndGetState(targetOuter)
            val predicatesOnTargetOuter = getTopLevelPredicates(Set(targetOuter))

            val variablePredicateOnTargetOuter =
              toVariablePredicates(targetOuter, predicatesOnTargetOuter.to(ListSet))
            exitableTargetInnerStates.foreach { targetInnerState =>
              builder.addTransition(
                targetInnerState,
                targetOuterState,
                NFA.NodeJuxtapositionPredicate(variablePredicateOnTargetOuter)
              )
            }

            // 5. For a repetition lower bound of 0, we need to add this shortcut around the QPP pattern
            if (repetition.min == 0) {
              builder.addTransition(
                sourceOuterState,
                targetOuterState,
                NFA.NodeJuxtapositionPredicate(variablePredicateOnTargetOuter)
              )
            }
            Selections.from(predicatesOnSourceInner ++ predicatesOnTargetOuter)
        }
        (builder, inlinedSelections ++ newlyInlinedSelections)
    }
    selectionsWithoutUniquenessPredicates -- inlinedSelections
  }

  private def toVariablePredicates(
    variable: LogicalVariable,
    predicates: ListSet[Expression]
  ): Option[VariablePredicate] = {
    Option.when(predicates.nonEmpty)(VariablePredicate(variable, Ands.create(predicates)))
  }
}
