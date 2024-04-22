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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.eclipse.collections.impl.block.factory.primitive.LongPredicates
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.RelationshipExpansionTransition
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.State
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.DirectionConverter.toGraphDb
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.function.Predicates
import org.neo4j.internal.kernel.api.RelationshipDataReader
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.NodeJuxtaposition
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.relationship

import java.util.function.LongPredicate
import java.util.function.Predicate

case class CommandNFA(
  states: Set[State],
  startState: State,
  finalState: State
) {

  /**
   * Compiles the CommandNFA into [[productgraph.State]]s.
   *
   * @param row row to compile the NFA for
   * @param queryState queryState to compile the NFA for
   * @return the start [[productgraph.State]] of the NFA.
   */
  def compile(row: CypherRow, queryState: QueryState): productgraph.State = {

    def statePredicate(state: State): LongPredicate =
      state.predicate match {
        case Some(predicate) => (l: Long) => predicate(row, queryState, VirtualValues.node(l))
        case _               => LongPredicates.alwaysTrue()
      }

    def relPredicate(transition: RelationshipExpansionTransition): Predicate[RelationshipDataReader] =
      transition.innerRelPred match {
        case Some(predicate) => (rel: RelationshipDataReader) => {
            predicate(
              row,
              queryState,
              relationship(
                rel.relationshipReference(),
                rel.sourceNodeReference(),
                rel.targetNodeReference(),
                rel.`type`()
              )
            )
          }
        case _ => Predicates.alwaysTrue()
      }

    // This is then used to retrieve each state given the id, completing the transition -> targetState.id -> targetState
    // mapping
    val stateLookup: Map[State, productgraph.State] = states.map(state =>
      state -> new productgraph.State(
        state.id,
        state.slotOrName,
        statePredicate(state),
        null,
        null,
        startState == state,
        finalState == state
      )
    ).toMap

    for ((state, pgState) <- stateLookup) {
      pgState.setNodeJuxtapositions(
        state.nodeTransitions.map(transition =>
          new NodeJuxtaposition(stateLookup(transition.targetState))
        ).toArray
      )

      pgState.setRelationshipExpansions(
        state.relTransitions.map(transition => {
          new RelationshipExpansion(
            relPredicate(transition),
            if (transition.types == null) null else transition.types.types(queryState.query),
            toGraphDb(transition.dir),
            transition.slotOrName,
            stateLookup(transition.targetState)
          )
        }).toArray
      )
    }

    stateLookup(startState)
  }
}

object CommandNFA {

  private type CommandPredicateFunction = (CypherRow, QueryState, AnyValue) => Boolean

  class State(
    val id: Int,
    val slotOrName: SlotOrName,
    val predicate: Option[CommandPredicateFunction],
    var nodeTransitions: Seq[NodeJuxtapositionTransition],
    var relTransitions: Seq[RelationshipExpansionTransition]
  ) {

    override def hashCode(): Int = id.hashCode()

    override def equals(obj: Any): Boolean = {
      obj.isInstanceOf[State] && obj.asInstanceOf[State].id == this.id
    }
  }

  case class NodeJuxtapositionTransition(
    targetState: State
  )

  case class RelationshipExpansionTransition(
    innerRelPred: Option[CommandPredicateFunction],
    slotOrName: SlotOrName,
    types: RelationshipTypes,
    dir: SemanticDirection,
    targetState: State
  )

  def fromLogicalNFA(
    logicalNFA: NFA,
    predicateToCommand: VariablePredicate => commands.predicates.Predicate,
    getSlotOrName: LogicalVariable => SlotOrName = x => SlotOrName.None
  )(implicit st: SemanticTable): CommandNFA = {

    def convertPredicate(varPredicate: VariablePredicate): CommandPredicateFunction = {
      val predicate = predicateToCommand(varPredicate)
      val offset = ExpressionVariable.cast(varPredicate.variable).offset
      (row: CypherRow, state: QueryState, entity: AnyValue) => {
        state.expressionVariables(offset) = entity
        predicate.isTrue(row, state)
      }
    }

    def compileStubbedRelationshipExpansion(
      logicalPredicate: NFA.RelationshipExpansionPredicate,
      end: State
    )(implicit st: SemanticTable): RelationshipExpansionTransition = {
      val commandRelPred = logicalPredicate.relPred.map(convertPredicate)

      // In planner land, empty type seq means all types. We use null in runtime land to represent all types
      val types = logicalPredicate.types
      val relTypes = if (types.isEmpty) null else RelationshipTypes(types.toArray)

      RelationshipExpansionTransition(
        commandRelPred,
        getSlotOrName(logicalPredicate.relationshipVariable),
        relTypes,
        logicalPredicate.dir,
        end
      )
    }

    var startState: State = null
    var finalState: State = null

    // We need to compile the NFA in two phases here due to potential cycles in the NFA

    // first phase: create the states
    val stateLookup = logicalNFA.states.iterator.map { logicalState =>
      val commandState = new State(
        logicalState.id,
        getSlotOrName(logicalState.variable),
        logicalState.predicate.map(convertPredicate),
        null,
        null
      )

      if (logicalNFA.startState == logicalState) {
        assert(startState == null, "There should only be one start state in an NFA")
        startState = commandState
      }
      if (logicalNFA.finalState == logicalState) {
        assert(finalState == null, "There should only be one final state in an NFA")
        finalState = commandState
      }

      logicalState.id -> commandState
    }.toMap

    // second phase: add the transitions
    for (logicalState <- logicalNFA.states) {
      val transitions = logicalNFA.transitions.getOrElse(logicalState.id, Seq.empty)

      val (nodeTransitions, relTransitions) = transitions.partitionMap {
        case NFA.NodeJuxtapositionTransition(endId) =>
          val end = logicalNFA.states(endId)
          Left(NodeJuxtapositionTransition(stateLookup(end.id)))

        case NFA.RelationshipExpansionTransition(rp: NFA.RelationshipExpansionPredicate, endId) =>
          val end = logicalNFA.states(endId)
          Right(compileStubbedRelationshipExpansion(rp, stateLookup(end.id)))
      }
      val commandState = stateLookup(logicalState.id)
      commandState.nodeTransitions = nodeTransitions.toSeq
      commandState.relTransitions = relTransitions.toSeq
    }

    CommandNFA(
      states = stateLookup.values.toSet,
      startState,
      finalState
    )
  }

}
