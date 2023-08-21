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
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.State
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.compileNodeJuxtapositionPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.compileRelationshipExpansionPredicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA.kernelDirection
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.RelationshipTypes
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.NodeJuxtaposition
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.RelationshipExpansion
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.State.VarName
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.relationship

import java.util.function.IntPredicate
import java.util.function.LongPredicate
import java.util.function.Predicate

case class CommandNFA(
  states: Set[State],
  startState: State,
  finalStates: Set[State]
) {

  /**
   * Compiles the CommandNFA into [[productgraph.State]]s.
   *
   * @param row row to compile the NFA for
   * @param queryState queryState to compile the NFA for
   * @return the start [[productgraph.State]] of the NFA.
   */
  def compile(row: CypherRow, queryState: QueryState): productgraph.State = {

    // This is then used to retrieve each state given the id, completing the transition -> targetState.id -> targetState
    // mapping
    val stateLookup = states.map(state =>
      state -> new productgraph.State(
        state.id,
        state.varName,
        null,
        null,
        startState == state,
        finalStates.contains(state)
      )
    ).toMap

    for ((state, pgState) <- stateLookup) {
      pgState.setNodeJuxtapositions(
        state.nodeTransitions.map(nt =>
          new NodeJuxtaposition(compileNodeJuxtapositionPredicate(nt, row, queryState), stateLookup(nt.targetState))
        ).toArray
      )

      pgState.setRelationshipExpansions(
        state.relTransitions.map(rt => {
          val (relPred, nodePred) = compileRelationshipExpansionPredicate(rt, row, queryState)
          new RelationshipExpansion(
            relPred,
            if (rt.types == null) null else rt.types.types(queryState.query),
            kernelDirection(rt.dir),
            rt.relVarName,
            nodePred,
            stateLookup(rt.targetState)
          )
        }).toArray
      )
    }

    stateLookup(startState)
  }
}

object CommandNFA {

  def kernelDirection(dir: SemanticDirection): Direction = {
    dir match {
      case SemanticDirection.OUTGOING => Direction.OUTGOING
      case SemanticDirection.INCOMING => Direction.INCOMING
      case SemanticDirection.BOTH     => Direction.BOTH
    }
  }

  type CommandPredicateFunction = (CypherRow, QueryState, AnyValue) => Boolean

  class State(
    val id: Int,
    val varName: VarName,
    var nodeTransitions: Seq[NodeJuxtapositionTransition],
    var relTransitions: Seq[RelationshipExpansionTransition]
  ) {

    override def hashCode(): Int = id.hashCode()

    override def equals(obj: Any): Boolean = {
      obj.isInstanceOf[State] && obj.asInstanceOf[State].id == this.id
    }
  }

  case class NodeJuxtapositionTransition(
    inner: Option[CommandPredicateFunction],
    targetState: State
  )

  case class RelationshipExpansionTransition(
    innerRelPred: Option[CommandPredicateFunction],
    relVarName: VarName,
    types: RelationshipTypes,
    dir: SemanticDirection,
    innerNodePred: Option[CommandPredicateFunction],
    targetState: State
  )

  def fromLogicalNFA(id: Id)(
    logicalNFA: NFA,
    stateCorrespondsToGroupVariable: IntPredicate,
    expressionConverters: ExpressionConverters,
    tokenContext: ReadTokenContext
  ): CommandNFA = {

    var startState: State = null
    val finalStates = Set.newBuilder[State]

    // We need to compile the NFA in two phases here due to potential cycles in the NFA

    // first phase: create the states
    val stateLookup = logicalNFA.states.iterator.map { logicalState =>
      val varName = new VarName(logicalState.variable.name, stateCorrespondsToGroupVariable.test(logicalState.id))
      val commandState = new State(logicalState.id, varName, null, null)

      if (logicalNFA.startState == logicalState) {
        assert(startState == null, "There should only be one start state in an NFA")
        startState = commandState
      }
      if (logicalNFA.finalStates.contains(logicalState)) {
        finalStates.addOne(commandState)
      }

      logicalState -> commandState
    }.toMap

    // second phase: add the transitions
    for (logicalState <- logicalNFA.states) {
      val transitions = logicalNFA.transitions.getOrElse(logicalState, Seq.empty)

      val (nodeTransitions, relTransitions) = transitions.partitionMap {

        case NFA.Transition(np: NFA.NodeJuxtapositionPredicate, end) =>
          Left(compileStubbedNodeJuxtaposition(id)(np, stateLookup(end), expressionConverters, tokenContext))

        case NFA.Transition(rp: NFA.RelationshipExpansionPredicate, end) =>
          Right(compileStubbedRelationshipExpansion(id)(
            rp,
            stateLookup(end),
            stateCorrespondsToGroupVariable.test(logicalState.id),
            expressionConverters,
            tokenContext
          ))
      }
      val commandState = stateLookup(logicalState)
      commandState.nodeTransitions = nodeTransitions.toSeq
      commandState.relTransitions = relTransitions.toSeq
    }

    CommandNFA(
      states = stateLookup.values.toSet,
      startState,
      finalStates.result()
    )
  }

  def compileStubbedNodeJuxtaposition(id: Id)(
    logicalPredicate: NFA.NodeJuxtapositionPredicate,
    end: State,
    expressionConverters: ExpressionConverters,
    tokenContext: ReadTokenContext
  ): NodeJuxtapositionTransition = {
    val commandPred = asCommand(id)(logicalPredicate.variablePredicate, expressionConverters, tokenContext)
    NodeJuxtapositionTransition(commandPred, end)
  }

  def compileStubbedRelationshipExpansion(id: Id)(
    logicalPredicate: NFA.RelationshipExpansionPredicate,
    end: State,
    relVarIsGroupVariable: Boolean,
    expressionConverters: ExpressionConverters,
    tokenContext: ReadTokenContext
  ): RelationshipExpansionTransition = {
    val commandRelPred = asCommand(id)(logicalPredicate.relPred, expressionConverters, tokenContext)
    val commandNodePred = asCommand(id)(logicalPredicate.nodePred, expressionConverters, tokenContext)

    // In planner land, empty type seq means all types. We use null in runtime land to represent all types,
    // (I think) since we may have a "real" empty type list when the original list only contained tokens
    // which don't exist in the DB
    val types = logicalPredicate.types
    val relTypes = if (types.isEmpty) null else RelationshipTypes(types.map(_.name).toArray, tokenContext)

    RelationshipExpansionTransition(
      commandRelPred,
      new VarName(logicalPredicate.relationshipVariable.name, relVarIsGroupVariable),
      relTypes,
      logicalPredicate.dir,
      commandNodePred,
      end
    )
  }

  def asCommand(id: Id)(
    variablePredicateOpt: Option[VariablePredicate],
    expressionConverters: ExpressionConverters,
    tokenContext: ReadTokenContext
  ): Option[CommandPredicateFunction] = {
    variablePredicateOpt.map { variablePredicate =>
      val command: predicates.Predicate =
        expressionConverters.toCommandPredicate(id, variablePredicate.predicate)
          .rewrite(KeyTokenResolver.resolveExpressions(_, tokenContext))
          .asInstanceOf[predicates.Predicate]

      val ev = ExpressionVariable.cast(variablePredicate.variable)
      (context: CypherRow, state: QueryState, entity: AnyValue) => {
        state.expressionVariables(ev.offset) = entity
        command.isTrue(context, state)
      }
    }
  }

  def compileNodeJuxtapositionPredicate(
    commandPredicate: NodeJuxtapositionTransition,
    row: CypherRow,
    state: QueryState
  ): LongPredicate = {
    commandPredicate.inner match {
      case Some(inner) => (l: Long) => inner(row, state, VirtualValues.node(l))
      case _           => LongPredicates.alwaysTrue()
    }
  }

  def compileRelationshipExpansionPredicate(
    commandPredicate: RelationshipExpansionTransition,
    row: CypherRow,
    state: QueryState
  ): (Predicate[RelationshipTraversalCursor], LongPredicate) = {
    (
      commandPredicate.innerRelPred match {
        case Some(inner) => (cursor: RelationshipTraversalCursor) => {
            inner(
              row,
              state,
              relationship(
                cursor.relationshipReference(),
                cursor.sourceNodeReference(),
                cursor.targetNodeReference(),
                cursor.`type`()
              )
            )
          }
        case _ => Predicates.alwaysTrue()
      },
      commandPredicate.innerNodePred match {
        case Some(inner) => (l: Long) => inner(row, state, VirtualValues.node(l))
        case _           => LongPredicates.alwaysTrue()
      }
    )
  }
}
