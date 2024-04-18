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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA.State
import org.neo4j.cypher.internal.logical.plans.NFA.Transition
import org.neo4j.cypher.internal.macros.AssertMacros

import scala.collection.immutable.ArraySeq
import scala.collection.mutable

/**
 * This class can be used to construct an NFA conveniently, without having to manually keep track of IDs.
 * This class is not thread-safe.
 *
 * A code example for the pattern
 * {{{(a)-[r]->(b) (c)}}}
 *
 * would look like this:
 * {{{
 *   val builder = new NFABuilder(varFor("a"))
 *
 *   val bState = builder.addAndGetState(varFor("b"))
 *   builder.addTransition(
 *     builder.getStartState,
 *     bState,
 *     RelationshipExpansionPredicate(
 *       varFor("r"),
 *       None,
 *       Seq.empty,
 *       SemanticDirection.OUTGOING,
 *       None
 *     )
 *   )
 *
 *   val cState = builder.addAndGetState(varFor("c"))
 *   builder.addTransition(
 *     bState,
 *     cState,
 *     NodeJuxtapositionPredicate(None)
 *   )
 *
 *   builder.setFinalState(cState)
 *   builder.build()
 *  }}}
 */
class NFABuilder protected (val startState: State) {

  private val states: mutable.SortedMap[Int, State] = mutable.SortedMap(startState.id -> startState)
  private var finalState: State = _
  private val transitions: mutable.MultiDict[Int, Transition] = mutable.MultiDict.empty
  private var nextId: Int = 0
  private var lastState: State = startState

  /**
   * @param startState the varName of the initial node of the start state of the NFA.
   */
  def this(startState: LogicalVariable) = {
    this(State(0, startState, None))
    nextId += 1
  }

  /**
   * Protected constructor to support TestNFABuilder.
   */
  protected def this(id: Int, startName: String) = {
    this(State(id, varFor(startName), None))
    nextId += 1
  }

  /**
   * To support TestNFABuilder.
   */
  protected def getState(id: Int): State = {
    states(id)
  }

  /**
   * To support TestNFABuilder.
   */
  protected def getOrCreateState(
    id: Int,
    variable: LogicalVariable,
    predicate: Option[VariablePredicate] = None
  ): State = {
    val state = states.getOrElseUpdate(id, State(id, variable, predicate))
    AssertMacros.checkOnlyWhenAssertionsAreEnabled(
      state.variable == variable,
      s"Found state with id $id to be `${state.variable.name}` instead of `${variable.name}`"
    )
    lastState = state
    state
  }

  def build(): NFA = {
    if (this.finalState == null) {
      throw new IllegalStateException("finalState not set")
    }

    val states = ArraySeq.from(this.states.values)

    val transitions = this.transitions.sets.view.mapValues(_.toSet).toMap

    NFA(states, transitions, startState.id, finalState.id)
  }

  def getStartState: State = startState

  def addAndGetState(variable: LogicalVariable, predicate: Option[VariablePredicate]): State = {
    val newState = State(this.nextId, variable, predicate)
    this.states += (newState.id -> newState)
    this.nextId = nextId + 1
    lastState = newState
    newState
  }

  def getLastState: State = lastState

  def setFinalState(finalState: State): NFABuilder = {
    if (this.finalState != null) {
      throw new IllegalStateException("finalState already set")
    }

    this.finalState = finalState
    this
  }

  def addTransition(from: State, to: State, nfaPredicate: NFA.Predicate): NFABuilder = {
    transitions.addOne(from.id -> Transition(nfaPredicate, to.id))
    this
  }
}
