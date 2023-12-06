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
import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.NFABuilder.State
import org.neo4j.cypher.internal.logical.plans.NFABuilder.StateImpl
import org.neo4j.cypher.internal.logical.plans.NFABuilder.Transition
import org.neo4j.cypher.internal.macros.AssertMacros

import scala.collection.mutable

object NFABuilder {

  /**
   * The method in this builder return their own representation of State.
   * We use a sealed trait with a private case class to forbid creating instance of State
   * outside of this builder. This was easiest since the apply method of a case class will be public
   * even if the constructor is private.
   */
  sealed trait State {
    def id: Int
    def variable: LogicalVariable
  }

  private case class StateImpl(id: Int, variable: LogicalVariable) extends State

  case class Transition(nfaPredicate: NFA.Predicate, end: State)
}

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
class NFABuilder protected (_startState: State) {
  private val states: mutable.Map[Int, LogicalVariable] = mutable.Map(_startState.id -> _startState.variable)
  private var startState: State = _startState
  private var finalState: State = _
  private val transitions: mutable.MultiDict[State, Transition] = mutable.MultiDict.empty
  private var nextId: Int = 0
  private var lastState: State = _startState

  /**
   * @param _startState the varName of the initial node of the start state of the NFA.
   */
  def this(_startState: LogicalVariable) = {
    this(StateImpl(0, _startState))
    nextId += 1
  }

  /**
   * Protected constructor to support TestNFABuilder.
   */
  protected def this(id: Int, startName: String) = {
    this(StateImpl(id, varFor(startName)))
    nextId += 1
  }

  /**
   * To support TestNFABuilder.
   */
  protected def getState(id: Int): State = {
    StateImpl(id, states(id))
  }

  /**
   * To support TestNFABuilder.
   */
  protected def getOrCreateState(id: Int, variable: LogicalVariable): State = {
    val state = StateImpl(id, states.getOrElseUpdate(id, variable))
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

    // Create a mapping from our own State to NFAState to avoid instantiating many identical copies of NFAStates.
    val stateToNFAState =
      this.states.map { case (key, value) => StateImpl(key, value) -> NFA.State(key, value) }.toMap[State, NFA.State]

    val states = this.states.map { case (key, value) => stateToNFAState(StateImpl(key, value)) }.toSet

    val transitions = this.transitions.sets.map {
      case (stateImpl: StateImpl, transitions) =>
        stateToNFAState(stateImpl) -> transitions.map {
          case Transition(nfaPredicate, end) =>
            plans.NFA.Transition(nfaPredicate, stateToNFAState(end))
        }.toSet
    }.toMap

    val startState = stateToNFAState(this.startState)
    val finalState = stateToNFAState(this.finalState)

    NFA(states, transitions, startState, finalState)
  }

  def getStartState: State = startState

  def addAndGetState(variable: LogicalVariable): State = {
    val newState = StateImpl(this.nextId, variable)
    this.states += (newState.id -> newState.variable)
    this.nextId = nextId + 1
    lastState = newState
    newState
  }

  def getLastState: State = lastState

  def setStartState(startState: State): NFABuilder = {
    this.startState = startState
    this
  }

  def setFinalState(finalState: State): NFABuilder = {
    if (this.finalState != null) {
      throw new IllegalStateException("finalState already set")
    }

    this.finalState = finalState
    this
  }

  def addTransition(from: State, to: State, nfaPredicate: NFA.Predicate): NFABuilder = {
    transitions.addOne(from -> Transition(nfaPredicate, to))
    this
  }
}
