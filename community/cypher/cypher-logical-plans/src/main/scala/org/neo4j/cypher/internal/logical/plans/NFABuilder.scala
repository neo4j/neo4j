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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.logical.plans
import org.neo4j.cypher.internal.logical.plans.NFA.State.GroupVarName
import org.neo4j.cypher.internal.logical.plans.NFA.State.SingletonVarName
import org.neo4j.cypher.internal.logical.plans.NFA.State.VarName
import org.neo4j.cypher.internal.logical.plans.NFABuilder.State
import org.neo4j.cypher.internal.logical.plans.NFABuilder.StateImpl
import org.neo4j.cypher.internal.logical.plans.NFABuilder.Transition
import org.neo4j.cypher.internal.logical.plans.NFABuilder.asVarName

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
    def name: VarName
  }

  private case class StateImpl(id: Int, name: VarName) extends State

  case class Transition(nfaPredicate: NFA.Predicate, end: State)

  def asVarName(name: String, groupVar: Boolean): VarName = {
    if (groupVar) GroupVarName(name) else SingletonVarName(name)
  }
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
 *   val builder = new NFABuilder(SingletonVarName("a"))
 *
 *   val bState = builder.addAndGetState(SingletonVarName("b"))
 *   builder.addTransition(
 *     builder.getStartState,
 *     bState,
 *     RelationshipExpansionPredicate(
 *       SingletonVarName("r"),
 *       None,
 *       Seq.empty,
 *       SemanticDirection.OUTGOING,
 *       None
 *     )
 *   )
 *
 *   val cState = builder.addAndGetState(SingletonVarName("c"))
 *   builder.addTransition(
 *     bState,
 *     cState,
 *     NodeJuxtapositionPredicate(None)
 *   )
 *
 *   builder.addFinalState(cState)
 *   builder.build()
 *  }}}
 */
class NFABuilder protected (_startState: State) {
  private val states: mutable.Map[Int, VarName] = mutable.Map(_startState.id -> _startState.name)
  private var startState: State = _startState
  private val finalStates: mutable.Set[State] = mutable.Set.empty
  private val transitions: mutable.MultiDict[State, Transition] = mutable.MultiDict.empty
  private var nextId: Int = 0

  /**
   * @param _startState the varName of the initial node of the start state of the NFA.
   */
  def this(_startState: VarName) = {
    this(StateImpl(0, _startState))
    nextId += 1
  }

  /**
   * Protected constructor to support TestNFABuilder.
   */
  protected def this(id: Int, startName: String, groupVar: Boolean) = {
    this(StateImpl(id, asVarName(startName, groupVar)))
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
  protected def getOrCreateState(id: Int, name: String, groupVar: Boolean): State = {
    StateImpl(id, states.getOrElseUpdate(id, asVarName(name, groupVar)))
  }

  def build(): NFA = {
    // Create a mapping from our own State to NFAState to avoid instantiating many identical copies of NFAStates.
    val stateToNFAState =
      this.states.map { case (key, value) => StateImpl(key, value) -> NFA.State(key, value) }.toMap[State, NFA.State]

    val states = this.states.map { case (key, value) => stateToNFAState(StateImpl(key, value)) }.toSet

    val transitions = this.transitions.sets.map {
      case (stateImpl: StateImpl, transitions) =>
        stateToNFAState(stateImpl) -> transitions.map {
          case Transition(nfaPredicate, end) => plans.NFA.Transition(nfaPredicate, stateToNFAState(end))
        }.toSet
    }.toMap

    val startState = stateToNFAState(this.startState)

    val finalStates = this.finalStates.toSet.map(stateToNFAState.apply)

    NFA(states, transitions, startState, finalStates)
  }

  def getStartState: State = startState

  def addAndGetState(name: VarName): State = {
    val newState = StateImpl(this.nextId, name)
    this.states += (newState.id -> newState.name)
    this.nextId = nextId + 1
    newState
  }

  def setStartState(startState: State): NFABuilder = {
    this.startState = startState
    this
  }

  def addFinalState(finalState: State): NFABuilder = {
    this.finalStates += finalState
    this
  }

  def addTransition(from: State, to: State, nfaPredicate: NFA.Predicate): NFABuilder = {
    transitions.addOne(from -> Transition(nfaPredicate, to))
    this
  }
}
