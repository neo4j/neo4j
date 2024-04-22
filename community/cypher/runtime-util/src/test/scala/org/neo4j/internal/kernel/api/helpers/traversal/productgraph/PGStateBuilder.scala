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

package org.neo4j.internal.kernel.api.helpers.traversal.productgraph

import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.RelationshipDataReader
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder.BuilderState

import java.util.function.LongPredicate
import java.util.function.Predicate

import scala.collection.mutable

/** A light wrapper over productgraph.State that autogenerates the state ID and facilitates addition of new transitions */
object PGStateBuilder {

  class BuilderState(val state: State) {

    def addNodeJuxtaposition(target: PGStateBuilder.BuilderState): Unit = {
      val existing = this.state.getNodeJuxtapositions
      val longer = new Array[NodeJuxtaposition](existing.length + 1)
      System.arraycopy(existing, 0, longer, 0, existing.length)
      longer(existing.length) = new NodeJuxtaposition(target.state)
      this.state.setNodeJuxtapositions(longer)
    }

    def addRelationshipExpansion(
      target: PGStateBuilder.BuilderState,
      relPredicate: Predicate[RelationshipDataReader] = Predicates.alwaysTrue(),
      types: Array[Int] = null,
      direction: Direction = Direction.BOTH
    ): Unit = {
      val existing = this.state.getRelationshipExpansions
      val longer = new Array[RelationshipExpansion](existing.length + 1)
      System.arraycopy(existing, 0, longer, 0, existing.length)
      longer(existing.length) =
        new RelationshipExpansion(relPredicate, types, direction, SlotOrName.none, target.state)
      this.state.setRelationshipExpansions(longer)
    }
  }
}

class PGStateBuilder {
  private val states = mutable.Map.empty[Int, BuilderState]
  private var startState: PGStateBuilder.BuilderState = null

  def newState(
    name: String = null,
    isStartState: Boolean = false,
    isFinalState: Boolean = false,
    predicate: LongPredicate = Predicates.ALWAYS_TRUE_LONG
  ): BuilderState = {
    val state = new BuilderState(new State(
      states.size,
      if (name == null) SlotOrName.none else SlotOrName.VarName(name, isGroup = false),
      predicate,
      new Array[NodeJuxtaposition](0),
      new Array[RelationshipExpansion](0),
      isStartState,
      isFinalState
    ))
    states += (state.state.id -> state)
    if (isStartState) {
      if (startState == null) {
        startState = state
      } else {
        throw new IllegalStateException("There is already a start state")
      }
    }
    state
  }

  def getState(id: Int): BuilderState = states(id)

  def getStart: PGStateBuilder.BuilderState = this.startState
  def stateCount: Int = this.states.size
}
