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
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion.Node
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.MultiRelationshipExpansion.Rel
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder.BuilderState

import java.util.function.LongPredicate
import java.util.function.Predicate

import scala.collection.mutable
import scala.reflect.ClassTag

/** A light wrapper over productgraph.State that autogenerates the state ID and facilitates addition of new transitions */
object PGStateBuilder {

  class BuilderState(val state: State) {

    def addNodeJuxtaposition(target: PGStateBuilder.BuilderState): Unit = {
      val nj = new NodeJuxtaposition(state, target.state)
      this.state.setNodeJuxtapositions(extend(this.state.getNodeJuxtapositions, nj))
      target.state.setReverseNodeJuxtapositions(extend(target.state.getReverseNodeJuxtapositions, nj))
    }

    def addRelationshipExpansion(
      target: PGStateBuilder.BuilderState,
      relPredicate: Predicate[RelationshipDataReader] = Predicates.alwaysTrue(),
      types: Array[Int] = null,
      direction: Direction = Direction.BOTH
    ): Unit = {
      val re = new RelationshipExpansion(this.state, relPredicate, types, direction, SlotOrName.none, target.state)
      this.state.setRelationshipExpansions(extend(this.state.getRelationshipExpansions, re))
      target.state.setReverseRelationshipExpansions(extend(target.state.getReverseRelationshipExpansions, re))
    }

    def addMultiRelationshipExpansion(
      target: PGStateBuilder.BuilderState,
      rels: Array[Rel],
      nodes: Array[Node]
    ): Unit = {
      val mre = new MultiRelationshipExpansion(this.state, rels, nodes, target.state)
      this.state.setMultiRelationshipExpansions(extend(this.state.getMultiRelationshipExpansions, mre))
      target.state.setReverseMultiRelationshipExpansions(extend(
        target.state.getReverseMultiRelationshipExpansions,
        mre
      ))
    }

    def addMultiRelationshipExpansion(target: BuilderState, builder: MultiRelationshipBuilder): Unit = {
      addMultiRelationshipExpansion(target, builder.rels.toArray, builder.nodes.toArray)
    }

    private def extend[T: ClassTag](existing: Array[T], item: T): Array[T] = {
      val arr = new Array[T](existing.length + 1)

      System.arraycopy(existing, 0, arr, 0, existing.length)
      arr(existing.length) = item
      arr
    }
  }

  case class MultiRelationshipBuilder(rels: Vector[Rel], nodes: Vector[Node]) {

    def r(
      name: String = null,
      predicate: Predicate[RelationshipDataReader] = Predicates.alwaysTrue(),
      types: Array[Int] = null,
      direction: Direction = Direction.BOTH
    ): MultiRelationshipBuilder =
      copy(rels =
        rels :+ new Rel(
          predicate,
          types,
          direction,
          if (name == null) SlotOrName.none else SlotOrName.VarName(name, isGroup = true)
        )
      )

    def n(
      name: String = null,
      predicate: LongPredicate = Predicates.ALWAYS_TRUE_LONG
    ): MultiRelationshipBuilder =
      copy(nodes =
        nodes :+ new Node(predicate, if (name == null) SlotOrName.none else SlotOrName.VarName(name, isGroup = true))
      )
  }

  object MultiRelationshipBuilder {
    def empty: MultiRelationshipBuilder = MultiRelationshipBuilder(Vector.empty, Vector.empty)

    def r(
      name: String = null,
      predicate: Predicate[RelationshipDataReader] = Predicates.alwaysTrue(),
      types: Array[Int] = null,
      direction: Direction = Direction.BOTH
    ): MultiRelationshipBuilder =
      empty.r(name, predicate, types, direction)
  }
}

class PGStateBuilder {
  private val states = mutable.Map.empty[Int, BuilderState]
  private var startState: PGStateBuilder.BuilderState = _

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

  def getStart: PGStateBuilder.BuilderState = this.states.values.find(_.state.isStartState).get
  def getFinal: PGStateBuilder.BuilderState = this.states.values.find(_.state.isFinalState).get
  def stateCount: Int = this.states.size
}
