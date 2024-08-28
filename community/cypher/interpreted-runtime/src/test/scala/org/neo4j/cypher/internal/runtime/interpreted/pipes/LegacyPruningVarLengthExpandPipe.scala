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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

/**
 * This implementation of pruning-var-expand is no longer used in production, but is used to testing purposes.
 */
case class LegacyPruningVarLengthExpandPipe(
  source: Pipe,
  fromName: String,
  toName: String,
  types: RelationshipTypes,
  dir: SemanticDirection,
  min: Int,
  max: Int,
  filteringStep: TraversalPredicates = TraversalPredicates.NONE
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  assert(min <= max)

  /*
  This algorithm has been implemented using a state machine. This Cypher statement shows the static connections between the states.

  create (ln:State  {name: "LoadNext", loads: "The start node of the expansion.", checksRelationshipUniqueness: false}),
         (pre:State {name: "PrePruning", loads: "The relationship iterator of the nodes before min length has been reached.", checksRelationshipUniqueness: true}),
         (pru:State {name: "Pruning", loads: "The relationship iterator of the nodes after min length has been reached.", checksRelationshipUniqueness: true}),
         (nil:State {name: "Empty", signals: "End of the line. No more expansions to be done."}),
         (ln)-[:GOES_TO {if: "Input iterator is not empty, load next input row and the start node from it"}]->(pre),
         (ln)-[:GOES_TO {if: "Input iterator is not empty"}]->(nil),
         (nil)-[:GOES_TO]->(nil),
         (pre)-[:GOES_TO {if: "Next step is still less than min"}]->(pre),
         (pre)-[:GOES_TO {if: "Next step is min or greater"}]->(pru),
         (pru)-[:GOES_TO {if: "Next step is still less than max"}]->(pru)

   Missing from this picture are the dynamic connections between the states - both `pre` and `pru` have a `whenEmptied`
   field that is followed once the loaded iterator has been emptied.
   */

  sealed trait State {
    // Note that the ExecutionContext part here can be null.
    // This code is used in a hot spot, and avoiding object creation is important.
    def next(): (State, CypherRow)
  }

  case object Empty extends State {
    override def next() = (this, null)
  }

  /**
   * Performs regular DFS for traversal of the var-length paths up to length (min-1), mapping to one node of the path.
   *
   * @param whenEmptied The state to return to when this node is done
   * @param node        The current node
   * @param path        The path so far. Only the first pathLength elements are valid.
   * @param pathLength  Length of the path so far
   * @param state       The QueryState
   * @param row         The current row we are adding reachable nodes to
   * @param expandMap   maps NodeID -> FullExpandDepths
   */
  class PrePruningDFS(
    whenEmptied: State,
    node: VirtualNodeValue,
    val path: Array[Long],
    val pathLength: Int,
    val state: QueryState,
    row: CypherRow,
    expandMap: LongObjectHashMap[FullExpandDepths]
  ) extends State with Expandable with CheckPath {

    private var rels: ClosingIterator[(VirtualRelationshipValue, VirtualNodeValue)] = _

    /*
    Loads the relationship iterator of the nodes before min length has been reached.
     */
    override def next(): (State, CypherRow) = {

      if (rels == null)
        rels = expand(row, node)

      while (rels.hasNext) {
        val (r, otherNode) = rels.next()
        val relId = r.id

        if (!seenRelationshipInPath(relId)) {
          val nextState: State = traverseRelationship(r, relId, otherNode)

          return nextState.next()
        }
      }

      if (pathLength >= self.min)
        (whenEmptied, rowFactory.copyWith(row, self.toName, node))
      else
        whenEmptied.next()
    }

    /**
     * Creates the appropriate state for following a relationship to the next node.
     */
    private def traverseRelationship(r: VirtualRelationshipValue, relId: Long, nextNode: VirtualNodeValue): State = {
      path(pathLength) = relId
      val nextPathLength = pathLength + 1
      if (nextPathLength >= self.min)
        new PruningDFS(
          whenEmptied = this,
          node = nextNode,
          path = path,
          pathLength = nextPathLength,
          state = state,
          row = row,
          expandMap = expandMap,
          updateMinFullExpandDepth = _ => {}
        )
      else
        new PrePruningDFS(
          whenEmptied = this,
          node = nextNode,
          path = path,
          pathLength = nextPathLength,
          state = state,
          row = row,
          expandMap = expandMap
        )
    }
  }

  /**
   * Performs DFS traversal, but omits traversing relationships that have been completely traversed (to the
   * remaining depth) before.
   *
   * Pruning and full expand depths
   * The full expand depth is an integer that denotes how deep a relationship has previously been explored. For
   * example, a full expand depth of 2 would mean that all nodes that can be reached by following this relationship
   * and maximally one more permitted relationship have already been visited. A relationship is permitted if is
   * conforms to the var-length predicates (which is controlled at expand time), and the relationship is not already
   * part of the current path.
   *
   * Before emitting a node (after relationship traversals), the PruningDFS updates the full expand depth of the
   * incoming relationship on the previous node in the path. The full expand depth is computed as
   * 1                 if the max path length is reached
   * A_VERY_BIG_VALUE  if the node has no relationships except the incoming one
   * d+1               otherwise, where d is the minimum outgoing full expand depth
   *
   * @param whenEmptied              The state to return to when this node is done
   * @param node                     The current node
   * @param path                     The path so far. Only the first pathLength elements are valid.
   * @param pathLength               Length of the path so far
   * @param state                    The QueryState
   * @param row                      The current row we are adding reachable nodes to
   * @param expandMap                maps NodeID -> FullExpandDepths
   * @param updateMinFullExpandDepth Method that is called on node completion. Updates the FullExpandDepth for
   *                                 incoming relationship in the previous node.
   *
   *                                 For this algorithm the incoming relationship is the one traversed to reach this
   *                                 node, while outgoing relationships are all other relationships connected to this
   *                                 node.
   **/
  class PruningDFS(
    whenEmptied: State,
    node: VirtualNodeValue,
    val path: Array[Long],
    val pathLength: Int,
    val state: QueryState,
    row: CypherRow,
    expandMap: LongObjectHashMap[FullExpandDepths],
    updateMinFullExpandDepth: Int => Unit
  ) extends State with Expandable with CheckPath {

    import FullExpandDepths.UNINITIALIZED

    var idx = 0
    var fullExpandDepths: FullExpandDepths = UNINITIALIZED

    override def next(): (State, CypherRow) = {

      if (pathLength < self.max) {
        if (fullExpandDepths == UNINITIALIZED)
          initiateRecursion()

        while (hasRelationships) {
          val currentRelIdx = nextRelationship()
          if (!haveFullyExploredTheRemainingDepthBefore(currentRelIdx)) {
            val (rel, nextNode) = fullExpandDepths.relAndNext(currentRelIdx)
            val relId = rel.id
            if (!seenRelationshipInPath(relId)) {
              path(pathLength) = relId
              val nextState = new PruningDFS(
                whenEmptied = this,
                node = nextNode,
                path = path,
                pathLength = pathLength + 1,
                state = state,
                row = row,
                expandMap = expandMap,
                updateMinFullExpandDepth = fullExpandDepths.depths(currentRelIdx) = _
              )
              return nextState.next()
            }
          }
        }

        expandMap.put(node.id(), fullExpandDepths)
      }

      updateMinFullExpandDepth(currentFullExpandDepth)

      (whenEmptied, rowFactory.copyWith(row, self.toName, node))
    }

    private def hasRelationships = idx < fullExpandDepths.relAndNext.length

    private def nextRelationship() = {
      val i = idx
      idx += 1
      i
    }

    private def depthLeft = self.max - pathLength

    private def haveFullyExploredTheRemainingDepthBefore(i: Int) = fullExpandDepths.depths(i) >= depthLeft

    private def currentFullExpandDepth: Int =
      if (pathLength == self.max) 1
      else {
        // The maximum amount of steps that have been fully explored is the minimum full expand depth of the
        // next relationships. The incoming relationship is not included, as that is the relationship that will be
        // updated.
        val incomingRelationship = path(pathLength - 1)
        fullExpandDepths.minOutgoingDepth(incomingRelationship) + 1
      }

    private def initiateRecursion() = {
      fullExpandDepths = expandMap.get(node.id())
      if (fullExpandDepths == null) {
        val relIter = expand(row, node)
        fullExpandDepths = FullExpandDepths(relIter.toArray)
      }
    }
  }

  class LoadNext(private val input: ClosingIterator[CypherRow], val state: QueryState) extends State with Expandable {

    override def next(): (State, CypherRow) = {
      def nextState(row: CypherRow, node: VirtualNodeValue) = {
        val nextState = new PrePruningDFS(
          whenEmptied = this,
          node = node,
          path = new Array[Long](max),
          pathLength = 0,
          state = state,
          row = row,
          expandMap = new LongObjectHashMap[FullExpandDepths]()
        )
        nextState.next()
      }

      if (input.isEmpty) {
        (Empty, null)
      } else {
        val row = input.next()
        row.getByName(fromName) match {
          case node: VirtualNodeValue =>
            nextState(row, node)
          case x: Value if x == Values.NO_VALUE =>
            (Empty, null)
          case _ =>
            throw new InternalException(s"Expected a node on `$fromName`")
        }
      }
    }
  }

  trait CheckPath {
    def pathLength: Int

    val path: Array[Long]

    def seenRelationshipInPath(r: Long): Boolean = {
      if (pathLength == 0) return false
      var idx = 0
      while (idx < pathLength) {
        if (path(idx) == r) return true
        idx += 1
      }
      false
    }
  }

  trait Expandable {
    def state: QueryState

    /**
     * List all relationships of a node, given the predicates of this pipe.
     */
    def expand(
      row: CypherRow,
      node: VirtualNodeValue
    ): ClosingIterator[(VirtualRelationshipValue, VirtualNodeValue)] = {
      val relationships = state.query.getRelationshipsForIds(node.id(), dir, types.types(state.query))
      PrimitiveLongHelper.map(
        relationships,
        r => (VirtualValues.relationship(r), VirtualValues.node(relationships.otherNodeId(node.id())))
      ).filter {
        case (rel, other) => filteringStep.filterRelationship(row, state, rel, node, other) &&
          filteringStep.filterNode(row, state, other)
      }
    }
  }

  object FullExpandDepths {
    def apply(rels: Array[(VirtualRelationshipValue, VirtualNodeValue)]) = new FullExpandDepths(rels)

    val UNINITIALIZED: FullExpandDepths = null
  }

  class FullExpandDepths( // all relationships that connect to this node, filtered by the var-length predicates
    val relAndNext: Array[(VirtualRelationshipValue, VirtualNodeValue)]) {
    // The fully expanded depth for each relationship in rels
    val depths = new Array[Int](relAndNext.length)

    /**
     * Computes the minimum outgoing full expanded depth.
     *
     * @param incomingRelId id of the incoming relationship, which is omitted from this computation
     * @return the full expand depth
     */
    def minOutgoingDepth(incomingRelId: Long): Int = {
      var min = Integer.MAX_VALUE >> 1 // we don't want it to overflow
      var i = 0
      while (i < relAndNext.length) {
        val (rel, _) = relAndNext(i)
        if (rel.id() != incomingRelId)
          min = math.min(depths(i), min)
        i += 1
      }
      min
    }
  }

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] =
    new ClosingIterator[CypherRow] {

      var (stateMachine, current) = new LoadNext(input, state).next()

      override protected[this] def closeMore(): Unit = ()

      override def innerHasNext: Boolean = current != null

      override def next(): CypherRow = {
        if (current == null) {
          // fail
          Iterator.empty.next()
        }
        val temp = current
        val (nextState, nextCurrent) = stateMachine.next()
        stateMachine = nextState
        current = nextCurrent
        temp
      }
    }
}
