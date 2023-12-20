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

import org.eclipse.collections.api.map.primitive.MutableLongObjectMap
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.HeapEstimator.shallowSizeOfInstance
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

case class PruningVarLengthExpandPipe(
  source: Pipe,
  fromName: String,
  toName: String,
  types: RelationshipTypes,
  dir: SemanticDirection,
  min: Int,
  max: Int,
  filteringStep: VarLengthPredicate = VarLengthPredicate.NONE
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  require(min <= max)

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
   *
   *   1                 if the max path length is reached
   *   inf               if the node has no relationships except the incoming one
   *   d+1               if at or above minLength, or if the node has been emitted
   *                       d is the minimum outgoing full expand depth
   *   0                 otherwise
   *
   * Full expand depth always increase, so if a newly computed full expand depth is smaller than the previous value,
   * the new depth is ignored.
   *
   * @param state                    The state to return to when this node is done
   * @param node                     The current node
   * @param path                     The path so far. Only the first pathLength elements are valid.
   * @param pathLength               Length of the path so far
   * @param queryState               The QueryState
   * @param row                      The current row we are adding reachable nodes to
   * @param expandMap                maps NodeID -> NodeState
   * @param prevLocalRelIndex        index of the incoming relationship in the NodeState
   * @param prevNodeState            The NodeState of the previous node in the path
   *
   * For this algorithm the incoming relationship is the one traversed to reach this
   * node, while outgoing relationships are all other relationships connected to this
   * node.
   **/
  class PruningDFS(
    val state: FullPruneState,
    val node: VirtualNodeValue,
    val path: Array[Long],
    val pathLength: Int,
    val queryState: QueryState,
    val row: CypherRow,
    val expandMap: MutableLongObjectMap[NodeState],
    val prevLocalRelIndex: Int,
    val prevNodeState: NodeState
  ) {

    var nodeState: NodeState = NodeState.UNINITIALIZED
    var relationshipCursor = 0

    def nextEndNode(): VirtualNodeValue = {

      initiate(state.memoryTracker)

      if (pathLength < self.max) {

        nodeState.ensureExpanded(queryState, row, node)

        while (hasRelationship) {
          val currentRelIdx = nextRelationship()
          if (!haveFullyExploredTheRemainingStepsBefore(currentRelIdx)) {
            val (rel, nextNode) = nodeState.relAndNext(currentRelIdx)
            val relId = rel.id()
            if (!seenRelationshipInPath(relId)) {
              path(pathLength) = relId
              val endNode = state.push(
                node = nextNode,
                pathLength = pathLength + 1,
                expandMap = expandMap,
                prevLocalRelIndex = currentRelIdx,
                prevNodeState = nodeState
              )

              if (endNode != null)
                return endNode
              else
                state.pop()
            }
          }
        }
      }

      updatePrevFullExpandDepth()

      if (!nodeState.isEmitted && pathLength >= self.min) {
        nodeState.isEmitted = true
        node
      } else {
        null
      }
    }

    private def hasRelationship: Boolean = relationshipCursor < nodeState.relAndNext.length

    private def nextRelationship(): Int = {
      val next = relationshipCursor
      relationshipCursor += 1
      next
    }

    private def seenRelationshipInPath(r: Long): Boolean = {
      if (pathLength == 0) return false
      var idx = 0
      while (idx < pathLength) {
        if (path(idx) == r) return true
        idx += 1
      }
      false
    }

    private def haveFullyExploredTheRemainingStepsBefore(i: Int) = {
      val stepsLeft = self.max - pathLength
      nodeState.depths(i) >= stepsLeft
    }

    private def updatePrevFullExpandDepth(): Unit = {
      if (pathLength > 0) {
        val requiredStepsFromPrev = math.max(0, self.min - pathLength + 1)
        if (requiredStepsFromPrev <= 1 || nodeState.isEmitted) {
          prevNodeState.updateFullExpandDepth(prevLocalRelIndex, currentOutgoingFullExpandDepth() + 1)
        }
      }
    }

    private def currentOutgoingFullExpandDepth(): Int = {
      require(pathLength > 0)
      if (pathLength == self.max) 0
      else {
        // The maximum amount of steps that have been fully explored is the minimum full expand depth of the
        // outgoing relationships. The incoming relationship is not included, as that is the relationship that will be
        // updated.
        val incomingRelationship = path(pathLength - 1)
        nodeState.minOutgoingDepth(incomingRelationship)
      }
    }

    private def initiate(memoryTracker: MemoryTracker): Unit = {
      nodeState = expandMap.get(node.id())
      if (nodeState == NodeState.UNINITIALIZED) {
        nodeState = new NodeState(memoryTracker)
        memoryTracker.allocateHeap(NodeState.INSTANCE_SIZE)
        expandMap.put(node.id(), nodeState)
      }
    }
  }

  object NodeState {
    final val INSTANCE_SIZE = shallowSizeOfInstance(classOf[NodeState])

    val UNINITIALIZED: NodeState = null

    val NOOP_REL: Int = 0

    val NOOP: NodeState = {
      val noop = new NodeState(EmptyMemoryTracker.INSTANCE)
      noop.relAndNext = Array(null)
      noop.depths = Array[Byte](0)
      noop
    }
  }

  /**
   * The state of expansion for one node
   */
  class NodeState(memoryTracker: MemoryTracker) {

    // All relationships that connect to this node and the next node, filtered by the var-length predicates
    var relAndNext: Array[(VirtualRelationshipValue, VirtualNodeValue)] = _

    // The fully expanded depth for each relationship in rels
    var depths: Array[Byte] = _

    // True if this node has been emitted before
    var isEmitted = false

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
        val (rels, _) = relAndNext(i)
        if (rels.id() != incomingRelId) {
          min = math.min(depths(i), min)
        }
        i += 1
      }
      min
    }

    def updateFullExpandDepth(relIndex: Int, depth: Int): Unit = {
      depths(relIndex) = depth.toByte
    }

    /**
     * If not already done, list all relationships of a node, given the predicates of this pipe.
     */
    def ensureExpanded(queryState: QueryState, row: CypherRow, node: VirtualNodeValue): Unit = {
      if (relAndNext == null) {
        val allRels = queryState.query.getRelationshipsForIds(node.id(), dir, types.types(queryState.query))
        val builder = Array.newBuilder[(VirtualRelationshipValue, VirtualNodeValue)]
        // Immediately exhausting allRels. No ClosingIterator needed for connecting them to the outside.
        while (allRels.hasNext) {
          val rel =
            VirtualValues.relationship(allRels.next(), allRels.startNodeId(), allRels.endNodeId(), allRels.typeId())
          val otherNode = VirtualValues.node(allRels.otherNodeId(node.id()))
          if (
            filteringStep.filterRelationship(row, queryState)(rel) &&
            filteringStep.filterNode(row, queryState)(otherNode)
          ) {
            builder += ((rel, otherNode))
            memoryTracker.allocateHeap(rel.estimatedHeapUsage)
          }
        }
        relAndNext = builder.result()
        depths = new Array[Byte](relAndNext.length)
        memoryTracker.allocateHeap(
          HeapEstimator.shallowSizeOfObjectArray(relAndNext.length) + HeapEstimator.sizeOf(depths)
        )
      }
    }
  }

  /**
   * The overall state of the full pruning var expand. Mostly manages stack of PruningDFS nodes.
   */
  class FullPruneState(queryState: QueryState, val memoryTracker: MemoryTracker) {
    private var inputRow: CypherRow = _
    private val nodeState = new Array[PruningDFS](self.max + 1)
    private val path = new Array[Long](max)
    memoryTracker.allocateHeap(HeapEstimator.shallowSizeOfObjectArray(nodeState.length) + HeapEstimator.sizeOf(path))
    private var depth = -1

    def startRow(inputRow: CypherRow): Unit = {
      memoryTracker.reset() // We build up a new state for each input row
      this.inputRow = inputRow
      depth = -1
    }
    def canContinue: Boolean = inputRow != null

    def next(): CypherRow = {
      val endNode =
        if (depth == -1) {
          val fromValue = inputRow.getByName(fromName)
          fromValue match {
            case node: VirtualNodeValue =>
              if (filteringStep.filterNode(inputRow, queryState)(node)) pushStartNode(node)
              else null

            case IsNoValue() => null

            case _ =>
              error(s"Expected variable `$fromName` to be a node, got $fromValue")
          }
        } else {
          var maybeEndNode: VirtualNodeValue = null
          while (depth >= 0 && maybeEndNode == null) {
            maybeEndNode = nodeState(depth).nextEndNode()
            if (maybeEndNode == null) pop()
          }
          maybeEndNode
        }
      if (endNode == null) {
        inputRow = null
        null
      } else rowFactory.copyWith(inputRow, self.toName, endNode)
    }

    def pushStartNode(node: VirtualNodeValue): VirtualNodeValue = {
      if (filteringStep.filterNode(inputRow, queryState)(node)) {
        push(
          node,
          pathLength = 0,
          expandMap = HeapTrackingCollections.newLongObjectMap[NodeState](memoryTracker),
          prevLocalRelIndex = -1,
          prevNodeState = NodeState.NOOP
        )
      } else {
        null
      }
    }

    def push(
      node: VirtualNodeValue,
      pathLength: Int,
      expandMap: MutableLongObjectMap[NodeState],
      prevLocalRelIndex: Int,
      prevNodeState: NodeState
    ): VirtualNodeValue = {
      queryState.query.transactionalContext.assertTransactionOpen() // Check transaction termination/timeout in case this is long running
      depth += 1
      nodeState(depth) =
        new PruningDFS(this, node, path, pathLength, queryState, inputRow, expandMap, prevLocalRelIndex, prevNodeState)

      nodeState(depth).nextEndNode()
    }

    def pop(): Unit = {
      nodeState(depth) = null // not needed, but nice for debugging
      depth -= 1
    }

    private def error(msg: String) = throw new InternalException(msg)
  }

  class FullyPruningIterator(
    private val input: ClosingIterator[CypherRow],
    private val queryState: QueryState
  ) extends ClosingIterator[CypherRow] {

    private val memoryTracker =
      queryState.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x).getScopedMemoryTracker
    private var outputRow: CypherRow = _
    private var fullPruneState: FullPruneState = new FullPruneState(queryState, memoryTracker)
    private var hasPrefetched = false

    override protected[this] def closeMore(): Unit = {
      if (fullPruneState != null) {
        fullPruneState = null
        memoryTracker.close()
        input.close()
      }
    }

    override def innerHasNext: Boolean = {
      prefetch()
      outputRow != null
    }

    override def next(): CypherRow = {
      prefetch()
      if (outputRow == null) {
        // fail
        Iterator.empty.next()
      }
      consumePrefetched()
    }

    private def consumePrefetched(): CypherRow = {
      val temp = outputRow
      hasPrefetched = false
      outputRow = null
      temp
    }

    private def prefetch(): Unit =
      if (!hasPrefetched) {
        outputRow = fetch()
        hasPrefetched = true
      }

    private def fetch(): CypherRow = {
      while (fullPruneState.canContinue || input.nonEmpty) {
        if (!fullPruneState.canContinue) {
          fullPruneState.startRow(input.next())
        } else {
          val row = fullPruneState.next()
          if (row != null) return row
        }
      }
      null
    }
  }

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    new FullyPruningIterator(input, state)
  }
}
