/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes

import org.neo4j.collection.primitive.{Primitive, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.Id
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection}
import org.neo4j.graphdb.{Node, Relationship}

case class FullPruningVarLengthExpandPipe(source: Pipe,
                                          fromName: String,
                                          toName: String,
                                          types: LazyTypes,
                                          dir: SemanticDirection,
                                          min: Int,
                                          max: Int,
                                          filteringStep: VarLengthPredicate = VarLengthPredicate.NONE)
                                         (val id: Id = new Id)
                                         (implicit pipeMonitor: PipeMonitor) extends PipeWithSource(source, pipeMonitor) with Pipe {
  self =>

  assert(min <= max)

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
  class PruningDFS(state: FullPruneState,
                   node: Node,
                   val path: Array[Long],
                   val pathLength: Int,
                   val queryState: QueryState,
                   row: ExecutionContext,
                   expandMap: PrimitiveLongObjectMap[NodeState],
                   prevLocalRelIndex: Int,
                   prevNodeState: NodeState
  ) extends CheckPath {

    import NodeState.UNINITIALIZED

    var relationshipCursor = 0
    var nodeState: NodeState = UNINITIALIZED

    def nextEndNode(): Node = {

      initiate()

      if (pathLength < self.max) {

        nodeState.ensureExpanded(queryState, row, node)

        while (hasRelationship) {
          val currentRelIdx = nextRelationship()
          if (!haveFullyExploredTheRemainingStepsBefore(currentRelIdx)) {
            val rel = nodeState.rels(currentRelIdx)
            val relId = rel.getId
            if (!seenRelationshipInPath(relId)) {
              val nextNode = rel.getOtherNode(node)
              path(pathLength) = relId
              val endNode = state.push( new PruningDFS(
                                      state = state,
                                      node = nextNode,
                                      path = path,
                                      pathLength = pathLength + 1,
                                      queryState = queryState,
                                      row = row,
                                      expandMap = expandMap,
                                      prevLocalRelIndex = currentRelIdx,
                                      prevNodeState = nodeState
                ) )
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
        expandMap.put(node.getId, nodeState)
        node
      } else {
        expandMap.put(node.getId, nodeState)
        null
      }
    }

    private def hasRelationship = relationshipCursor < nodeState.rels.length

    private def nextRelationship() = {
      val next = relationshipCursor
      relationshipCursor += 1
      next
    }

    private def haveFullyExploredTheRemainingStepsBefore(i: Int) = {
      val stepsLeft = self.max - pathLength
      nodeState.depths(i) >= stepsLeft
    }

    def updatePrevFullExpandDepth() = {
      if ( pathLength > 0 ) {
        val requiredStepsFromPrev = math.max(0, self.min - pathLength + 1)
        if (requiredStepsFromPrev <= 1 || nodeState.isEmitted) {
          prevNodeState.updateFullExpandDepth(prevLocalRelIndex, currentOutgoingFullExpandDepth() + 1)
        }
      }
    }

    private def currentOutgoingFullExpandDepth(): Int = {
      assert(pathLength > 0)
      if (pathLength == self.max) 0
      else {
        // The maximum amount of steps that have been fully explored is the minimum full expand depth of the
        // outgoing relationships. The incoming relationship is not included, as that is the relationship that will be
        // updated.
        val incomingRelationship = path(pathLength - 1)
        nodeState.minOutgoingDepth(incomingRelationship)
      }
    }

    private def initiate() = {
      nodeState = expandMap.get(node.getId)
      if (nodeState == UNINITIALIZED) {
        nodeState = new NodeState()
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

  object NodeState {
    val UNINITIALIZED: NodeState = null

    val NOOP = new NodeState() {
      override def minOutgoingDepth(incomingRelId: Long): Int = 0
      override def updateFullExpandDepth(relIndex:Int, depth:Int ) = {}
    }
  }

  /**
    * The state of expansion for one node
    */
  class NodeState() {
    // All relationships that connect to this node, filtered by the var-length predicates
    var rels: Array[Relationship] = _
    // The fully expanded depth for each relationship in rels
    var depths:Array[Byte] = _
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
      while (i < rels.length) {
        if (rels(i).getId != incomingRelId) {
          min = math.min(depths(i), min)
        }
        i += 1
      }
      min
    }

    def updateFullExpandDepth(relIndex:Int, depth:Int ): Unit = {
      depths(relIndex) = depth.toByte
    }

    /**
      * If not already done, list all relationships of a node, given the predicates of this pipe.
      */
    def ensureExpanded(queryState: QueryState, row: ExecutionContext, node: Node) = {
      if ( rels == null ) {
        val allRels = queryState.query.getRelationshipsForIds(node, dir, types.types(queryState.query))
        rels = allRels.filter(r => {
          filteringStep.filterRelationship(row, queryState)(r) &&
            filteringStep.filterNode(row, queryState)(r.getOtherNode(node))
        }).toArray
        depths = new Array[Byte](rels.length)
      }
    }
  }

  /**
    * The overall state of the full pruning var expand. Mostly manages stack of PruningDFS nodes.
    */
  class FullPruneState(queryState:QueryState ) {
    var inputRow:ExecutionContext = _
    val nodeState = new Array[PruningDFS](self.max + 1)
    val path = new Array[Long](max)
    var depth = -1

    def startRow( inputRow:ExecutionContext ) = {
      this.inputRow = inputRow
      depth = -1
    }
    def canContinue: Boolean = inputRow != null
    def next(): ExecutionContext = {
      val endNode =
        if (depth == -1) {
          push( new PruningDFS(
                                state = this,
                                node = getNodeFromRow(inputRow),
                                path = path,
                                pathLength = 0,
                                queryState = queryState,
                                row = inputRow,
                                expandMap = Primitive.longObjectMap[NodeState](),
                                prevLocalRelIndex = -1,
                                prevNodeState = NodeState.NOOP) )
        } else {
          var maybeEndNode: Node = null
          while ( depth >= 0 && maybeEndNode == null ) {
            maybeEndNode = nodeState(depth).nextEndNode()
            if (maybeEndNode == null) pop()
          }
          maybeEndNode
        }
      if (endNode == null) {
        inputRow = null
        null
      }
      else inputRow.newWith(self.toName -> endNode)
    }

    def push( pruningDFS: PruningDFS ): Node = {
      depth += 1
      nodeState(depth) = pruningDFS
      pruningDFS.nextEndNode()
    }

    def pop(): Unit = {
      nodeState(depth) = null // not needed, but nice for debugging
      depth -= 1
    }

    private def getNodeFromRow(row: ExecutionContext): Node =
      row.getOrElse(fromName, throw new InternalException(s"Expected a node on `$fromName`")).asInstanceOf[Node]
  }

  class FullyPruningIterator(
                       private val input: Iterator[ExecutionContext],
                       val queryState: QueryState
  ) extends Iterator[ExecutionContext] {

    var outputRow:ExecutionContext = _
    var fullPruneState:FullPruneState = new FullPruneState( queryState )
    var hasPrefetched = false

    override def hasNext = {
      prefetch()
      outputRow != null
    }

    override def next() = {
      prefetch()
      if (outputRow == null) {
        // fail
        Iterator.empty.next()
      }
      consumePrefetched()
    }

    def consumePrefetched() = {
      val temp = outputRow
      hasPrefetched = false
      outputRow = null
      temp
    }

    def prefetch(): Unit =
      if (!hasPrefetched) {
        outputRow = fetch()
        hasPrefetched = true
      }

    def fetch(): ExecutionContext = {
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


    private def getNodeFromRow(row: ExecutionContext): Node =
      row.getOrElse(fromName, throw new InternalException(s"Expected a node on `$fromName`")).asInstanceOf[Node]
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] = {
    new FullyPruningIterator(input, state)
  }
}
