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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes

import org.neo4j.collection.primitive.{Primitive, PrimitiveLongObjectMap}
import org.neo4j.cypher.internal.apa.v3_4.InternalException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlanId
import org.neo4j.helpers.ValueUtils
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.values.virtual.{EdgeValue, NodeValue}

case class PruningVarLengthExpandPipe(source: Pipe,
                                      fromName: String,
                                      toName: String,
                                      types: LazyTypes,
                                      dir: SemanticDirection,
                                      min: Int,
                                      max: Int,
                                      filteringStep: VarLengthPredicate = VarLengthPredicate.NONE)
                                     (val id: LogicalPlanId = LogicalPlanId.DEFAULT) extends PipeWithSource(source) with Pipe {
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
    def next(): (State, ExecutionContext)
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
  class PrePruningDFS(whenEmptied: State,
                      node: NodeValue,
                      val path: Array[Long],
                      val pathLength: Int,
                      val state: QueryState,
                      row: ExecutionContext,
                      expandMap: PrimitiveLongObjectMap[FullExpandDepths]
                     ) extends State with Expandable with CheckPath {

    private var rels: Iterator[EdgeValue] = _

    /*
    Loads the relationship iterator of the nodes before min length has been reached.
     */
    override def next(): (State, ExecutionContext) = {

      if (rels == null)
        rels = expand(row, node)

      while (rels.hasNext) {
        val r = rels.next()
        val relId = r.id

        if (!seenRelationshipInPath(relId)) {
          val nextState: State = traverseRelationship(r, relId)

          return nextState.next()
        }
      }

      if (pathLength >= self.min)
        (whenEmptied, row.newWith1(self.toName, node))
      else
        whenEmptied.next()
    }

    /**
      * Creates the appropriate state for following a relationship to the next node.
      */
    private def traverseRelationship(r: EdgeValue, relId: Long): State = {
      val nextNode = r.otherNode(node)
      path(pathLength) = relId
      val nextPathLength = pathLength + 1
      if (nextPathLength >= self.min)
        new PruningDFS(whenEmptied = this,
          node = nextNode,
          path = path,
          pathLength = nextPathLength,
          state = state,
          row = row,
          expandMap = expandMap,
          updateMinFullExpandDepth = _ => {})
      else
        new PrePruningDFS(whenEmptied = this,
          node = nextNode,
          path = path,
          pathLength = nextPathLength,
          state = state,
          row = row,
          expandMap = expandMap)
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
  class PruningDFS(whenEmptied: State,
                   node: NodeValue,
                   val path: Array[Long],
                   val pathLength: Int,
                   val state: QueryState,
                   row: ExecutionContext,
                   expandMap: PrimitiveLongObjectMap[FullExpandDepths],
                   updateMinFullExpandDepth: Int => Unit) extends State with Expandable with CheckPath {

    import FullExpandDepths.UNINITIALIZED

    var idx = 0
    var fullExpandDepths: FullExpandDepths = UNINITIALIZED

    override def next(): (State, ExecutionContext) = {

      if (pathLength < self.max) {
        if (fullExpandDepths == UNINITIALIZED)
          initiateRecursion()

        while (hasRelationships) {
          val currentRelIdx = nextRelationship()
          if (!haveFullyExploredTheRemainingDepthBefore(currentRelIdx)) {
            val rel = fullExpandDepths.rels(currentRelIdx)
            val relId = rel.id
            if (!seenRelationshipInPath(relId)) {
              val nextNode = rel.otherNode(node)
              path(pathLength) = relId
              val nextState = new PruningDFS(whenEmptied = this,
                node = nextNode,
                path = path,
                pathLength = pathLength + 1,
                state = state,
                row = row,
                expandMap = expandMap,
                updateMinFullExpandDepth = fullExpandDepths.depths(currentRelIdx) = _)
              return nextState.next()
            }
          }
        }

        expandMap.put(node.id(), fullExpandDepths)
      }

      updateMinFullExpandDepth(currentFullExpandDepth)

      (whenEmptied, row.newWith1(self.toName, node))
    }

    private def hasRelationships = idx < fullExpandDepths.rels.length

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

  class LoadNext(private val input: Iterator[ExecutionContext], val state: QueryState) extends State with Expandable {

    override def next(): (State, ExecutionContext) =
      if (input.isEmpty) {
        (Empty, null)
      } else {
        val row = input.next()
        val nextState = new PrePruningDFS(whenEmptied = this,
                                          node = getNodeFromRow(row),
                                          path = new Array[Long](max),
                                          pathLength = 0,
                                          state = state,
                                          row = row,
                                          expandMap = Primitive.longObjectMap[FullExpandDepths]())
        nextState.next()
      }

    private def getNodeFromRow(row: ExecutionContext): NodeValue =
      row.getOrElse(fromName, throw new InternalException(s"Expected a node on `$fromName`")).asInstanceOf[NodeValue]
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
    def expand(row: ExecutionContext, node: NodeValue) = {
      val relationships = state.query.getRelationshipsForIds(node.id(), dir, types.types(state.query)).map(ValueUtils.fromRelationshipProxy)
      relationships.filter(r => {
        filteringStep.filterRelationship(row, state)(r) &&
          filteringStep.filterNode(row, state)(r.otherNode(node))
      })
    }
  }

  object FullExpandDepths {
    def apply(rels: Array[EdgeValue]) = new FullExpandDepths(rels)

    val UNINITIALIZED: FullExpandDepths = null
  }

  class FullExpandDepths(// all relationships that connect to this node, filtered by the var-length predicates
                         val rels: Array[EdgeValue]) {
    // The fully expanded depth for each relationship in rels
    val depths = new Array[Int](rels.length)

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
        if (rels(i).id() != incomingRelId)
          min = math.min(depths(i), min)
        i += 1
      }
      min
    }
  }

  override protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    new Iterator[ExecutionContext] {

      var (stateMachine, current) = new LoadNext(input, state).next()

      override def hasNext = current != null

      override def next() = {
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
