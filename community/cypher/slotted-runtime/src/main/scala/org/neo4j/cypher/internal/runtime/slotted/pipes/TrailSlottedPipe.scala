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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingCollections.newArrayDeque
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveRelationshipFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetValueFromSlotFunctionFor
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeSetValueInSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CastSupport.castOrFail
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TrailPipe.emptyLists
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TrailState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.util.function.ToLongFunction

import scala.annotation.tailrec

case class TrailSlottedPipe(
  source: Pipe,
  inner: Pipe,
  repetition: Repetition,
  startSlot: Slot,
  endOffset: Option[Int],
  innerStarOffset: Int,
  innerEndSlot: Slot,
  groupNodes: Set[GroupSlot],
  groupRelationships: Set[GroupSlot],
  allRelationships: Set[Slot],
  allRelationshipGroups: Set[Slot],
  slots: SlotConfiguration,
  rhsSlots: SlotConfiguration,
  argumentSize: SlotConfiguration.Size
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  private val getStartNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(startSlot)
  private val getInnerEndNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(innerEndSlot)

  private val groupNodeGetters = groupNodes.map { case GroupSlot(s, _) =>
    makeGetPrimitiveRelationshipFromSlotFunctionFor(s)
  }.toArray

  private val groupRelGetters = groupRelationships.map { case GroupSlot(s, _) =>
    makeGetPrimitiveRelationshipFromSlotFunctionFor(s)
  }.toArray
  private val allRelGetters = allRelationships.map(s => makeGetPrimitiveRelationshipFromSlotFunctionFor(s)).toArray
  private val allRelGroupsGetters = allRelationshipGroups.map(s => makeGetValueFromSlotFunctionFor(s)).toArray
  private val groupNodeSetters = groupNodes.map { case GroupSlot(_, s) => makeSetValueInSlotFunctionFor(s) }.toArray

  private val groupRelSetters = groupRelationships.map { case GroupSlot(_, s) =>
    makeSetValueInSlotFunctionFor(s)
  }.toArray
  private val emptyGroupNodes = emptyLists(groupNodes.size)
  private val emptyGroupRelationships = emptyLists(groupRelationships.size)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val queryContext: QueryContext = state.query

    val tracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    input.flatMap {
      outerRow =>
        {
          def newResultRow(
            groupNodes: HeapTrackingArrayList[ListValue],
            groupRelationships: HeapTrackingArrayList[ListValue],
            innerEndNode: Long
          ): Some[SlottedRow] = {
            val resultRow = SlottedRow(slots)
            resultRow.copyFrom(outerRow, argumentSize.nLongs, argumentSize.nReferences)
            writeGroupVariables(groupNodes, groupRelationships, innerEndNode, resultRow)
            Some(resultRow)
          }

          val startNode = getStartNodeFunction.applyAsLong(outerRow)
          if (NullChecker.entityIsNull(startNode)) {
            ClosingIterator.empty
          } else {
            // Every invocation of RHS for this incoming LHS row can use the same argument row (avoids unnecessary row creation)
            val rhsInitialRow = SlottedRow(rhsSlots)
            rhsInitialRow.copyFrom(outerRow, argumentSize.nLongs, argumentSize.nReferences)

            val stack = newArrayDeque[TrailState](tracker)
            if (repetition.max.isGreaterThan(0)) {
              val relationshipsSeen = HeapTrackingCollections.newLongSet(tracker)
              val ig = allRelGroupsGetters.iterator
              while (ig.hasNext) {
                val i = castOrFail[ListValue](ig.next().apply(outerRow)).iterator()
                while (i.hasNext) {
                  relationshipsSeen.add(castOrFail[VirtualRelationshipValue](i.next()).id())
                }
              }
              stack.push(TrailState(startNode, emptyGroupNodes, emptyGroupRelationships, relationshipsSeen, 1))
            }
            new PrefetchingIterator[CypherRow] {
              private var innerResult: ClosingIterator[CypherRow] = ClosingIterator.empty
              private var trailState: TrailState = _
              private var emitFirst = repetition.min == 0

              override protected[this] def closeMore(): Unit = {
                if (trailState != null) {
                  trailState.close()
                }
                innerResult.close()
                stack.close()
              }

              @tailrec
              def produceNext(): Option[CypherRow] = {
                if (emitFirst) {
                  emitFirst = false
                  newResultRow(emptyGroupNodes, emptyGroupRelationships, startNode)
                } else if (innerResult.hasNext) {
                  val row = innerResult.next()
                  val innerEndNode = getInnerEndNodeFunction.applyAsLong(row)
                  val newGroupNodes =
                    computeNodeGroupVariables(groupNodeGetters, trailState.groupNodes, row, queryContext, tracker)
                  val newGroupRels =
                    computeRelGroupVariables(groupRelGetters, trailState.groupRelationships, row, queryContext, tracker)
                  if (repetition.max.isGreaterThan(trailState.iterations)) {
                    val newSet = HeapTrackingCollections.newLongSet(tracker, trailState.relationshipsSeen)

                    var allRelationshipsUnique = true
                    var i = 0
                    while (allRelationshipsUnique && i < allRelGetters.length) {
                      val r = allRelGetters(i)
                      // TODO optimization: allRelationships is a superset of RHS rels, we should only check RHS rels as only they can change
                      if (!newSet.add(r.applyAsLong(row))) {
                        allRelationshipsUnique = false
                      }
                      i += 1
                    }

                    if (allRelationshipsUnique) {
                      stack.push(TrailState(
                        innerEndNode,
                        newGroupNodes,
                        newGroupRels,
                        newSet,
                        trailState.iterations + 1
                      ))
                    }
                  }
                  // if iterated long enough emit, otherwise recurse
                  if (trailState.iterations >= repetition.min) {
                    newResultRow(newGroupNodes, newGroupRels, innerEndNode)
                  } else {
                    produceNext()
                  }
                } else if (!stack.isEmpty) {
                  // close previous state
                  if (trailState != null) {
                    trailState.close()
                  }
                  // Run RHS with previous end-node as new innerStartNode
                  trailState = stack.pop()
                  rhsInitialRow.setLongAt(innerStarOffset, trailState.node)
                  val innerState = state.withInitialContext(rhsInitialRow)
                  innerResult = inner.createResults(innerState).filter(row => {
                    var relationshipsAreUnique = true
                    var i = 0
                    while (relationshipsAreUnique && i < allRelGetters.length) {
                      val r = allRelGetters(i)
                      // TODO optimization: allRelationships is a superset of RHS rels, we should only check RHS rels as only they can change
                      if (trailState.relationshipsSeen.contains(r.applyAsLong(row))) {
                        relationshipsAreUnique = false
                      }
                      i += 1
                    }
                    relationshipsAreUnique
                  })
                  produceNext()
                } else {
                  None
                }
              }
            }
          }
        }
    }
  }

  private def computeNodeGroupVariables(
    innerEntityGetters: Array[ToLongFunction[ReadableRow]],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: CypherRow,
    queryContext: QueryContext,
    tracker: MemoryTracker
  ): HeapTrackingArrayList[ListValue] = {
    val res = HeapTrackingCollections.newArrayList[ListValue](groupVariables.size(), tracker)
    var i = 0
    while (i < innerEntityGetters.length) {
      res.add(groupVariables.get(i).append(queryContext.nodeById(innerEntityGetters(i).applyAsLong(row))))
      i += 1
    }
    res
  }

  private def computeRelGroupVariables(
    innerEntityGetters: Array[ToLongFunction[ReadableRow]],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: CypherRow,
    queryContext: QueryContext,
    tracker: MemoryTracker
  ): HeapTrackingArrayList[ListValue] = {
    val res = HeapTrackingCollections.newArrayList[ListValue](groupVariables.size(), tracker)
    var i = 0
    while (i < innerEntityGetters.length) {
      res.add(groupVariables.get(i).append(queryContext.relationshipById(innerEntityGetters(i).applyAsLong(row))))
      i += 1
    }
    res
  }

  private def writeGroupVariables(
    newGroupNodes: HeapTrackingArrayList[ListValue],
    newGroupRels: HeapTrackingArrayList[ListValue],
    innerEndNode: Long,
    row: CypherRow
  ): Unit = {
    var i = 0
    while (i < groupNodeSetters.length) {
      groupNodeSetters(i)(row, newGroupNodes.get(i))
      i += 1
    }
    i = 0
    while (i < groupRelSetters.length) {
      groupRelSetters(i)(row, newGroupRels.get(i))
      i += 1
    }
    endOffset.foreach(o => row.setLongAt(o, innerEndNode))
  }
}

case class GroupSlot(innerSlot: Slot, outerSlot: Slot)
