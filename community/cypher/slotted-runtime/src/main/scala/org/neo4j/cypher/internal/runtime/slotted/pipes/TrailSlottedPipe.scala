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
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
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
import org.neo4j.cypher.internal.runtime.ReadQueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.WritableRow
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TrailPipe.emptyLists
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.runtime.slotted.expressions.TrailState
import org.neo4j.cypher.internal.runtime.slotted.helpers.NullChecker
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.memory.HeapEstimator
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.AnyValueWriter
import org.neo4j.values.Equality
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.ValueRepresentation
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualRelationshipValue

import java.util.function.ToLongFunction

import scala.annotation.tailrec
import scala.runtime.ScalaRunTime

case class SlottedTrailState(
  node: Long,
  groupNodes: HeapTrackingArrayList[ListValue],
  groupRelationships: HeapTrackingArrayList[ListValue],
  override val relationshipsSeen: HeapTrackingLongHashSet,
  iterations: Int,
  closeGroupsOnClose: Boolean
) extends AnyValue with TrailState {

  override protected def equalTo(other: Any): Boolean = ScalaRunTime.equals(other)

  override protected def computeHash(): Int = ScalaRunTime._hashCode(SlottedTrailState.this)

  override def writeTo[E <: Exception](writer: AnyValueWriter[E]): Unit = throw new UnsupportedOperationException()

  override def ternaryEquals(other: AnyValue): Equality = throw new UnsupportedOperationException()

  override def map[T](mapper: ValueMapper[T]): T = throw new UnsupportedOperationException()

  override def getTypeName: String = "PipelinedTrailState"

  override def estimatedHeapUsage(): Long = SlottedTrailState.SHALLOW_SIZE

  override def valueRepresentation(): ValueRepresentation = ValueRepresentation.UNKNOWN

  def close(): Unit = {
    if (closeGroupsOnClose) {
      groupNodes.close()
      groupRelationships.close()
    }
    relationshipsSeen.close()
  }
}

object SlottedTrailState {
  final val SHALLOW_SIZE: Long = HeapEstimator.shallowSizeOfInstance(classOf[SlottedTrailState])
}

case class TrailSlottedPipe(
  source: Pipe,
  inner: Pipe,
  repetition: Repetition,
  startSlot: Slot,
  endOffset: Int,
  innerStarOffset: Int,
  trailStateMetadataSlot: Int,
  innerEndSlot: Slot,
  groupNodes: Array[GroupSlot],
  groupRelationships: Array[GroupSlot],
  innerRelationships: Set[Slot],
  previouslyBoundRelationships: Set[Slot],
  previouslyBoundRelationshipGroups: Set[Slot],
  slots: SlotConfiguration,
  rhsSlots: SlotConfiguration,
  argumentSize: SlotConfiguration.Size,
  reverseGroupVariableProjections: Boolean
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  private val getStartNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(startSlot)
  private val getInnerEndNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(innerEndSlot)

  private val groupNodeGetters = groupNodes.map { case GroupSlot(s, _) =>
    makeGetPrimitiveRelationshipFromSlotFunctionFor(s)
  }

  private val groupRelGetters = groupRelationships.map { case GroupSlot(s, _) =>
    makeGetPrimitiveRelationshipFromSlotFunctionFor(s)
  }
  private val innerRelGetters = innerRelationships.map(s => makeGetPrimitiveRelationshipFromSlotFunctionFor(s)).toArray

  private val previouslyBoundRelGetters =
    previouslyBoundRelationships.map(s => makeGetPrimitiveRelationshipFromSlotFunctionFor(s)).toArray

  private val previouslyBoundRelGroupGetters =
    previouslyBoundRelationshipGroups.map(s => makeGetValueFromSlotFunctionFor(s)).toArray
  private val groupNodeSetters = groupNodes.map { case GroupSlot(_, s) => makeSetValueInSlotFunctionFor(s) }

  private val groupRelSetters = groupRelationships.map { case GroupSlot(_, s) =>
    makeSetValueInSlotFunctionFor(s)
  }
  private val emptyGroupNodes = emptyLists(groupNodes.length)
  private val emptyGroupRelationships = emptyLists(groupRelationships.length)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val queryContext: QueryContext = state.query

    val tracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    input.flatMap {
      outerRow =>
        {
          def newResultRowWithEmptyGroups(innerEndNode: Long): Some[SlottedRow] = {
            val resultRow = SlottedRow(slots)
            resultRow.copyFrom(outerRow, argumentSize.nLongs, argumentSize.nReferences)
            TrailSlottedPipe.writeResultColumnsWithProvidedGroups(
              emptyGroupNodes,
              emptyGroupRelationships,
              innerEndNode,
              resultRow,
              groupNodeSetters,
              groupRelSetters,
              endOffset
            )
            Some(resultRow)
          }

          def newResultRow(
            rhsInnerRow: CypherRow,
            prevRepetitionGroupNodes: HeapTrackingArrayList[ListValue],
            prevRepetitionGroupRelationships: HeapTrackingArrayList[ListValue],
            innerEndNode: Long
          ): Some[CypherRow] = {
            TrailSlottedPipe.writeResultColumns(
              prevRepetitionGroupNodes,
              prevRepetitionGroupRelationships,
              innerEndNode,
              rhsInnerRow,
              groupNodeGetters,
              groupRelGetters,
              groupNodeSetters,
              groupRelSetters,
              endOffset,
              queryContext,
              reverseGroupVariableProjections
            )
            Some(rhsInnerRow)
          }

          val startNode = getStartNodeFunction.applyAsLong(outerRow)
          if (NullChecker.entityIsNull(startNode)) {
            ClosingIterator.empty
          } else {
            // Every invocation of RHS for this incoming LHS row can use the same argument row (avoids unnecessary row creation)
            val rhsInitialRow = SlottedRow(rhsSlots)
            rhsInitialRow.copyFrom(outerRow, argumentSize.nLongs, argumentSize.nReferences)

            val stack = newArrayDeque[SlottedTrailState](tracker)
            if (repetition.max.isGreaterThan(0)) {
              val relationshipsSeen = HeapTrackingCollections.newLongSet(tracker)
              val ir = previouslyBoundRelGetters.iterator
              while (ir.hasNext) {
                relationshipsSeen.add(ir.next().applyAsLong(outerRow))
              }

              val ig = previouslyBoundRelGroupGetters.iterator
              while (ig.hasNext) {
                val i = castOrFail[ListValue](ig.next().apply(outerRow)).iterator()
                while (i.hasNext) {
                  relationshipsSeen.add(castOrFail[VirtualRelationshipValue](i.next()).id())
                }
              }
              stack.push(SlottedTrailState(
                startNode,
                emptyGroupNodes,
                emptyGroupRelationships,
                relationshipsSeen,
                iterations = 1,
                // empty groups are reused for every argument, so can not be closed until the whole query finishes
                closeGroupsOnClose = false
              ))
            }
            new PrefetchingIterator[CypherRow] {
              private var innerResult: ClosingIterator[CypherRow] = ClosingIterator.empty
              private var trailState: SlottedTrailState = _
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
                  newResultRowWithEmptyGroups(startNode)
                } else if (innerResult.hasNext) {
                  val row = innerResult.next()
                  val innerEndNode = getInnerEndNodeFunction.applyAsLong(row)
                  if (repetition.max.isGreaterThan(trailState.iterations)) {
                    val newSet = HeapTrackingCollections.newLongSet(tracker, trailState.relationshipsSeen)

                    var i = 0
                    while (i < innerRelGetters.length) {
                      val r = innerRelGetters(i)
                      if (!newSet.add(r.applyAsLong(row))) {
                        throw new IllegalStateException(
                          "Method should only be called when all relationships are known to be unique"
                        )
                      }
                      i += 1
                    }

                    stack.push(SlottedTrailState(
                      innerEndNode,
                      TrailSlottedPipe.computeNodeGroupVariables(
                        groupNodeGetters,
                        trailState.groupNodes,
                        row,
                        queryContext,
                        tracker
                      ),
                      TrailSlottedPipe.computeRelGroupVariables(
                        groupRelGetters,
                        trailState.groupRelationships,
                        row,
                        queryContext,
                        tracker
                      ),
                      newSet,
                      trailState.iterations + 1,
                      closeGroupsOnClose = true
                    ))
                  }
                  // if iterated long enough emit, otherwise recurse
                  if (trailState.iterations >= repetition.min) {
                    newResultRow(
                      row,
                      trailState.groupNodes,
                      trailState.groupRelationships,
                      innerEndNode
                    )
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
                  rhsInitialRow.setRefAt(trailStateMetadataSlot, trailState)
                  val innerState = state.withInitialContext(rhsInitialRow)
                  innerResult = inner.createResults(innerState).filter(row => {
                    var relationshipsAreUnique = true
                    var i = 0
                    val innerRelationshipsSeen = collection.mutable.Set[Long]()
                    while (relationshipsAreUnique && i < innerRelGetters.length) {
                      val r = innerRelGetters(i)
                      val rel = r.applyAsLong(row)
                      if (trailState.relationshipsSeen.contains(rel)) {
                        relationshipsAreUnique = false
                      }
                      if (relationshipsAreUnique && !innerRelationshipsSeen.add(rel)) {
                        relationshipsAreUnique = false
                      }
                      i += 1
                    }
                    relationshipsAreUnique
                  })
                  produceNext()
                } else {
                  if (trailState != null) {
                    trailState.close()
                    trailState = null
                  }
                  None
                }
              }
            }
          }
        }
    }
  }
}

object TrailSlottedPipe {

  def computeNodeGroupVariables(
    innerEntityGetters: Array[ToLongFunction[ReadableRow]],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: ReadableRow,
    queryContext: ReadQueryContext,
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

  def computeRelGroupVariables(
    innerEntityGetters: Array[ToLongFunction[ReadableRow]],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: ReadableRow,
    queryContext: ReadQueryContext,
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

  def writeResultColumns(
    groupNodes: HeapTrackingArrayList[ListValue],
    groupRels: HeapTrackingArrayList[ListValue],
    innerEndNode: Long,
    row: CypherRow,
    groupNodeGetters: Array[ToLongFunction[ReadableRow]],
    groupRelGetters: Array[ToLongFunction[ReadableRow]],
    groupNodeSetters: Array[(WritableRow, AnyValue) => Unit],
    groupRelSetters: Array[(WritableRow, AnyValue) => Unit],
    endOffset: Int,
    queryContext: ReadQueryContext,
    reverseGroupVariableProjections: Boolean
  ): Unit = {
    var i = 0
    while (i < groupNodeGetters.length) {
      val nodeGroup = groupNodes.get(i).append(queryContext.nodeById(groupNodeGetters(i).applyAsLong(row)))
      val projectedNodeGroup = if (reverseGroupVariableProjections) nodeGroup.reverse() else nodeGroup
      groupNodeSetters(i)(row, projectedNodeGroup)
      i += 1
    }

    i = 0
    while (i < groupRelGetters.length) {
      val relGroup = groupRels.get(i).append(queryContext.relationshipById(groupRelGetters(i).applyAsLong(row)))
      val projectedRelGroup = if (reverseGroupVariableProjections) relGroup.reverse() else relGroup
      groupRelSetters(i)(row, projectedRelGroup)
      i += 1
    }

    row.setLongAt(endOffset, innerEndNode)
  }

  def writeResultColumnsWithProvidedGroups(
    groupNodes: HeapTrackingArrayList[ListValue],
    groupRels: HeapTrackingArrayList[ListValue],
    innerEndNode: Long,
    resultRow: WritableRow,
    groupNodeSetters: Array[(WritableRow, AnyValue) => Unit],
    groupRelSetters: Array[(WritableRow, AnyValue) => Unit],
    endOffset: Int
  ): Unit = {
    var i = 0
    while (i < groupNodeSetters.length) {
      groupNodeSetters(i)(resultRow, groupNodes.get(i))
      i += 1
    }
    i = 0
    while (i < groupRelSetters.length) {
      groupRelSetters(i)(resultRow, groupRels.get(i))
      i += 1
    }
    resultRow.setLongAt(endOffset, innerEndNode)
  }
}

case class GroupSlot(innerSlot: Slot, outerSlot: Slot)
