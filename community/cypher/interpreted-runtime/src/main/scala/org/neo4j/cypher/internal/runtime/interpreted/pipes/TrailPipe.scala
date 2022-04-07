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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingCollections.newArrayDeque
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.logical.plans.Limited
import org.neo4j.cypher.internal.logical.plans.Repetitions
import org.neo4j.cypher.internal.logical.plans.Unlimited
import org.neo4j.cypher.internal.runtime.CastSupport.castOrFail
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TrailPipe.EMPTY_SET
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST

import scala.annotation.tailrec

case class TrailState(node: Long,
                      groupNodes: Array[ListValue],
                      groupRelationships: Array[ListValue],
                      relationshipsSeen: HeapTrackingLongHashSet,
                      iterations: Int) extends AutoCloseable {
  def close(): Unit = {
    relationshipsSeen.close()
  }
}

case class TrailPipe(source: Pipe,
                     inner: Pipe,
                     repetitions: Repetitions,
                     start: String,
                     end: Option[String],
                     innerStart: String,
                     innerEnd: String,
                     groupNodes: Set[String],
                     groupRelationships: Set[String],
                     allRelationships: Set[String]
                    )(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  private val groupNodeNames = groupNodes.toArray.sorted
  private val groupRelationshipNames = groupRelationships.toArray.sorted
  private val emptyGroupNodes = Array.fill(groupNodeNames.length)(EMPTY_LIST)
  private val emptyGroupRelationships = Array.fill(groupRelationshipNames.length)(EMPTY_LIST)

  override protected def internalCreateResults(input: ClosingIterator[CypherRow],
                                               state: QueryState): ClosingIterator[CypherRow] = {

    val tracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    input.flatMap {
      outerRow => {
        outerRow.getByName(start) match {
          case startNode: VirtualNodeValue =>
            val stack = newArrayDeque[TrailState](tracker)
            if (checkUpperLimit(1)) {
              stack.push(TrailState(startNode.id(), emptyGroupNodes, emptyGroupRelationships, EMPTY_SET, 1))
            }
            new PrefetchingIterator[CypherRow] {
              private var innerResult: ClosingIterator[CypherRow] = ClosingIterator.empty
              private var trailState: TrailState = _
              private var emitFirst = repetitions.min == 0

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
                  Some(outerRow.copyWith(computeNewEntries(emptyGroupNodes, emptyGroupRelationships, startNode)))
                }
                else if (innerResult.hasNext) {
                  val row = innerResult.next()
                  val innerEndNode = castOrFail[VirtualNodeValue](row.getByName(innerEnd))
                  val newGroupNodes = computeGroupVariables(groupNodeNames, trailState.groupNodes, row)
                  val newGroupRels = computeGroupVariables(groupRelationshipNames, trailState.groupRelationships, row)
                  if (checkUpperLimit(trailState.iterations)) {
                    val newSet = HeapTrackingCollections.newLongSet(tracker)
                    newSet.addAll(trailState.relationshipsSeen)

                    val allRelationshipsUnique = allRelationships.forall(r => newSet.add(castOrFail[VirtualRelationshipValue](row.getByName(r)).id()))
                    if (allRelationshipsUnique) {
                      stack.push(TrailState(innerEndNode.id(), newGroupNodes, newGroupRels, newSet, trailState.iterations + 1))
                    }
                  }
                  //if iterated long enough emit, otherwise recurse
                  if (trailState.iterations >= repetitions.min) {
                    Some(row.copyWith(computeNewEntries(newGroupNodes, newGroupRels, innerEndNode)))
                  } else {
                    produceNext()
                  }
                } else if (!stack.isEmpty) {
                  //close previous state
                  if (trailState != null) {
                    trailState.close()
                  }
                  //Run RHS with previous end-node as new innerStartNode
                  trailState = stack.pop()
                  val innerState = state.withInitialContext(outerRow.copyWith(innerStart, VirtualValues.node(trailState.node)))
                  innerResult = inner.createResults(innerState).filter(row =>
                    allRelationships.forall(r => !trailState.relationshipsSeen.contains(castOrFail[VirtualRelationshipValue](row.getByName(r)).id())
                  ))
                  produceNext()
                } else {
                  None
                }
              }
            }

          case IsNoValue() => ClosingIterator.empty
          case value => throw new InternalException(s"Expected to find a node at '$start' but found $value instead")
        }
      }
    }
  }

  private def computeGroupVariables(groupNames: Array[String],
                                    groupVariables: Array[ListValue],
                                    row: CypherRow): Array[ListValue] = {
    val res = new Array[ListValue](groupNames.length)
    var i = 0
    while (i < groupNames.length) {
      res(i) = groupVariables(i).append(row.getByName(groupNames(i)))
      i += 1
    }
    res
  }

  private def computeNewEntries(newGroupNodes: Array[ListValue],
                                newGroupRels: Array[ListValue],
                                innerEndNode: VirtualNodeValue): collection.Seq[(String, AnyValue)] = {
    val newSize = newGroupNodes.length + newGroupRels.length + end.size
    val res = new Array[(String, AnyValue)](newSize)
    var i = 0
    while (i < newGroupNodes.length) {
      res(i) = (groupNodeNames(i), newGroupNodes(i))
      i += 1
    }
    var j = 0
    while (j < newGroupRels.length) {
      res(i) = (groupRelationshipNames(j), newGroupRels(j))
      j += 1
      i += 1
    }
    end.foreach(e => res(i) = (e, innerEndNode))
    res
  }

  private def checkUpperLimit(count: Int): Boolean = repetitions.max match {
    case Unlimited => true
    case Limited(n) => count < n
  }
}

object TrailPipe {
  def EMPTY_SET: HeapTrackingLongHashSet = HeapTrackingCollections.newLongSet(EmptyMemoryTracker.INSTANCE, 0)
}

