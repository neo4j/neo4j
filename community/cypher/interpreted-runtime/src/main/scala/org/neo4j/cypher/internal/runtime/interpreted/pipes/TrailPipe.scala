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

import org.neo4j.collection.trackable.HeapTrackingArrayList
import org.neo4j.collection.trackable.HeapTrackingCollections
import org.neo4j.collection.trackable.HeapTrackingCollections.newArrayDeque
import org.neo4j.collection.trackable.HeapTrackingLongHashSet
import org.neo4j.cypher.internal.logical.plans.GroupEntity
import org.neo4j.cypher.internal.logical.plans.Repetitions
import org.neo4j.cypher.internal.runtime.CastSupport.castOrFail
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.PrefetchingIterator
import org.neo4j.cypher.internal.runtime.interpreted.pipes.TrailPipe.emptyLists
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.memory.MemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_LIST

import scala.annotation.tailrec

case class TrailState(
  node: Long,
  groupNodes: HeapTrackingArrayList[ListValue],
  groupRelationships: HeapTrackingArrayList[ListValue],
  relationshipsSeen: HeapTrackingLongHashSet,
  iterations: Int
) extends AutoCloseable {

  def close(): Unit = {
    relationshipsSeen.close()
  }
}

case class TrailPipe(
  source: Pipe,
  inner: Pipe,
  repetitions: Repetitions,
  start: String,
  end: Option[String],
  innerStart: String,
  innerEnd: String,
  groupNodes: Set[GroupEntity],
  groupRelationships: Set[GroupEntity],
  allRelationships: Set[String],
  allRelationshipGroups: Set[String]
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) {

  private val groupNodeNames = groupNodes.toArray.sortBy(_.innerName)
  private val groupRelationshipNames = groupRelationships.toArray.sortBy(_.innerName)
  private val emptyGroupNodes = emptyLists(groupNodes.size)
  private val emptyGroupRelationships = emptyLists(groupRelationships.size)
  private val allRelationshipsArray = allRelationships.toArray

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {

    val tracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)
    input.flatMap {
      outerRow =>
        {
          outerRow.getByName(start) match {
            case startNode: VirtualNodeValue =>
              val stack = newArrayDeque[TrailState](tracker)
              if (repetitions.max.isGreaterThan(0)) {
                val relationshipsSeen = HeapTrackingCollections.newLongSet(tracker)
                val ig = allRelationshipGroups.iterator
                while (ig.hasNext) {
                  val i = castOrFail[ListValue](outerRow.getByName(ig.next())).iterator()
                  while (i.hasNext) {
                    relationshipsSeen.add(castOrFail[VirtualRelationshipValue](i.next()).id())
                  }
                }
                stack.push(TrailState(startNode.id(), emptyGroupNodes, emptyGroupRelationships, relationshipsSeen, 1))
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
                    val resultRow =
                      outerRow.copyWith(computeNewEntries(emptyGroupNodes, emptyGroupRelationships, startNode))
                    Some(resultRow)
                  } else if (innerResult.hasNext) {
                    val row = innerResult.next()
                    val innerEndNode = castOrFail[VirtualNodeValue](row.getByName(innerEnd))
                    val newGroupNodes = computeGroupVariables(groupNodeNames, trailState.groupNodes, row, tracker)
                    val newGroupRels =
                      computeGroupVariables(groupRelationshipNames, trailState.groupRelationships, row, tracker)
                    if (repetitions.max.isGreaterThan(trailState.iterations)) {
                      val newSet = HeapTrackingCollections.newLongSet(tracker, trailState.relationshipsSeen)

                      var allRelationshipsUnique = true
                      var i = 0
                      while (allRelationshipsUnique && i < allRelationshipsArray.length) {
                        val r = allRelationshipsArray(i)
                        // TODO optimization: allRelationships is a superset of RHS rels, we should only check RHS rels as only they can change
                        if (!newSet.add(castOrFail[VirtualRelationshipValue](row.getByName(r)).id())) {
                          allRelationshipsUnique = false
                        }
                        i += 1
                      }

                      if (allRelationshipsUnique) {
                        stack.push(TrailState(
                          innerEndNode.id(),
                          newGroupNodes,
                          newGroupRels,
                          newSet,
                          trailState.iterations + 1
                        ))
                      }
                    }
                    // if iterated long enough emit, otherwise recurse
                    if (trailState.iterations >= repetitions.min) {
                      val resultRow = row.copyWith(computeNewEntries(newGroupNodes, newGroupRels, innerEndNode))
                      Some(resultRow)
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
                    outerRow.set(innerStart, VirtualValues.node(trailState.node))
                    val innerState = state.withInitialContext(outerRow)
                    innerResult = inner.createResults(innerState).filter(row => {
                      var relationshipsAreUnique = true
                      var i = 0
                      while (relationshipsAreUnique && i < allRelationshipsArray.length) {
                        val r = allRelationshipsArray(i)
                        // TODO optimization: allRelationships is a superset of RHS rels, we should only check RHS rels as only they can change
                        if (
                          trailState.relationshipsSeen.contains(
                            castOrFail[VirtualRelationshipValue](row.getByName(r)).id()
                          )
                        ) {
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

            case IsNoValue() => ClosingIterator.empty
            case value => throw new InternalException(s"Expected to find a node at '$start' but found $value instead")
          }
        }
    }
  }

  private def computeGroupVariables(
    groupNames: Array[GroupEntity],
    groupVariables: HeapTrackingArrayList[ListValue],
    row: CypherRow,
    tracker: MemoryTracker
  ): HeapTrackingArrayList[ListValue] = {
    val res = HeapTrackingCollections.newArrayList[ListValue](groupNames.length, tracker)
    var i = 0
    while (i < groupNames.length) {
      res.add(groupVariables.get(i).append(row.getByName(groupNames(i).innerName)))
      i += 1
    }
    res
  }

  private def computeNewEntries(
    newGroupNodes: HeapTrackingArrayList[ListValue],
    newGroupRels: HeapTrackingArrayList[ListValue],
    innerEndNode: VirtualNodeValue
  ): collection.Seq[(String, AnyValue)] = {
    val newSize = newGroupNodes.size() + newGroupRels.size() + end.size
    val res = new Array[(String, AnyValue)](newSize)
    var i = 0
    while (i < newGroupNodes.size()) {
      res(i) = (groupNodeNames(i).outerName, newGroupNodes.get(i))
      i += 1
    }
    var j = 0
    while (j < newGroupRels.size()) {
      res(i) = (groupRelationshipNames(j).outerName, newGroupRels.get(j))
      j += 1
      i += 1
    }
    end.foreach(e => res(i) = (e, innerEndNode))
    res
  }
}

object TrailPipe {

  def emptyLists(size: Int): HeapTrackingArrayList[ListValue] = {
    val emptyList = HeapTrackingCollections.newArrayList[ListValue](size, EmptyMemoryTracker.INSTANCE)
    (0 until size).foreach(_ => emptyList.add(EMPTY_LIST))
    emptyList
  }
}
