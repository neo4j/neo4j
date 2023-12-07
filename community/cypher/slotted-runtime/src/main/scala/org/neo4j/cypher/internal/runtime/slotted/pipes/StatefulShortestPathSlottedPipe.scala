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
package org.neo4j.cypher.internal.runtime.slotted.pipes

import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.NO_ENTITY_FUNCTION
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeWithSource
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFS
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer.TracedPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualValues

import java.util.function.ToLongFunction

import scala.collection.mutable

case class StatefulShortestPathSlottedPipe(
  source: Pipe,
  sourceSlot: Slot,
  intoTargetSlot: Option[Slot],
  commandNFA: CommandNFA,
  preFilters: Option[Predicate],
  selector: StatefulShortestPath.Selector,
  groupSlots: List[Int],
  slots: SlotConfiguration,
  reverseGroupVariableProjections: Boolean
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(source) with Pipe {
  self =>

  private val getSourceNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(sourceSlot, throwOnTypeError = false)

  private val getTargetNodeFunction: ToLongFunction[ReadableRow] = intoTargetSlot.map(slot =>
    makeGetPrimitiveNodeFromSlotFunctionFor(slot, throwOnTypeError = false)
  ).getOrElse(NO_ENTITY_FUNCTION)

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val memoryTracker = state.memoryTrackerForOperatorProvider.memoryTrackerForOperator(id.x)

    val nodeCursor = state.query.nodeCursor()
    state.query.resources.trace(nodeCursor)
    val traversalCursor = state.query.traversalCursor()
    state.query.resources.trace(traversalCursor)

    val hooks = PPBFSHooks.getInstance()
    val pathTracer = new PathTracer(memoryTracker, hooks)
    val pathPredicate =
      preFilters.fold[java.util.function.Predicate[CypherRow]](_ => true)(pred => pred.isTrue(_, state))

    input.flatMap { inputRow =>
      val sourceNode = getSourceNodeFunction.applyAsLong(inputRow)
      val intoTargetNode = getTargetNodeFunction.applyAsLong(inputRow)

      val ppbfs = new PGPathPropagatingBFS(
        sourceNode,
        intoTargetNode,
        commandNFA.compile(inputRow, state),
        state.query.transactionalContext.dataRead,
        nodeCursor,
        traversalCursor,
        pathTracer,
        selector.k.toInt,
        commandNFA.states.size,
        memoryTracker,
        hooks
      )

      ppbfs.iterate(
        withPathVariables(inputRow, _),
        pathPredicate,
        selector.isGroup
      ).asClosingIterator.closing(ppbfs)

    }.closing(nodeCursor).closing(traversalCursor)
  }

  private def withPathVariables(original: CypherRow, path: TracedPath): CypherRow = {
    val row = new SlottedRow(slots)
    row.copyAllFrom(original)
    val groupMap = mutable.HashMap.empty[Int, ListValueBuilder]

    var i = 0
    while (i < path.entities().length) {
      val e = path.entities()(i)
      e.slotOrName match {
        case SlotOrName.Slotted(slotOffset, isGroup) =>
          if (isGroup) {
            groupMap.getOrElseUpdate(slotOffset, ListValueBuilder.newListBuilder()).add(e.idValue)
          } else {
            row.setLongAt(slotOffset, e.id)
          }
        case _: SlotOrName.VarName => throw new IllegalStateException("Legacy metadata in Slotted runtime")
        case SlotOrName.None       => ()
      }
      i += 1
    }

    groupSlots.foreach { offset =>
      val value = groupMap.get(offset) match {
        case Some(list) => if (reverseGroupVariableProjections) list.build().reverse() else list.build()
        case None       => VirtualValues.EMPTY_LIST
      }
      row.setRefAt(offset, value)
    }

    row
  }
}
