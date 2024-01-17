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

import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.ClosingIterator.JavaAutoCloseableIteratorAsClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException
import org.neo4j.internal.kernel.api.helpers.traversal.SlotOrName
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFS
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PathTracer.TracedPath
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.hooks.PPBFSHooks
import org.neo4j.values.virtual.ListValueBuilder
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues

import scala.collection.mutable

case class StatefulShortestPathPipe(
  source: Pipe,
  sourceNodeName: String,
  intoTargetNodeName: Option[String],
  commandNFA: CommandNFA,
  preFilters: Option[Predicate],
  selector: StatefulShortestPath.Selector,
  grouped: Set[String],
  reverseGroupVariableProjections: Boolean
)(val id: Id = Id.INVALID_ID)
    extends PipeWithSource(source) {
  self =>

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
      // TODO will blow up if not a VirtualNodeValue, clean up later
      val intoTargetNodeId: Long =
        intoTargetNodeName.map(inputRow.getByName).map(_.asInstanceOf[VirtualNodeValue].id()).getOrElse(-1L)
      inputRow.getByName(sourceNodeName) match {

        case sourceNode: VirtualNodeValue =>
          PGPathPropagatingBFS.create(
            sourceNode.id(),
            intoTargetNodeId,
            commandNFA.compile(inputRow, state),
            state.query.transactionalContext.dataRead,
            nodeCursor,
            traversalCursor,
            pathTracer,
            withPathVariables(inputRow, _),
            pathPredicate,
            selector.isGroup,
            selector.k.toInt,
            commandNFA.states.size,
            memoryTracker,
            hooks
          ).asSelfClosingIterator

        case value =>
          throw new InternalException(
            s"Expected to find a node at '($sourceNodeName)' but found $value instead"
          )
      }
    }.closing(nodeCursor).closing(traversalCursor)
  }

  private def withPathVariables(original: CypherRow, path: TracedPath): CypherRow = {
    val row = original.createClone()

    val groupMap = mutable.HashMap.empty[String, ListValueBuilder]

    var i = 0
    while (i < path.entities().length) {
      val e = path.entities()(i)
      e.slotOrName match {
        case SlotOrName.VarName(varName, isGroup) =>
          if (isGroup) {
            groupMap.getOrElseUpdate(varName, ListValueBuilder.newListBuilder())
              .add(e.idValue)
          } else {
            row.set(varName, e.idValue)
          }
        case _: SlotOrName.Slotted => throw new IllegalStateException("Slotted metadata in Legacy runtime")
        case SlotOrName.None       => ()
      }
      i += 1
    }

    grouped.foreach { name =>
      val value = groupMap.get(name) match {
        case Some(list) => if (reverseGroupVariableProjections) list.build().reverse() else list.build()
        case None       => VirtualValues.EMPTY_LIST
      }
      row.set(name, value)
    }

    row
  }

}
