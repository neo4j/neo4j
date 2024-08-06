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
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.physicalplanning.Slot
import org.neo4j.cypher.internal.physicalplanning.SlotConfigurationUtils.makeGetPrimitiveNodeFromSlotFunctionFor
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CreateNode.handleNaNValue
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CreateNode.handleNoValue
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CreateRelationship
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.SideEffect
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateNodeSlottedCommand
import org.neo4j.cypher.internal.runtime.slotted.pipes.CreateRelationshipSlottedCommand
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.kernel.api.StatementConstants.NO_SUCH_RELATIONSHIP
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.Values

import scala.jdk.CollectionConverters.CollectionHasAsScala

trait SlottedSideEffect extends SideEffect

case class CreateSlottedNode(command: CreateNodeSlottedCommand, allowNullOrNaNProperty: Boolean)
    extends SlottedSideEffect {

  override def execute(row: CypherRow, state: QueryState): Unit = {
    val query = state.query
    val labelIds = command.labels.map(_.getOrCreateId(query)).toArray
    val node = query.createNodeId(labelIds)
    row.setLongAt(command.idOffset, node)
    command.properties.foreach(p =>
      p.apply(row, state) match {
        case IsMap(map) =>
          map(state).foreach {
            case (k, v) if (v eq Values.NO_VALUE) =>
              if (!allowNullOrNaNProperty) {
                handleNoValue(command.labels.map(_.name), k)
              }
            case (k, v: FloatingPointValue) if !allowNullOrNaNProperty && v.isNaN =>
              handleNaNValue(command.labels.map(_.name), k)
            case (k, v) =>
              val propId = query.getOrCreatePropertyKeyId(k)
              query.nodeWriteOps.setProperty(node, propId, makeValueNeoSafe(v))
          }

        case value =>
          throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")

      }
    )
  }
}

case class CreateSlottedRelationship(command: CreateRelationshipSlottedCommand, allowNullOrNaNProperty: Boolean)
    extends SlottedSideEffect {

  override def execute(row: CypherRow, state: QueryState): Unit = {
    def handleMissingNode(nodeName: String) =
      if (state.lenientCreateRelationship) NO_SUCH_RELATIONSHIP
      else throw new InternalException(LenientCreateRelationship.errorMsg(command.relName, nodeName))

    val start = command.startNodeIdGetter.applyAsLong(row)
    val end = command.endNodeIdGetter.applyAsLong(row)

    val relationship =
      if (start == StatementConstants.NO_SUCH_NODE) handleMissingNode(command.startName)
      else if (end == StatementConstants.NO_SUCH_NODE) handleMissingNode(command.endName)
      else {
        val typeId = state.query.getOrCreateRelTypeId(command.relType.name)
        val relationship = state.query.createRelationshipId(start, end, typeId)
        command.properties.foreach(p =>
          p.apply(row, state) match {
            case IsMap(map) =>
              map(state).foreach {
                case (k: String, v: AnyValue) if v eq Values.NO_VALUE =>
                  if (!allowNullOrNaNProperty) {
                    CreateRelationship.handleNoValue(command.startName, command.relType.name, command.endName, k)
                  }
                case (k: String, v: FloatingPointValue) if !allowNullOrNaNProperty && v.isNaN =>
                  CreateRelationship.handleNaNValue(command.startName, command.relType.name, command.endName, k)
                case (k: String, v: AnyValue) =>
                  val propId = state.query.getOrCreatePropertyKeyId(k)
                  state.query.relationshipWriteOps.setProperty(relationship, propId, makeValueNeoSafe(v))
              }

            case value =>
              throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
          }
        )
        relationship
      }
    row.setLongAt(command.idOffset, relationship)
  }
}

case class SlottedRemoveLabelsOperation(nodeSlot: Slot, labels: Seq[LazyLabel], dynamicLabels: Seq[Expression])
    extends SideEffect {
  private val getFromNodeFunction = makeGetPrimitiveNodeFromSlotFunctionFor(nodeSlot)

  override def execute(executionContext: CypherRow, state: QueryState): Unit = {
    val node = getFromNodeFunction.applyAsLong(executionContext)
    if (node != StatementConstants.NO_SUCH_NODE) {
      val labelIds = labels.map(_.getOrCreateId(state.query)) ++ dynamicLabels.flatMap(e => {
        CypherFunctions.asStringList(e(executionContext, state)).asScala.map(l => state.query.getOrCreateLabelId(l))
      })
      state.query.removeLabelsFromNode(node, labelIds.iterator)
    }
  }
}
