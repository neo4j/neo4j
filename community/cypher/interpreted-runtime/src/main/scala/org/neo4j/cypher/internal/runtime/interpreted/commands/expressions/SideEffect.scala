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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CastSupport
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CreateNode.handleNaNValue
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CreateNode.handleNoValue
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateNodeCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateRelationshipCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.DeletePipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.operations.CypherFunctions
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues

trait SideEffect {
  def execute(row: CypherRow, state: QueryState): Unit
}

case class CreateNode(command: CreateNodeCommand, allowNullOrNaNProperty: Boolean) extends SideEffect {

  override def execute(row: CypherRow, state: QueryState): Unit = {
    val query = state.query
    val labelIds = command.labels.map(_.getOrCreateId(query)).toArray
    val node = query.createNodeId(labelIds)
    command.properties.foreach(p =>
      p.apply(row, state) match {
        case IsMap(map) =>
          map(state).foreach {
            case (k, v) if v eq Values.NO_VALUE =>
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
    row.set(command.idName, VirtualValues.node(node))
  }
}

object CreateNode {

  def handleNoValue(labels: Seq[String], key: String): Unit = {
    val labelsString = if (labels.nonEmpty) ":" + labels.mkString(":") else ""
    throw new InvalidSemanticsException(
      s"Cannot merge the following node because of null property value for '$key': ($labelsString {$key: null})"
    )
  }

  def handleNaNValue(labels: Seq[String], key: String): Unit = {
    val labelsString = if (labels.nonEmpty) ":" + labels.mkString(":") else ""
    throw new InvalidSemanticsException(
      s"Cannot merge the following node because of NaN property value for '$key': ($labelsString {$key: NaN})"
    )
  }
}

case class CreateRelationship(command: CreateRelationshipCommand, allowNullOrNaNProperty: Boolean) extends SideEffect {

  override def execute(row: CypherRow, state: QueryState): Unit = {
    val start = getNode(row, command.idName, command.startNode, state.lenientCreateRelationship)
    val end = getNode(row, command.idName, command.endNode, state.lenientCreateRelationship)

    if (start == null || end == null) {
      row.set(command.idName, Values.NO_VALUE)
    } // lenient create relationship NOOPs on missing node
    else {
      val typeId = state.query.getOrCreateRelTypeId(command.relType.name)
      val relationship = state.query.createRelationshipId(start.id(), end.id(), typeId)
      command.properties.foreach(p =>
        p.apply(row, state) match {
          case IsMap(map) =>
            map(state).foreach {
              case (k, v) if v eq Values.NO_VALUE =>
                if (!allowNullOrNaNProperty) {
                  CreateRelationship.handleNoValue(command.startNode, command.relType.name, command.endNode, k)
                }
              case (k, v: FloatingPointValue) if v.isNaN =>
                if (!allowNullOrNaNProperty) {
                  CreateRelationship.handleNaNValue(command.startNode, command.relType.name, command.endNode, k)
                }
              case (k, v) =>
                val propId = state.query.getOrCreatePropertyKeyId(k)
                state.query.relationshipWriteOps.setProperty(relationship, propId, makeValueNeoSafe(v))
            }

          case value =>
            throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")

        }
      )
      row.set(command.idName, VirtualValues.relationship(relationship, start.id(), end.id(), typeId))
    }

  }

  private def getNode(row: CypherRow, relName: String, name: String, lenient: Boolean): VirtualNodeValue =
    row.getByName(name) match {
      case n: VirtualNodeValue => n
      case IsNoValue() =>
        if (lenient) null
        else throw new InternalException(LenientCreateRelationship.errorMsg(relName, name))
      case x => throw new InternalException(s"Expected to find a node at '$name' but found instead: $x")
    }
}

object CreateRelationship {

  private def fail(
    startVariableName: String,
    relTypeName: String,
    endVariableName: String,
    key: String,
    value: String
  ) = {
    val startVarPart =
      if (startVariableName.startsWith(" ")) {
        ""
      } else {
        startVariableName
      }
    val endVarPart =
      if (endVariableName.startsWith(" ")) {
        ""
      } else {
        endVariableName
      }
    s"($startVarPart)-[:$relTypeName {$key: $value}]->($endVarPart)"
    throw new InvalidSemanticsException(
      s"Cannot merge the following relationship because of $value property value for '$key': ($startVarPart)-[:$relTypeName {$key: $value}]->($endVarPart)"
    )
  }

  def handleNoValue(startVariableName: String, relTypeName: String, endVariableName: String, key: String): Unit =
    fail(startVariableName, relTypeName, endVariableName, key, "null")

  def handleNaNValue(startVariableName: String, relTypeName: String, endVariableName: String, key: String): Unit =
    fail(startVariableName, relTypeName, endVariableName, key, "NaN")
}

case class RemoveLabelsOperation(nodeName: String, labels: Seq[LazyLabel], dynamicLabels: Seq[Expression])
    extends SideEffect {

  override def execute(executionContext: CypherRow, state: QueryState): Unit = {
    val value: AnyValue = executionContext.getByName(nodeName)
    if (!(value eq Values.NO_VALUE)) {
      val nodeId = CastSupport.castOrFail[VirtualNodeValue](value).id()
      val labelIds = labels.map(_.getOrCreateId(state.query)) ++ dynamicLabels.map(e => {
        state.query.getOrCreateLabelId(CypherFunctions.asString(e(executionContext, state)))
      })
      state.query.removeLabelsFromNode(nodeId, labelIds.iterator)
    }
  }
}

case class DeleteOperation(expression: Expression, forced: Boolean) extends SideEffect {

  override def execute(executionContext: CypherRow, state: QueryState): Unit = {
    DeletePipe.delete(expression(executionContext, state), state, forced)
  }
}
