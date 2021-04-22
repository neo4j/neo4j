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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateNodeCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CreateRelationshipCommand
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.NodeValue

trait SideEffect {
  def execute(row: CypherRow, state: QueryState)
}

case class CreateNode(command: CreateNodeCommand, allowNullProperty: Boolean) extends SideEffect {
  override def execute(row: CypherRow,
                       state: QueryState): Unit = {
    val query = state.query
    val labelIds = command.labels.map(_.getOrCreateId(query)).toArray
    val node = query.createNode(labelIds)
    command.properties.foreach(p => p.apply(row, state) match {
      case IsMap(map) =>
        map(state).foreach((k: String, v: AnyValue) => {
          if (v eq Values.NO_VALUE) {
            if (!allowNullProperty) {
              CreateNode.handleNoValue(command.labels.map(_.name), k)
            }
          }
          else {
            val propId = query.getOrCreatePropertyKeyId(k)
            query.nodeOps.setProperty(node.id(), propId, makeValueNeoSafe(v))
          }
        })

      case value =>
        throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")

    })
    row.set(command.idName, node)
  }
}

object CreateNode {
  def handleNoValue(labels: Seq[String], key: String): Unit = {
    val labelsString = if (labels.nonEmpty) ":" + labels.mkString(":") else ""
    throw new InvalidSemanticsException(s"Cannot merge the following node because of null property value for '$key': ($labelsString {$key: null})")
  }
}

case class CreateRelationship(command: CreateRelationshipCommand, allowNullProperty: Boolean) extends SideEffect {
  override def execute(row: CypherRow,
                       state: QueryState): Unit = {
    val start = getNode(row, command.idName, command.startNode, state.lenientCreateRelationship)
    val end = getNode(row, command.idName, command.endNode, state.lenientCreateRelationship)

    val relationship = if (start == null || end == null) {
      Values.NO_VALUE
    } // lenient create relationship NOOPs on missing node
    else {
      val typeId = state.query.getOrCreateRelTypeId(command.relType.name)
      val relationship = state.query.createRelationship(start.id(), end.id(), typeId)
      command.properties.foreach(p => p.apply(row, state) match {
        case IsMap(map) =>
          map(state).foreach((k: String, v: AnyValue) => {
            if (v eq Values.NO_VALUE) {
              if (!allowNullProperty) {
               CreateRelationship.handleNoValue(command.startNode, command.relType.name, command.endNode, k)
              }
            } else {
              val propId = state.query.getOrCreatePropertyKeyId(k)
              state.query.relationshipOps.setProperty(relationship.id(), propId, makeValueNeoSafe(v))
            }
          })

        case value =>
          throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")

      })
      relationship
    }

    row.set(command.idName, relationship)
  }

  private def getNode(row: CypherRow, relName: String, name: String, lenient: Boolean): NodeValue =
    row.getByName(name) match {
      case n: NodeValue => n
      case IsNoValue() =>
        if (lenient) null
        else throw new InternalException(LenientCreateRelationship.errorMsg(relName, name))
      case x => throw new InternalException(s"Expected to find a node at '$name' but found instead: $x")
    }
}

object CreateRelationship {
  def handleNoValue(startVariableName: String, relTypeName:String, endVariableName:String, key: String): Unit = {
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
    throw new InvalidSemanticsException(
      s"Cannot merge the following relationship because of null property value for '$key': ($startVarPart)-[:$relTypeName {$key: null}]->($endVarPart)")
  }
}


