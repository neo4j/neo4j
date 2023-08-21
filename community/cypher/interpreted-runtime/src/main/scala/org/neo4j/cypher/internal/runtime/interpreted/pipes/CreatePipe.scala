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

import org.eclipse.collections.impl.factory.primitive.IntObjectMaps
import org.neo4j.cypher.internal.macros.AssertMacros.checkOnlyWhenAssertionsAreEnabled
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.LenientCreateRelationship
import org.neo4j.cypher.internal.runtime.WriteOperations
import org.neo4j.cypher.internal.runtime.interpreted.IsMap
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.exceptions.InternalException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues

/**
 * Extends PipeWithSource with methods for setting properties and labels on entities.
 */
abstract class BaseCreatePipe(src: Pipe) extends PipeWithSource(src) {

  /**
   * Set properties on node by delegating to `setProperty`.
   */
  protected def setProperties(
    context: CypherRow,
    state: QueryState,
    entityId: Long,
    properties: Expression,
    ops: WriteOperations[_, _]
  ): Unit = {
    val value = properties(context, state)
    value match {
      case _: VirtualNodeValue | _: VirtualRelationshipValue =>
        throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
      case IsMap(map) =>
        val propertyMap = map(state)
        val propertiesToSet = IntObjectMaps.mutable.withInitialCapacity[Value](propertyMap.size())
        propertyMap.foreach {
          case (_, noValue) if noValue eq Values.NO_VALUE => // Do nothing
          case (name, value) => propertiesToSet.put(state.query.getOrCreatePropertyKeyId(name), makeValueNeoSafe(value))
        }
        ops.setProperties(entityId, propertiesToSet)

      case _ =>
        throw new CypherTypeException(s"Parameter provided for node creation is not a Map, instead got $value")
    }
  }
}

/**
 * Extend BaseCreatePipe with methods to create nodes and relationships from commands.
 */
abstract class EntityCreatePipe(src: Pipe) extends BaseCreatePipe(src) {

  /**
   * Create node from command.
   */
  def createNode(
    context: CypherRow,
    state: QueryState,
    data: CreateNodeCommand
  ): (String, VirtualNodeValue) = {
    val labelIds = data.labels.map(_.getOrCreateId(state.query)).toArray
    val node = state.query.createNodeId(labelIds)

    data.properties.foreach(setProperties(context, state, node, _, state.query.nodeWriteOps))
    data.idName -> VirtualValues.node(node)
  }

  /**
   * Create relationship from command.
   */
  def createRelationship(
    context: CypherRow,
    state: QueryState,
    data: CreateRelationshipCommand
  ): (String, AnyValue) = {
    val start = getNode(context, data.idName, data.startNode, state.lenientCreateRelationship)
    val end = getNode(context, data.idName, data.endNode, state.lenientCreateRelationship)

    val relationship =
      if (start == null || end == null)
        Values.NO_VALUE // lenient create relationship NOOPs on missing node
      else {
        val typeId = data.relType.getOrCreateType(state.query)
        val relationship = state.query.createRelationshipId(start.id(), end.id(), typeId)
        data.properties.foreach(setProperties(context, state, relationship, _, state.query.relationshipWriteOps))
        VirtualValues.relationship(relationship, start.id(), end.id(), typeId)
      }
    data.idName -> relationship
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

/**
 * Creates nodes and relationships from the constructor commands.
 */
case class CreatePipe(src: Pipe, commands: Array[CreateCommand])(
  val id: Id = Id.INVALID_ID
) extends EntityCreatePipe(src) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] =
    input.map(row => {
      commands.foreach { command =>
        val (key, node) = command.createEntity(this, row, state)
        row.set(key, node)
      }
      row
    })
}

sealed trait CreateCommand {
  def createEntity(pipe: CreatePipe, context: CypherRow, state: QueryState): (String, AnyValue)
  def idName: String
}

case class CreateNodeCommand(idName: String, labels: Seq[LazyLabel], properties: Option[Expression])
    extends CreateCommand {
  checkOnlyWhenAssertionsAreEnabled(labels.toSet.size == labels.size)

  override def createEntity(pipe: CreatePipe, context: CypherRow, state: QueryState): (String, AnyValue) =
    pipe.createNode(context, state, this)
}

case class CreateRelationshipCommand(
  idName: String,
  startNode: String,
  relType: LazyType,
  endNode: String,
  properties: Option[Expression]
) extends CreateCommand {

  override def createEntity(pipe: CreatePipe, context: CypherRow, state: QueryState): (String, AnyValue) =
    pipe.createRelationship(context, state, this)
}
