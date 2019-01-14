/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.{LenientCreateRelationship, Operations, QueryContext}
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{Value, Values}
import org.neo4j.values.virtual.{NodeValue, RelationshipValue}
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.{CypherTypeException, InternalException, InvalidSemanticsException}

/**
  * Extends PipeWithSource with methods for setting properties and labels on entities.
  */
abstract class BaseCreatePipe(src: Pipe) extends PipeWithSource(src) {

  /**
    * Set properties on node by delegating to `setProperty`.
    */
  protected def setProperties(context: ExecutionContext,
                              state: QueryState,
                              entityId: Long,
                              properties: Expression,
                              ops: Operations[_]): Unit =
    properties(context, state) match {
      case _: NodeValue | _: RelationshipValue =>
        throw new CypherTypeException("Parameter provided for node creation is not a Map")
      case IsMap(map) =>
        map(state.query).foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
          override def accept(k: String, v: AnyValue): Unit = setProperty(entityId, k, v, state.query, ops)
        })

      case _ =>
        throw new CypherTypeException("Parameter provided for node creation is not a Map")
    }

  /**
    * Set property on node, or call `handleNoValue` if value is `NO_VALUE`.
    */
  protected def setProperty(entityId: Long,
                            key: String,
                            value: AnyValue,
                            qtx: QueryContext,
                            ops: Operations[_]): Unit = {
    //do not set properties for null values
    if (value == Values.NO_VALUE) {
      handleNoValue(key)
    } else {
      val propertyKeyId = qtx.getOrCreatePropertyKeyId(key)
      ops.setProperty(entityId, propertyKeyId, makeValueNeoSafe(value))
    }
  }

  /**
    * Callback for when setProperty encounters a NO_VALUE
    *
    * @param key the property key associated with the NO_VALUE
    */
  protected def handleNoValue(key: String): Unit
}

/**
  * Extend BaseCreatePipe with methods to create nodes and relationships from commands.
  */
abstract class EntityCreatePipe(src: Pipe) extends BaseCreatePipe(src) {

  /**
    * Create node from command.
    */
  protected def createNode(context: ExecutionContext,
                           state: QueryState,
                           data: CreateNodeCommand): (String, NodeValue) = {
    val labelIds = data.labels.map(_.getOrCreateId(state.query).id).toArray
    val node = state.query.createNode(labelIds)
    data.properties.foreach(setProperties(context, state, node.id(), _, state.query.nodeOps))
    data.idName -> node
  }

  /**
    * Create relationship from command.
    */
  protected def createRelationship(context: ExecutionContext,
                                   state: QueryState,
                                   data: CreateRelationshipCommand): (String, AnyValue) = {
    val start = getNode(context, data.idName, data.startNode, state.lenientCreateRelationship)
    val end = getNode(context, data.idName, data.endNode, state.lenientCreateRelationship)

    val relationship =
      if (start == null || end == null)
        Values.NO_VALUE // lenient create relationship NOOPs on missing node
      else {
        val typeId = data.relType.typ(state.query)
        val relationship = state.query.createRelationship(start.id(), end.id(), typeId)
        data.properties.foreach(setProperties(context, state, relationship.id(), _, state.query.relationshipOps))
        relationship
      }
    data.idName -> relationship
  }

  private def getNode(row: ExecutionContext, relName: String, name: String, lenient: Boolean): NodeValue =
    row.get(name) match {
      case Some(n: NodeValue) => n
      case Some(Values.NO_VALUE) =>
        if (lenient) null
        else throw new InternalException(LenientCreateRelationship.errorMsg(relName, name))
      case Some(x) => throw new InternalException(s"Expected to find a node at '$name' but found instead: $x")
      case None => throw new InternalException(s"Expected to find a node at '$name' but found instead: null")
    }
}

/**
  * Creates nodes and relationships from the constructor commands.
  */
case class CreatePipe(src: Pipe, nodes: Array[CreateNodeCommand], relationships: Array[CreateRelationshipCommand])
                     (val id: Id = Id.INVALID_ID) extends EntityCreatePipe(src) {

  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(row => {
      nodes.foreach { nodeCommand =>
        val (key, node) = createNode(row, state, nodeCommand)
        row.set(key, node)
      }

      relationships.foreach{ relCommand =>
        val (key, node) = createRelationship(row, state, relCommand)
        row.set(key, node)
      }

      row
    })

  override protected def handleNoValue(key: String) {
    // do nothing
  }
}

case class CreateNodeCommand(idName: String,
                             labels: Seq[LazyLabel],
                             properties: Option[Expression])

case class CreateRelationshipCommand(idName: String,
                                     startNode: String,
                                     relType: LazyType,
                                     endNode: String,
                                     properties: Option[Expression])

/**
  * Create a node corresponding to the constructor command.
  *
  * Differs from CreatePipe in that it throws on NO_VALUE properties. Merge cannot use null properties,
  * * since in that case the match part will not find the result of the create.
  */
case class MergeCreateNodePipe(src: Pipe, data: CreateNodeCommand)
                              (val id: Id = Id.INVALID_ID) extends EntityCreatePipe(src) {

  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(inRow => {
      val (idName, node) = createNode(inRow, state, data)
      inRow.copyWith(idName, node)
    })

  override protected def handleNoValue(key: String): Unit = {
    throw new InvalidSemanticsException(s"Cannot merge node using null property value for $key")
  }
}

/**
  * Create a relationship corresponding to the constructor command.
  *
  * Differs from CreatePipe in that it throws on NO_VALUE properties. Merge cannot use null properties,
  * since in that case the match part will not find the result of the create.
  */
case class MergeCreateRelationshipPipe(src: Pipe, data: CreateRelationshipCommand)
                                      (val id: Id = Id.INVALID_ID)
  extends EntityCreatePipe(src) {

  override def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState): Iterator[ExecutionContext] =
    input.map(inRow => {
      val (idName, relationship) = createRelationship(inRow, state, data)
      inRow.copyWith(idName, relationship)
    })

  override protected def handleNoValue(key: String): Unit = {
    throw new InvalidSemanticsException(s"Cannot merge relationship using null property value for $key")
  }
}
