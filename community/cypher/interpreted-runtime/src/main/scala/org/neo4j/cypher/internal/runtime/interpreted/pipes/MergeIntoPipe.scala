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

import org.eclipse.collections.api.map.primitive.IntObjectMap
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.PrimitiveLongHelper
import org.neo4j.cypher.internal.runtime.QueryContext
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.ExpandIntoPipe.getRowNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.MergeIntoPipe.mergeIntoIterator
import org.neo4j.cypher.internal.runtime.iterators.BaseRelationshipCursorIterator
import org.neo4j.cypher.internal.runtime.makeValueNeoSafe
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.operations.CypherTypeValueMapper
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.exceptions.ParameterWrongTypeException
import org.neo4j.internal.kernel.api.MutatingEntityCursor
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualValues

case class MergePropertySets(keys: Array[LazyPropertyKey], values: Array[Expression]) {
  require(keys.length == values.length)

  def compute(row: ReadableRow, state: QueryState): IntObjectMap[Value] = {
    if (keys.length == 0) {
      IntObjectMaps.immutable.empty()
    } else {
      val queryContext = state.query
      var i = 0

      val propValues = IntObjectMaps.mutable.ofInitialCapacity[Value](keys.length)
      while (i < keys.length) {
        val propertyKey = keys(i)
        val expression = values(i)
        val maybePropertyKey = propertyKey.id(queryContext) // if the key was already looked up
        val value = makeValueNeoSafe(expression(row, state))

        if (value eq Values.NO_VALUE) {
          if (maybePropertyKey != LazyPropertyKey.UNKNOWN) {
            propValues.put(maybePropertyKey, Values.NO_VALUE)
          }
        } else {
          val propertyId =
            if (maybePropertyKey == LazyPropertyKey.UNKNOWN) {
              queryContext.getOrCreatePropertyKeyId(propertyKey.name)
            } else {
              maybePropertyKey
            }
          propValues.put(propertyId, value)
        }
        i += 1
      }
      propValues
    }
  }
}

object MergePropertySets {
  def apply(kv: (Array[LazyPropertyKey], Array[Expression])): MergePropertySets = new MergePropertySets(kv._1, kv._2)
}

class MergeIntoPipe(
  src: Pipe,
  fromName: String,
  relName: String,
  toName: String,
  direction: SemanticDirection,
  lazyType: LazyType,
  onMatchProperties: MergePropertySets,
  onCreateProperties: MergePropertySets
)(val id: Id = Id.INVALID_ID) extends PipeWithSource(src) {

  override protected def internalCreateResults(
    input: ClosingIterator[CypherRow],
    state: QueryState
  ): ClosingIterator[CypherRow] = {
    val query = state.query
    input.flatMap {
      row =>
        val fromNode = getRowNode(row, fromName)
        val toNode = getRowNode(row, toName)
        val relType = lazyType.getOrCreateType(row, state)

        (fromNode, toNode) match {
          case (IsNoValue(), _) => handleMissingNode(row, state, fromName)
          case (_, IsNoValue()) => handleMissingNode(row, state, toName)
          case (f: VirtualNodeValue, t: VirtualNodeValue) =>
            PrimitiveLongHelper.map(
              mergeIntoIterator(
                query,
                f.id(),
                direction,
                relType,
                t.id(),
                onMatchProperties.compute(row, state),
                onCreateProperties.compute(row, state)
              ),
              r =>
                rowFactory.copyWith(
                  row,
                  relName,
                  VirtualValues.relationship(
                    r,
                    f.id(),
                    t.id(),
                    relType
                  )
                )
            )

          case (other, _) =>
            throw ParameterWrongTypeException.expectedNodeAtFoundInstead(
              fromName,
              String.valueOf(other),
              String.valueOf(other),
              CypherTypeValueMapper.valueType(other)
            )
        }
    }
  }

  private def handleMissingNode(
    row: CypherRow,
    state: QueryState,
    nodeName: String
  ): ClosingIterator[CypherRow] =
    if (state.lenientCreateRelationship) {
      ClosingIterator.single(rowFactory.copyWith(row, relName, Values.NO_VALUE))
    } else {
      throw InvalidArgumentException.createRelationshipMissingNode(relName, nodeName)
    }
}

object MergeIntoPipe {

  def mergeIntoIterator(
    query: QueryContext,
    sourceNodeId: Long,
    direction: SemanticDirection,
    relType: Int,
    targetNodeId: Long,
    onMatchProperties: IntObjectMap[Value],
    onCreateProperties: IntObjectMap[Value]
  ): BaseRelationshipCursorIterator = {

    val nodeCursor = query.nodeCursor()
    val traversalCursor = query.traversalCursor()
    val propertyCursor = query.propertyCursor()
    val cursor: MutatingEntityCursor = query.mergeInto(
      nodeCursor,
      traversalCursor,
      propertyCursor,
      sourceNodeId,
      relType,
      direction,
      targetNodeId,
      onMatchProperties,
      onCreateProperties
    )
    query.resources.trace(nodeCursor)
    query.resources.trace(traversalCursor)
    query.resources.trace(propertyCursor)

    new BaseRelationshipCursorIterator {
      this.relTypeId = relType
      this.source = sourceNodeId
      this.target = targetNodeId

      override protected def fetchNext(): Long =
        if (cursor.next(query)) cursor.reference()
        else {
          StatementConstants.NO_SUCH_RELATIONSHIP
        }

      override protected def storeState(): Unit = {}

      override def close(): Unit = {
        traversalCursor.close()
        nodeCursor.close()
        propertyCursor.close()
      }
    }
  }
}
