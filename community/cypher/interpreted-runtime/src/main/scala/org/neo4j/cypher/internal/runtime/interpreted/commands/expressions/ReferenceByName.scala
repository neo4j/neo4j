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

import org.eclipse.collections.impl.factory.primitive.IntSets
import org.neo4j.cypher.internal.runtime.ReadableRow
import org.neo4j.cypher.internal.runtime.ValuePopulation
import org.neo4j.cypher.internal.runtime.ValuePopulation.MISSING_NODE
import org.neo4j.cypher.internal.runtime.ValuePopulation.labels
import org.neo4j.cypher.internal.runtime.ValuePopulation.properties
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.EMPTY_STRING
import org.neo4j.values.storable.Values.EMPTY_TEXT_ARRAY
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualNodeValue
import org.neo4j.values.virtual.VirtualRelationshipValue
import org.neo4j.values.virtual.VirtualValues
import org.neo4j.values.virtual.VirtualValues.EMPTY_MAP

case class ReferenceByName(col: String) extends Expression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: collection.Seq[Expression] = Seq.empty

  override def children: collection.Seq[AstNode[_]] = Seq.empty

  override def apply(row: ReadableRow, state: QueryState): AnyValue = row.getByName(col)
}

case class ValuePopulatingReferenceByName(col: String, cachedProperties: Array[(LazyPropertyKey, Expression)])
    extends Expression {

  override def rewrite(f: Expression => Expression): Expression = f(this)

  override def arguments: collection.Seq[Expression] = Seq.empty

  override def children: collection.Seq[AstNode[_]] = Seq.empty

  override def apply(row: ReadableRow, state: QueryState): AnyValue = {
    val result = row.getByName(col)
    if (state.prePopulateResults) {
      result match {
        case node: VirtualNodeValue        => populate(node, row, state)
        case rel: VirtualRelationshipValue => populate(rel, row, state)
        case v                             => v
      }
    } else {
      result
    }
  }

  private def populate(node: VirtualNodeValue, row: ReadableRow, state: QueryState): VirtualNodeValue = {
    node match {
      case n: NodeValue => n
      case _ =>
        val id = node.id()
        val dbAccess = state.query
        val nodeCursor = state.cursors.nodeCursor
        dbAccess.singleNode(id, nodeCursor)
        val elementId = dbAccess.elementIdMapper().nodeElementId(id)
        if (!nodeCursor.next()) {
          // the node has probably been deleted, we still return it but just a bare id
          VirtualValues.nodeValue(id, elementId, EMPTY_TEXT_ARRAY, EMPTY_MAP, true);
        } else {
          val cachedTokens = IntSets.mutable.empty()
          val builder = new MapValueBuilder()
          cachedProperties.foreach {
            case (p, e) =>
              cachedTokens.add(p.id(dbAccess))
              val value = e(row, state)
              builder.add(p.name, value)
          }
          val propertyCursor = state.cursors.propertyCursor
          nodeCursor.properties(
            propertyCursor,
            ALL_PROPERTIES.excluding((value: Int) => cachedTokens.contains(value))
          )
          VirtualValues.nodeValue(
            id,
            elementId,
            labels(dbAccess, nodeCursor.labels()),
            ValuePopulation.properties(propertyCursor, dbAccess, builder)
          )
        }
    }
  }

  private def populate(rel: VirtualRelationshipValue, row: ReadableRow, state: QueryState): VirtualRelationshipValue = {
    rel match {
      case n: RelationshipValue => n
      case _ =>
        val id = rel.id()
        val dbAccess = state.query
        val relCursor = state.cursors.relationshipScanCursor
        dbAccess.singleRelationship(id, relCursor)
        val idMapper = dbAccess.elementIdMapper
        val elementId = idMapper.relationshipElementId(id)

        if (!relCursor.next()) {
          // the relationship has probably been deleted, we still return it but just a bare id
          VirtualValues.relationshipValue(
            id,
            elementId,
            MISSING_NODE,
            MISSING_NODE,
            EMPTY_STRING,
            EMPTY_MAP,
            true
          )
        } else {
          val cachedTokens = IntSets.mutable.empty()
          val builder = new MapValueBuilder()
          cachedProperties.foreach {
            case (p, e) =>
              cachedTokens.add(p.id(state.query))
              val value = e(row, state)
              builder.add(p.name, value)
          }
          val start = VirtualValues.node(relCursor.sourceNodeReference, idMapper)
          val end = VirtualValues.node(relCursor.targetNodeReference, idMapper)
          val propertyCursor = state.cursors.propertyCursor
          relCursor.properties(propertyCursor, ALL_PROPERTIES.excluding((value: Int) => cachedTokens.contains(value)))
          VirtualValues.relationshipValue(
            id,
            elementId,
            start,
            end,
            Values.stringValue(dbAccess.relationshipTypeName(relCursor.`type`)),
            properties(propertyCursor, dbAccess, builder)
          )
        }
    }
  }
}
