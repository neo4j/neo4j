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
package org.neo4j.cypher.cucumber.glue.regular.steps

import io.cucumber.datatable.DataTable
import org.eclipse.collections.api.set.MutableSet
import org.eclipse.collections.api.set.primitive.IntSet
import org.eclipse.collections.api.set.primitive.LongSet
import org.eclipse.collections.impl.factory.Sets
import org.eclipse.collections.impl.factory.primitive.IntSets
import org.eclipse.collections.impl.factory.primitive.LongSets
import org.neo4j.cypher.cucumber.util.KernelOperation
import org.neo4j.cypher.cucumber.value.ResultValueMapper
import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.values.storable.DoubleValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

import scala.util.Using

/** Side effects recorded by examining the graph. These are different from query statistics! */
case class SideEffects(
  nodesCreated: Int,
  nodesDeleted: Int,
  relsCreated: Int,
  relsDeleted: Int,
  labelsAdded: Int,
  labelsRemoved: Int,
  propsAdded: Int,
  propsRemoved: Int
) {

  def asDataTable: DataTable = DataTable.create(java.util.List.of(
    java.util.List.of("+nodes", nodesCreated.toString),
    java.util.List.of("-nodes", nodesDeleted.toString),
    java.util.List.of("+relationships", relsCreated.toString),
    java.util.List.of("-relationships", relsDeleted.toString),
    java.util.List.of("+labels", labelsAdded.toString),
    java.util.List.of("-labels", labelsRemoved.toString),
    java.util.List.of("+properties", propsAdded.toString),
    java.util.List.of("-properties", propsRemoved.toString)
  ))

  override def toString: String = asDataTable.toString
}

object SideEffects {

  def from(table: DataTable): SideEffects = {
    if (table.isEmpty) {
      SideEffects(0, 0, 0, 0, 0, 0, 0, 0)
    } else {
      val map = table.asMap(classOf[String], classOf[java.lang.Integer])
      SideEffects(
        nodesCreated = map.getOrDefault("+nodes", 0),
        nodesDeleted = map.getOrDefault("-nodes", 0),
        relsCreated = map.getOrDefault("+relationships", 0),
        relsDeleted = map.getOrDefault("-relationships", 0),
        labelsAdded = map.getOrDefault("+labels", 0),
        labelsRemoved = map.getOrDefault("-labels", 0),
        propsAdded = map.getOrDefault("+properties", 0),
        propsRemoved = map.getOrDefault("-properties", 0)
      )
    }
  }
}

sealed trait GraphState

case class KernelGraphState(
  nodeIds: LongSet,
  relIds: LongSet,
  labels: IntSet,
  props: MutableSet[KernelGraphState.Property]
) extends GraphState {

  def sideEffects(other: KernelGraphState): SideEffects = SideEffects(
    nodesCreated = other.nodeIds.difference(nodeIds).size(),
    nodesDeleted = nodeIds.difference(other.nodeIds).size(),
    relsCreated = other.relIds.difference(relIds).size(),
    relsDeleted = relIds.difference(other.relIds).size(),
    labelsAdded = other.labels.difference(labels).size(),
    labelsRemoved = labels.difference(other.labels).size(),
    propsAdded = other.props.count(v => !props.contains(v)),
    propsRemoved = props.count(v => !other.props.contains(v))
  )
}

object KernelGraphState {
  case class Property(entityId: Long, isNode: Boolean, propKey: Int, propValue: Value)

  def recordGraphState(db: GraphDatabaseAPI): KernelGraphState = KernelOperation.withKernelTx(db) { tx =>
    val nodeIds = LongSets.mutable.empty()
    val relIds = LongSets.mutable.empty()
    val labelIds = IntSets.mutable.empty()
    val props = Sets.mutable.empty[Property]()

    Using.resources(
      tx.cursors().allocateNodeCursor(tx.cursorContext()),
      tx.cursors().allocateRelationshipScanCursor(tx.cursorContext()),
      tx.cursors().allocatePropertyCursor(tx.cursorContext(), tx.memoryTracker())
    ) { (nodeCur, relCur, propCur) =>
      tx.dataRead().allNodesScan(nodeCur)
      while (nodeCur.next()) {
        val id = nodeCur.nodeReference()
        nodeIds.add(id)
        labelIds.addAll(nodeCur.labels().all(): _*)

        nodeCur.properties(propCur)
        while (propCur.next()) {
          props.add(Property(id, isNode = true, propCur.propertyKey(), replaceNan(propCur.propertyValue())))
        }
      }

      tx.dataRead().allRelationshipsScan(relCur)
      while (relCur.next()) {
        val id = relCur.relationshipReference()
        relIds.add(relCur.relationshipReference())

        relCur.properties(propCur)
        while (propCur.next()) {
          props.add(Property(id, isNode = false, propCur.propertyKey(), replaceNan(propCur.propertyValue())))
        }
      }
    }
    KernelGraphState(nodeIds, relIds, labelIds, props)
  }

  def replaceNan(value: Value): Value = value match {
    case double: DoubleValue if double.value().isNaN =>
      // NaN values are not equal to each other.
      Values.stringValue("!!!This is not a string, it's a NaN???")
    case _ => value
  }
}

case class CypherGraphState(
  nodeIds: LongSet,
  relIds: LongSet,
  labels: MutableSet[String],
  props: MutableSet[CypherGraphState.Property]
) extends GraphState {

  def sideEffects(other: CypherGraphState): SideEffects = SideEffects(
    nodesCreated = other.nodeIds.difference(nodeIds).size(),
    nodesDeleted = nodeIds.difference(other.nodeIds).size(),
    relsCreated = other.relIds.difference(relIds).size(),
    relsDeleted = relIds.difference(other.relIds).size(),
    labelsAdded = other.labels.difference(labels).size(),
    labelsRemoved = labels.difference(other.labels).size(),
    propsAdded = other.props.count(v => !props.contains(v)),
    propsRemoved = props.count(v => !other.props.contains(v))
  )
}

object CypherGraphState {
  case class Property(entityId: Long, isNode: Boolean, propKey: String, propValue: Value)

  def recordGraphState(tx: CypherExecutorTransaction): CypherGraphState = {
    val nodeIds = LongSets.mutable.empty()
    val relIds = LongSets.mutable.empty()
    val labelIds = Sets.mutable.empty[String]()
    val props = Sets.mutable.empty[Property]()

    tx.execute("match (n) return id(n)").consume(ResultValueMapper).rows
      .forEach(row => nodeIds.add(row.get(0).asInstanceOf[Long]))
    tx.execute("match ()-[r]->() return id(r)").consume(ResultValueMapper).rows
      .forEach(row => relIds.add(row.get(0).asInstanceOf[Long]))
    tx.execute("match (n) unwind labels(n) as label return distinct label").consume(ResultValueMapper).rows
      .forEach(row => labelIds.add(row.get(0).asInstanceOf[String]))
    tx.execute(
      """match (n)
        |with n, properties(n) as props
        |unwind keys(props) AS key
        |return id(n) AS id, true as isNode, key, props[key] AS value
        |union all
        |match ()-[r]->()
        |with r, properties(r) as props
        |unwind keys(props) AS key
        |return id(r) AS id, false as isNode, key, props[key] AS value
        |""".stripMargin
    ).consume(ResultValueMapper).rows
      .forEach { row =>
        val propValue = row.get(3) match {
          case list: java.util.List[_] if list.isEmpty => new Array[String](0)
          case list: java.util.List[_] =>
            val itemCls = list.getFirst.getClass
            list.stream()
              .map(i => itemCls.cast(i))
              .toArray(i => java.lang.reflect.Array.newInstance(itemCls, i).asInstanceOf[Array[AnyRef]])
          case other => other
        }
        props.add(Property(
          entityId = row.get(0).asInstanceOf[Long],
          isNode = row.get(1).asInstanceOf[Boolean],
          propKey = row.get(2).asInstanceOf[String],
          propValue = KernelGraphState.replaceNan(Values.of(propValue))
        ))
      }
    CypherGraphState(nodeIds, relIds, labelIds, props)
  }
}
