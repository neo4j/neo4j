/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.helpers

import org.neo4j.cypher.internal.runtime.{QueryTransactionalContext, RuntimeScalaValueConverter}
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.KeyToken
import org.neo4j.graphdb.Result.ResultRow
import org.neo4j.graphdb.{Entity, Node, Path, Relationship}
import org.neo4j.internal.kernel.api.{PropertyCursor, Transaction}

import scala.collection.mutable.ArrayBuffer

// Converts scala runtime values to human readable text
//
// Main use: Printing results when using ExecutionEngine
//
class RuntimeTextValueConverter(scalaValues: RuntimeScalaValueConverter,
                                txContext: QueryTransactionalContext) {

  def asTextValue(a: Any): String = {
    val scalaValue = scalaValues.asShallowScalaValue(a)
    scalaValue match {
      case node: Node => s"Node[${node.getId}]${props(node)}"
      case relationship: Relationship => s":${relationship.getType.name()}[${relationship.getId}]${props(relationship)}"
      case path: Path => pathAsTextValue(path)
      case map: Map[_, _] => makeString(map)
      case opt: Option[_] => opt.map(asTextValue).getOrElse("None")
      case array: Array[_] => array.map(elem => asTextValue(elem)).mkString("[", ",", "]")
      case iterable: Iterable[_] => iterable.map(elem => asTextValue(elem)).mkString("[", ",", "]")
      case str: String => "\"" + str + "\""
      case token: KeyToken => token.name
      case null => "<null>"
      case value => value.toString
    }
  }

  private def makeString(m: Map[_, _]) = m.map { case (k, v) => s"$k -> ${asTextValue(v)}" }.mkString("{", ", ", "}")

  def dumpRowToString(columns: Array[String], row: ResultRow): Map[String, String] = {
    val map = Map.newBuilder[String, String]
    for (c <- columns) {
      map += c -> asTextValue(row.get(c))
    }
    map.result()
  }

  def pathAsTextValue(path: Path): String = {
    val nodes = path.nodes().iterator()
    val relationships = path.relationships().iterator()
    val tx = txContext.transaction
    val sb = new StringBuilder

    def formatNode(n: Node) = {
      val isDeleted = tx.dataRead().nodeDeletedInTransaction(n.getId)
      val deletedString = if (isDeleted) ",deleted" else ""
      sb ++= s"(${n.getId}$deletedString)"
    }

    def formatRelationship(leftNode: Node, r: Relationship) = {
      val isDeleted = tx.dataRead().relationshipDeletedInTransaction(r.getId)
      val deletedString = if (isDeleted) ",deleted" else ""
      if (r.getStartNode != leftNode)
        sb += '<'
      sb ++= s"-[${r.getId}:${r.getType}$deletedString]-"
      if (r.getEndNode != leftNode)
        sb += '>'
    }

    var n = nodes.next()
    formatNode(n)
    while (relationships.hasNext) {
      val r = relationships.next()
      formatRelationship(n, r)
      n = nodes.next()
      formatNode(n)
    }

    sb.result()
  }

  private def props(n: Node): String = {
    val tx = txContext.transaction
    if (tx.dataRead().nodeDeletedInTransaction(n.getId))
      "{deleted}"
    else if (isVirtualEntityHack(n))
      "{}"
    else {
      val nodeCursor = tx.cursors().allocateNodeCursor()
      val propertyCursor = tx.cursors().allocatePropertyCursor()

      try {

        tx.dataRead().singleNode(n.getId, nodeCursor)

        if (nodeCursor.next()) {
          nodeCursor.properties(propertyCursor)
          propertiesAsTextValue(propertyCursor)
        } else "{}"

      } finally {
        propertyCursor.close()
        nodeCursor.close()
      }
    }
  }

  private def props(r: Relationship): String = {
    val tx = txContext.transaction
    if (tx.dataRead().relationshipDeletedInTransaction(r.getId))
      "{deleted}"
    else if (isVirtualEntityHack(r))
      "{}"
    else {
      val relationshipCursor = tx.cursors().allocateRelationshipScanCursor()
      val propertyCursor = tx.cursors().allocatePropertyCursor()

      try {

        tx.dataRead().singleRelationship(r.getId, relationshipCursor)

        if (relationshipCursor.next()) {
          relationshipCursor.properties(propertyCursor)
          propertiesAsTextValue(propertyCursor)
        } else "{}"

      } finally {
        propertyCursor.close()
        relationshipCursor.close()
      }
    }
  }

  private def propertiesAsTextValue(propertyCursor: PropertyCursor): String = {
    val keyValues = new ArrayBuffer[String]
    val tokenRead = txContext.transaction.tokenRead()
    while (propertyCursor.next()) {
      val key = tokenRead.propertyKeyName(propertyCursor.propertyKey())
      val value = asTextValue(propertyCursor.propertyValue().asObject())
      keyValues.append(s"$key:$value")
    }
    keyValues.mkString("{", ",", "}")
  }

  private def isVirtualEntityHack(entity:Entity): Boolean = entity.getId < 0
}
