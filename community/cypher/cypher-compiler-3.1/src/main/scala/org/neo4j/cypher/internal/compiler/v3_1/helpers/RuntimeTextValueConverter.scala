/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_1.helpers

import org.neo4j.cypher.internal.compiler.v3_1.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v3_1.spi.QueryContext
import org.neo4j.graphdb.{Node, Path, Relationship}

import scala.collection.Map

// Converts scala runtime values to human readable text
//
// Main use: Printing results when using ExecutionEngine
//
class RuntimeTextValueConverter(scalaValues: RuntimeScalaValueConverter)(implicit context: QueryContext) {

  def asTextValue(a: Any): String = {
    val scalaValue = scalaValues.asShallowScalaValue(a)
    scalaValue match {
      case node: Node => s"$node${props(node)}"
      case relationship: Relationship => s":${relationship.getType.name()}[${relationship.getId}]${props(relationship)}"
      case path: Path => path.toString
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

  private def props(n: Node): String = {
    val ops = context.nodeOps
    val properties = ops.propertyKeyIds(n.getId)
    val keyValStrings = properties.map(pkId => s"${context.getPropertyKeyName(pkId)}:${asTextValue(ops.getProperty(n.getId, pkId))}")
    keyValStrings.mkString("{", ",", "}")
  }

  private def props(r: Relationship): String = {
    val ops = context.relationshipOps
    val properties = ops.propertyKeyIds(r.getId)
    val keyValStrings = properties.map(pkId => s"${context.getPropertyKeyName(pkId)}:${asTextValue(ops.getProperty(r.getId, pkId))}")
    keyValStrings.mkString("{", ",", "}")
  }
}
