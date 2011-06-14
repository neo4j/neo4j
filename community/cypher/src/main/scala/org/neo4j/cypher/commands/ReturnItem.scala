/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.commands

import org.neo4j.graphdb.{PropertyContainer, NotFoundException}
import org.neo4j.cypher.pipes.Pipe

abstract sealed class ReturnItem(val identifier: Identifier) extends (Map[String, Any] => Map[String, Any]) {
  def assertDependencies(source:Pipe)
}

case class EntityOutput(name: String) extends ReturnItem(NodeIdentifier(name)) {
  def apply(m: Map[String, Any]): Map[String, Any] = Map(name -> m.getOrElse(name, throw new NotFoundException))

  def assertDependencies(source: Pipe) {
    source.symbols.assertHas(name)
  }
}

case class PropertyOutput(entityName: String, property: String) extends ReturnItem(PropertyIdentifier(entityName, property)) {
  def apply(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(entityName, throw new NotFoundException).asInstanceOf[PropertyContainer]
    Map(entityName + "." + property -> node.getProperty(property))
  }

  def assertDependencies(source: Pipe) {
    source.symbols.assertHas(entityName)
  }
}

case class NullablePropertyOutput(entity: String, property: String) extends ReturnItem(PropertyIdentifier(entity, property)) {
  def apply(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(entity, throw new NotFoundException).asInstanceOf[PropertyContainer]

    val value = try {
      node.getProperty(property)
    } catch {
      case x: NotFoundException => null
    }

    Map(entity + "." + property -> value)
  }

  def assertDependencies(source: Pipe) {
    source.symbols.assertHas(entity)
  }
}

abstract sealed class AggregationItem(ident: String) extends ReturnItem(AggregationIdentifier(ident))

case class Count(variable: String) extends AggregationItem(variable) {
  def apply(m: Map[String, Any]): Map[String, Any] = m
  def assertDependencies(source: Pipe) {
  }
}