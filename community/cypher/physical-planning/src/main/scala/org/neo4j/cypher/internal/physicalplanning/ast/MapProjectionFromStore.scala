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
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.physicalplanning.ast.IsNodeProjectionFromStore.equalOffset
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Map projections where all properties needs to be read from store. Used for a runtime optimisation.
 * 
 * Entities needs to be null checked during expression evaluation!
 */
trait MapProjectionFromStore extends Expression {
  override def isConstantForQuery: Boolean = false
}

case class PropertyMapEntry(mapKey: String, property: PropertyFromStore)

case class NodeProjectionFromStore(entityOffset: Int, entries: Seq[PropertyMapEntry])(
  val position: InputPosition
) extends MapProjectionFromStore

case class RelationshipProjectionFromStore(entityOffset: Int, entries: Seq[PropertyMapEntry])(
  val position: InputPosition
) extends MapProjectionFromStore

/**
 * Map projection of the form map{.foo, .bar, baz} where we don't know at compile
 * time if properties are projected from store or from some other map-like structure.
 */
case class PropertyProjection(map: Expression, entries: Seq[PropertyProjectionEntry])(
  val position: InputPosition
) extends MapProjectionFromStore {}

object MapProjectionFromStore {

  def unapply(projection: DesugaredMapProjection): Option[MapProjectionFromStore] = projection match {

    case m @ DesugaredMapProjection(_, IsNodeProjectionFromStore(offset, properties), false) =>
      Some(NodeProjectionFromStore(offset, properties)(m.position))
    case m @ DesugaredMapProjection(_, IsRelationshipProjectionFromStore(offset, properties), false) =>
      Some(RelationshipProjectionFromStore(offset, properties)(m.position))

    case m @ DesugaredMapProjection(map: Expression, IsPropertyProjection(innerMap, propertyProjection), false)
      if map == innerMap =>
      Some(PropertyProjection(map, propertyProjection)(m.position))
    case _ => None
  }
}

object IsNodeProjectionFromStore {

  def unapply(items: Seq[LiteralEntry]): Option[(Int, Seq[PropertyMapEntry])] = {
    val entries = items.collect { case LiteralEntry(key, IsNodePropertyFromStore(p)) => PropertyMapEntry(key.name, p) }
    equalOffset(items, entries)
  }

  def equalOffset(items: Seq[LiteralEntry], entries: Seq[PropertyMapEntry]): Option[(Int, Seq[PropertyMapEntry])] = {
    if (items.size == entries.size) {
      entries.headOption.collect {
        case headEntry if entries.forall(_.property.offset == headEntry.property.offset) =>
          (headEntry.property.offset, entries)
      }
    } else {
      None
    }
  }
}

object IsRelationshipProjectionFromStore {

  def unapply(items: Seq[LiteralEntry]): Option[(Int, Seq[PropertyMapEntry])] = {
    val entries = items.collect { case LiteralEntry(key, IsRelationshipPropertyFromStore(p)) =>
      PropertyMapEntry(key.name, p)
    }
    equalOffset(items, entries)
  }
}

object IsPropertyProjection {

  def unapply(items: Seq[LiteralEntry]): Option[(Expression, Seq[PropertyProjectionEntry])] = {
    val propertiesLookup = items.collect { case LiteralEntry(key, p: LogicalProperty) => key.name -> p }

    if (propertiesLookup.size == items.size) {
      propertiesLookup.headOption.collect {
        case (_, head) if propertiesLookup.map(_._2).forall(_.map == head.map) =>
          (
            head.map,
            propertiesLookup.map {
              case (n, p) => PropertyProjectionEntry(n, p.propertyKey)
            }
          )
      }
    } else {
      None
    }
  }
}

case class PropertyProjectionEntry(key: String, propertyKeyName: PropertyKeyName)
