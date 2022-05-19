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
package org.neo4j.cypher.internal.physicalplanning.ast

import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LiteralEntry
import org.neo4j.cypher.internal.util.InputPosition

/**
 * Map projections where all properties needs to be read from store. Used for a runtime optimisation.
 * 
 * Entities needs to be null checked during expression evaluation!
 */
trait MapProjectionFromStore extends Expression

case class NodeProjectionFromStore(entityOffset: Int, properties: Seq[NodePropertyFromStore])(
  val position: InputPosition
) extends MapProjectionFromStore

case class RelationshipProjectionFromStore(entityOffset: Int, properties: Seq[RelationshipPropertyFromStore])(
  val position: InputPosition
) extends MapProjectionFromStore

object MapProjectionFromStore {

  def unapply(projection: DesugaredMapProjection): Option[MapProjectionFromStore] = projection match {
    case m @ DesugaredMapProjection(_, PropertiesFromStore(offset, properties), false) =>
      if (properties.forall(_.isInstanceOf[NodePropertyFromStore])) {
        Some(NodeProjectionFromStore(
          offset,
          properties.map(_.asInstanceOf[NodePropertyFromStore])
        )(m.position))
      } else if (properties.forall(_.isInstanceOf[RelationshipPropertyFromStore])) {
        Some(RelationshipProjectionFromStore(
          offset,
          properties.map(_.asInstanceOf[RelationshipPropertyFromStore])
        )(m.position))
      } else {
        None
      }
    case _ => None
  }
}

object PropertiesFromStore {

  def unapply(items: Seq[LiteralEntry]): Option[(Int, Seq[PropertyFromStore])] = {
    val expressions = items.map(_.exp)
    expressions.headOption match {
      case Some(PropertyFromStore(p)) =>
        expressions.tail.foldLeft(Option((p.offset, Seq(p)))) {
          case (Some((offset, ps)), PropertyFromStore(p)) if p.offset == offset => Some((offset, ps :+ p))
          case _                                                                => None
        }
      case _ => None
    }
  }
}
