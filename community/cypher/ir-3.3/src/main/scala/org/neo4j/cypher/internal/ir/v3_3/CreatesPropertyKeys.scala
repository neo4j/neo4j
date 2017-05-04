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
package org.neo4j.cypher.internal.ir.v3_3

import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression, MapExpression, PropertyKeyName}

/*
 * Used to simplify finding overlap between writing and reading properties
 */
sealed trait CreatesPropertyKeys {
  def overlaps(propertyKeyName: PropertyKeyName): Boolean

  def +(createsPropertyKeys: CreatesPropertyKeys): CreatesPropertyKeys
}

object CreatesPropertyKeys {
  def apply(properties: Expression*): CreatesPropertyKeys = {
    //CREATE ()
    if (properties.isEmpty) CreatesNoPropertyKeys
    else {
      val knownProp: Seq[Seq[(PropertyKeyName, Expression)]] = properties.collect {
        case MapExpression(props) => props
      }
      //all prop keys are known, CREATE ({prop1:1, prop2:2})
      if (knownProp.size == properties.size) CreatesKnownPropertyKeys(knownProp.flatMap(_.map(s => s._1)).toSet)
      //props created are not known, e.g. CREATE ({props})
      else CreatesUnknownPropertyKeys
    }
  }
}

/*
 * CREATE (a:L)
 */
case object CreatesNoPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = false

  override def +(createsPropertyKeys: CreatesPropertyKeys) = createsPropertyKeys
}

/*
 * CREATE ({prop1: 42, prop2: 42})
 */
case class CreatesKnownPropertyKeys(keys: Set[PropertyKeyName]) extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName): Boolean = keys(propertyKeyName)

  override def +(createsPropertyKeys: CreatesPropertyKeys) = createsPropertyKeys match {
    case CreatesNoPropertyKeys => this
    case CreatesKnownPropertyKeys(otherKeys) => CreatesKnownPropertyKeys(keys ++ otherKeys)
    case CreatesUnknownPropertyKeys => CreatesUnknownPropertyKeys
  }
}

object CreatesKnownPropertyKeys {
  def apply(propertyKeyNames: PropertyKeyName*): CreatesKnownPropertyKeys = CreatesKnownPropertyKeys(propertyKeyNames.toSet)
}

/*
 * CREATE ({props})
 */
case object CreatesUnknownPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = true

  override def +(createsPropertyKeys: CreatesPropertyKeys) = CreatesUnknownPropertyKeys
}
