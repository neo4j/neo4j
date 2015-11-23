/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner

import org.neo4j.cypher.internal.frontend.v3_0.ast.{Expression, MapExpression, PropertyKeyName}

/*
 * Used to simplify finding overlap between writing and reading properties
 */
sealed trait CreatesPropertyKeys {
  def overlaps(propertyKeyName: PropertyKeyName): Boolean
}

/*
 * CREATE (a:L)
 */
case object CreatesNoPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = false
}

/*
 * CREATE ({prop1: 42, prop2: 42})
 */
case class CreatesKnownPropertyKeys(keys: Set[PropertyKeyName]) extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName): Boolean = keys(propertyKeyName)
}

/*
 * CREATE ({props})
 */
case object CreatesUnknownPropertyKeys extends CreatesPropertyKeys {
  override def overlaps(propertyKeyName: PropertyKeyName) = true
}

object CreatesPropertyKeys {
  def apply(properties: Seq[Expression]) = {
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

