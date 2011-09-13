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

import org.neo4j.cypher.SyntaxException

case class Identifier(name: String) {

  def assertIs( clazz:Class[_] ) {
    if(!clazz.isAssignableFrom( this.getClass ))
      throw new SyntaxException("Expected " + name + " to be of type " + clazz.getSimpleName + ", but it was of type " + this.getClass.getSimpleName)
  }
}

case class PropertyContainerIdentifier(propContainerName:String) extends Identifier(propContainerName)
case class UnboundIdentifier(subName: String, wrapped:Option[Identifier]) extends Identifier(subName)
case class LiteralIdentifier(subName: String) extends Identifier(subName)
case class PropertyIdentifier(entity: String, property: String) extends Identifier(entity + "." + property)
case class AggregationIdentifier(subName: String) extends Identifier(subName)

case class PathIdentifier(path:String) extends ArrayIdentifier(path)
case class NodeIdentifier(subName: String) extends PropertyContainerIdentifier(subName)
case class RelationshipIdentifier(subName: String) extends PropertyContainerIdentifier(subName)

case class ValueIdentifier(subName: String) extends Identifier(subName)

case class ArrayIdentifier(array: String) extends Identifier(array)