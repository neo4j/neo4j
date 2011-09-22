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



abstract sealed class StartItem(val variable:String)

abstract class RelationshipStartItem(varName:String) extends StartItem(varName)
abstract class NodeStartItem(varName:String) extends StartItem(varName)

case class RelationshipById(varName:String, id: Long*) extends RelationshipStartItem(varName)
case class RelationshipByIndex(varName:String, idxName: String, key:String, value: Any) extends RelationshipStartItem(varName)

case class NodeByIndex(varName:String, idxName: String, key:Value, value: Value) extends NodeStartItem(varName)
case class NodeByIndexQuery(varName:String, idxName: String, query: Value) extends NodeStartItem(varName)
case class NodeById(varName:String, value:Value) extends NodeStartItem(varName)

object NodeById {
  def apply(varName:String, id: Long*) = new NodeById(varName, Literal(id))
}