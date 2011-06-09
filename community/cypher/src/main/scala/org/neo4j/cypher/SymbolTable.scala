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
package org.neo4j.cypher

import commands.{SymbolType, NodeType, RelationshipType}
import scala.collection.mutable.{Map, Buffer}
import scala.Some

class SymbolTable(val identifiers: Map[String, SymbolType]) {
  def this() = this (Map())

  val columns: Buffer[String] = Buffer()

  def registerNode(name: String) {
    identifiers.get(name) match {
      case Some(RelationshipType(_)) => throw new SyntaxError("Identifier \"" + name + "\" already defined as a relationship.")
      case None => identifiers(name) = NodeType(name)
      case Some(NodeType(_)) =>
    }
  }

  def registerRelationship(name: String) {
    identifiers.get(name) match {
      case Some(NodeType(_)) => throw new SyntaxError("Identifier \"" + name + "\" already defined as a node.")
      case None => identifiers(name) = RelationshipType(name)
      case Some(RelationshipType(_)) =>
    }
  }

  def registerColumn(name: String) {
    columns ++ name
  }

  def assertHas(name: String) {
    if (!identifiers.contains(name)) {
      throw new SyntaxError("Unknown identifier \"" + name + "\".")
    }
  }

  def ++(other: SymbolTable): SymbolTable = {
    identifiers.foreach {
      case (key, value) => {
        other.identifiers.get(key) match {
          case None =>
          case Some(x) => if (!x.getClass.isInstance(value)) {
            throw new SyntaxError("Identifier " + key + " already defined with different type")
          }
        }
      }
    }

    new SymbolTable(identifiers ++ other.identifiers)
  }
}
