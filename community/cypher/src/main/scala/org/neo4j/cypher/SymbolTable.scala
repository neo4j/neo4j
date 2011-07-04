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

import commands.{Match, UnboundIdentifier, Identifier}
import scala.Some

class SymbolTable(val identifiers: Set[Identifier]) {
  def this(identifier:Identifier)=this(Set(identifier))
  def this(data:Seq[Identifier])=this(data.toSet)
  def this() = this(Set[Identifier]())
  def this(other:SymbolTable) = this(other.identifiers)

  def assertHas(name: String) {
    if (get(name).isEmpty) {
      throw new SyntaxException("Unknown identifier \"" + name + "\".")
    }
  }

  def add(idents: Seq[Identifier]) = this ++ new SymbolTable(idents)

  def get(name: String): Option[Identifier] = identifiers.find(_.name == name)

  def merge(other: SymbolTable) : Set[Identifier] = {
    def handleUnmatched(newIdentifier: Identifier): Identifier = {
      newIdentifier match {
        case UnboundIdentifier(_, _) => throw new SyntaxException("Unbound Identifier " + newIdentifier + " not resolved!")
        case _ => newIdentifier
      }
    }
    def handleMatched(newIdentifier: Identifier, existingIdentifier: Identifier): Identifier = {
      newIdentifier match {
        case UnboundIdentifier(name, None) => existingIdentifier
        case UnboundIdentifier(name, Some(wrapped)) => wrapped
        case _ => {
          if (newIdentifier.getClass == existingIdentifier.getClass) {
            existingIdentifier
          } else {
            throw new SyntaxException("Identifier " + existingIdentifier + " already defined with different type " + newIdentifier)
          }
        }
      }
    }

    identifiers ++
    other.identifiers.map( newIdentifier => {
        get(newIdentifier.name) match {
          case None => handleUnmatched(newIdentifier)
          case Some(existingIdentifier) => handleMatched(newIdentifier, existingIdentifier) } } )
  }

  def ++(other: SymbolTable): SymbolTable = {
    new SymbolTable(merge(other))
  }
}
