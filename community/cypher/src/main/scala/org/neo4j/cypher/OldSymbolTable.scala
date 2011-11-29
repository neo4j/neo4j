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

import commands.{UnboundIdentifier, OldIdentifier}

class OldSymbolTable(val identifiers: Set[OldIdentifier]) {
  def this(identifier: OldIdentifier) = this (Set(identifier))

  def this(data: Seq[OldIdentifier]) = this (data.toSet)

  def this() = this (Set[OldIdentifier]())

  def this(other: OldSymbolTable) = this (other.identifiers)

  def assertHas(expected: OldIdentifier) {
    val name = expected.name

    val actual = getOrElse(name, () => throw new SyntaxException("Unknown identifier \"" + name + "\"."))
    if (!expected.getClass.isAssignableFrom(actual.getClass)) {
      throw new SyntaxException("Expected `" + name + "` to be a " + expected.getClass.getSimpleName + " but it was " + actual.getClass.getSimpleName)
    }
  }

  def add(idents: Seq[OldIdentifier]): OldSymbolTable = this ++ new OldSymbolTable(idents)
  def add(ident:OldIdentifier): OldSymbolTable = add(Seq(ident))

  def get(name: String): Option[OldIdentifier] = identifiers.find(_.name == name)

  def getOrElse(name: String, f: () => OldIdentifier): OldIdentifier = {
    val option = get(name)
    option match {
      case Some(id) => id
      case None => f()
    }
  }

  def merge(other: OldSymbolTable): Set[OldIdentifier] = {
    val matchedIdentifiers = other.identifiers.map(newIdentifier => get(newIdentifier.name) match {
      case None => handleUnmatched(newIdentifier)
      case Some(existingIdentifier) => handleMatched(newIdentifier, existingIdentifier)
    })

    identifiers ++ matchedIdentifiers
  }

  private def handleMatched(newIdentifier: OldIdentifier, existingIdentifier: OldIdentifier): OldIdentifier = {
    newIdentifier match {
      case UnboundIdentifier(name, None) => existingIdentifier
      case UnboundIdentifier(name, Some(wrapped)) => wrapped
      case _ => if (newIdentifier.getClass.isAssignableFrom(existingIdentifier.getClass)) {
        existingIdentifier
      } else {
        throw new SyntaxException("Identifier " + existingIdentifier + " already defined with different type " + newIdentifier)
      }
    }
  }


  private def handleUnmatched(newIdentifier: OldIdentifier): OldIdentifier = {
    newIdentifier match {
      case UnboundIdentifier(_, _) => throw new SyntaxException("Unbound Identifier " + newIdentifier + " not resolved!")
      case _ => newIdentifier
    }
  }

  def ++(other: OldSymbolTable): OldSymbolTable = {
    new OldSymbolTable(merge(other))
  }

  def columns : String = identifiers.map(_.name).mkString(", ")
}
