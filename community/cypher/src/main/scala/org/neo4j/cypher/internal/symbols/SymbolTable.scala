/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.symbols

import org.neo4j.cypher.{CypherTypeException, SyntaxException}
import org.neo4j.cypher.internal.commands.Expression


class SymbolTable(val identifiers: Identifier*) {
  assertNoDuplicatesExist()

  def satisfies(needs: Seq[Identifier]): Boolean = try {
    needs.foreach(assertHas(_))
    true
  } catch {
    case _ => false
  }
  
  def missingDependencies(needs:Seq[Identifier]):Seq[Identifier] = needs.filter( id=> !satisfies(Seq(id))  )
  def missingExpressions(needs:Seq[Expression]):Seq[Expression] = needs.filter( id=> !satisfies(Seq(id.identifier))  )

  def assertHas(name: String, typ: AnyType) {
    assertHas(Identifier(name, typ))
  }

  def contains(identifier: Identifier) = identifiers.contains(identifier)

  def assertHas(expected: Identifier) {
    identifiers.find(_.name == expected.name) match {
      case None => throwMissingKey(expected.name)
      case Some(existing) => if (!expected.typ.isAssignableFrom(existing.typ)) {
        throw new CypherTypeException("Expected `" + expected.name + "` to be a " + expected.typ + " but it was " + existing.typ)
      }
    }
  }

  def assertThat(id: Identifier): AnyType = actualIdentifier(id).typ

  def keys = identifiers.map(_.name)

  def throwMissingKey(key: String) {
    throw new SyntaxException("Unknown identifier `" + key + "`.")
  }

  def filter(keys: String*): SymbolTable = {

    keys.foreach(key => if (!identifiers.exists(_.name == key))
      throwMissingKey(key)
    )

    new SymbolTable(identifiers.filter(id => keys.contains(id.name)): _*)
  }

  private def get(key: String): Option[Identifier] = identifiers.find(_.name == key)

  def add(newIdentifiers: Identifier*): SymbolTable = {
    val matchedIdentifiers = newIdentifiers.map(newIdentifier => get(newIdentifier.name) match {
      case None => newIdentifier
      case Some(existingIdentifier) => handleMatched(newIdentifier, existingIdentifier)
    })


    val a = identifiers ++ matchedIdentifiers
    val b = a.toSet
    new SymbolTable(b.toSeq: _*)
  }

  def actualIdentifier(newIdentifier: Identifier): Identifier = get(newIdentifier.name) match {
    case None => newIdentifier
    case Some(existing) => handleMatched(newIdentifier, existing)
  }

  private def handleMatched(newIdentifier: Identifier, existingIdentifier: Identifier): Identifier = {
    newIdentifier match {
      case _ => {
        val a = existingIdentifier.typ
        val b = newIdentifier.typ
        if (b.isAssignableFrom(a)) {
          existingIdentifier
        } else {
          throw new SyntaxException("Identifier " + existingIdentifier + " already defined with different type " + newIdentifier)
        }
      }
    }
  }

  private def assertNoDuplicatesExist() {
    val names: Set[String] = identifiers.map(_.name).toSet

    if (names.size != identifiers.size) {
      names.foreach(n => if (identifiers.filter(_.name == n).size > 1) throw new SyntaxException("Identifier " + n + " defined multiple times"))
    }
  }
}