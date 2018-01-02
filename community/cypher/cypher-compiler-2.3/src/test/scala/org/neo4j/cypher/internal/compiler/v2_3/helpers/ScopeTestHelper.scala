/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, Scope, Symbol, SymbolUse}

object ScopeTestHelper {

  def symUse(name: String, offset: Int) =
    SymbolUse(name, pos(offset))

  def scope(entries: Symbol*)(children: Scope*): Scope =
    Scope(entries.map { symbol => symbol.name -> symbol }.toMap, children.toSeq)

  def nodeSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTNode), offsets: _*)

  def allSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.all, offsets: _*)

  def intSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTInteger), offsets: _*)

  def stringSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTString), offsets: _*)

  def intCollectionSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTCollection(CTInteger)), offsets: _*)

  def pathCollectionSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTCollection(CTPath)), offsets: _*)

  def intCollectionCollectionSymbol(name: String, offsets: Int*): Symbol =
    typedSymbol(name, TypeSpec.exact(CTCollection(CTCollection(CTInteger))), offsets: _*)

  def typedSymbol(name: String, typeSpec: TypeSpec, offsets: Int*) =
    Symbol(name, offsets.map(offset => pos(offset)).toSet, typeSpec)

  def pos(offset: Int): InputPosition = {
    new InputPosition(offset, 1, offset + 1)
  }
}
