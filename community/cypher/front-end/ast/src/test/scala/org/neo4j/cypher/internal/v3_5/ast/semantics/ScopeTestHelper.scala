/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v3_5.ast.semantics

import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.util.symbols.{CTInteger, CTList, CTNode, CTPath, CTString, TypeSpec}
import org.neo4j.cypher.internal.v3_5.ast.semantics

object ScopeTestHelper {

  def symUse(name: String, offset: Int) =
    SymbolUse(name, pos(offset))

  def scope(entries: semantics.Symbol*)(children: Scope*): Scope =
    Scope(entries.map { symbol => symbol.name -> symbol }.toMap, children.toSeq)

  def nodeSymbol(name: String, offsets: Int*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTNode), offsets: _*)

  def allSymbol(name: String, offsets: Int*): semantics.Symbol =
    typedSymbol(name, TypeSpec.all, offsets: _*)

  def intSymbol(name: String, offsets: Int*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTInteger), offsets: _*)

  def stringSymbol(name: String, offsets: Int*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTString), offsets: _*)

  def intCollectionSymbol(name: String, offsets: Int*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTList(CTInteger)), offsets: _*)

  def pathCollectionSymbol(name: String, offsets: Int*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTList(CTPath)), offsets: _*)

  def intCollectionCollectionSymbol(name: String, offsets: Int*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTList(CTList(CTInteger))), offsets: _*)

  def typedSymbol(name: String, typeSpec: TypeSpec, offsets: Int*) =
    semantics.Symbol(name, offsets.map(offset => pos(offset)).toSet, typeSpec)

  def pos(offset: Int): InputPosition = {
    new InputPosition(offset, 1, offset + 1)
  }
}
