/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.semantics
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.TypeSpec

object ScopeTestHelper {

  case class SymbolUses(definition: SymbolUse, uses: Seq[SymbolUse]) {
    def defVar: LogicalVariable = definition.asVariable

    def useVars: Seq[LogicalVariable] = uses.map(_.asVariable)

    def mapToDefinition: Map[SymbolUse, SymbolUse] =
      (uses :+ definition).map(_ -> definition).toMap
  }

  def scope(entries: semantics.Symbol*)(children: Scope*): Scope =
    Scope(entries.map { symbol => symbol.name -> symbol }.toMap, children.toSeq)

  def nodeSymbol(name: String, definition: LogicalVariable, uses: LogicalVariable*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTNode), definition, uses: _*)

  def nodeSymbol(name: String, definition: LogicalVariable, unionVariable: Boolean): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTNode), definition, unionVariable)

  def nodeSymbol(name: String, symbolUses: SymbolUses): semantics.Symbol =
    nodeSymbol(name, symbolUses.defVar, symbolUses.useVars: _*)

  def allSymbol(name: String, definition: LogicalVariable, uses: LogicalVariable*): semantics.Symbol =
    typedSymbol(name, TypeSpec.all, definition, uses: _*)

  def allSymbol(name: String, symbolUses: SymbolUses): semantics.Symbol =
    allSymbol(name, symbolUses.defVar, symbolUses.useVars: _*)

  def intSymbol(name: String, definition: LogicalVariable, uses: LogicalVariable*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTInteger), definition, uses: _*)

  def intSymbol(name: String, symbolUses: SymbolUses): semantics.Symbol =
    intSymbol(name, symbolUses.defVar, symbolUses.useVars: _*)

  def stringSymbol(name: String, definition: LogicalVariable, uses: LogicalVariable*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTString), definition, uses: _*)

  def stringSymbol(name: String, symbolUses: SymbolUses): semantics.Symbol =
    stringSymbol(name, symbolUses.defVar, symbolUses.useVars: _*)

  def intCollectionSymbol(name: String, definition: LogicalVariable, uses: LogicalVariable*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTList(CTInteger)), definition, uses: _*)

  def intCollectionSymbol(name: String, symbolUses: SymbolUses): semantics.Symbol =
    intCollectionSymbol(name, symbolUses.defVar, symbolUses.useVars: _*)

  def pathCollectionSymbol(name: String, definition: LogicalVariable, uses: LogicalVariable*): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTList(CTPath)), definition, uses: _*)

  def pathCollectionSymbol(name: String, symbolUses: SymbolUses): semantics.Symbol =
    pathCollectionSymbol(name, symbolUses.defVar, symbolUses.useVars: _*)

  def intCollectionCollectionSymbol(
    name: String,
    definition: LogicalVariable,
    uses: LogicalVariable*
  ): semantics.Symbol =
    typedSymbol(name, TypeSpec.exact(CTList(CTList(CTInteger))), definition, uses: _*)

  def intCollectionCollectionSymbol(name: String, symbolUses: SymbolUses): semantics.Symbol =
    intCollectionCollectionSymbol(name, symbolUses.defVar, symbolUses.useVars: _*)

  def typedSymbol(name: String, typeSpec: TypeSpec, definition: LogicalVariable, uses: LogicalVariable*): Symbol =
    semantics.Symbol(name, typeSpec, SymbolUse(definition), uses.map(SymbolUse(_)).toSet)

  def typedSymbol(name: String, typeSpec: TypeSpec, definition: LogicalVariable, unionVariable: Boolean): Symbol =
    semantics.Symbol(name, typeSpec, SymbolUse(definition), Set.empty, unionVariable)

  def typedSymbol(name: String, typeSpec: TypeSpec, symbolUses: SymbolUses): Symbol =
    typedSymbol(name, typeSpec, symbolUses.defVar, symbolUses.useVars: _*)
}
