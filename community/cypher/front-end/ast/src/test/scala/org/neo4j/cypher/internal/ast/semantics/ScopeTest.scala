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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.semantics.Scope.DeclarationsAndDependencies
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.SymbolUses
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.intSymbol
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.nodeSymbol
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.scope
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.stringSymbol
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeZipper
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

//noinspection ZeroIndexToHead
class ScopeTest extends CypherFunSuite with AstConstructionTestSupport {

  private def symbolUses(name: String, uses: Int): SymbolUses = {
    val v = varFor(name)
    SymbolUses(SymbolUse(v), Seq.fill(uses)(v.copyId).map(SymbolUse(_)))
  }

  test("Should retrieve local symbol definitions") {
    val as = symbolUses("a", 1)
    val bs = symbolUses("b", 1)
    val given = scope(
      intSymbol("a", as),
      intSymbol("b", bs)
    )()

    given.symbolDefinitions should equal(Set(as.definition, bs.definition))
  }

  test("Should find all scopes") {
    val child11 = scope(
      stringSymbol("name", varFor("name"), varFor("name")),
      nodeSymbol("root", varFor("root"), varFor("root")),
      nodeSymbol("tag", varFor("tag")),
      nodeSymbol("book", varFor("book"), varFor("book"))
    )()
    val child1 = scope(
      nodeSymbol("root", varFor("root")),
      nodeSymbol("book", varFor("book"), varFor("book"))
    )(child11)
    val child2 = scope(
      nodeSymbol("book", varFor("book"), varFor("book"))
    )()
    val given = scope()(child1, child2)

    given.allScopes should equal(Seq(given, child1, child11, child2))
  }

  test("Should find all definitions") {
    val names = symbolUses("name", 1)
    val roots = symbolUses("root", 0)
    val tags = symbolUses("tag", 0)
    val books1 = symbolUses("book", 2)
    val books2 = symbolUses("book", 1)

    val child11 = scope(
      stringSymbol("name", names),
      nodeSymbol("root", roots),
      nodeSymbol("tag", tags.defVar),
      nodeSymbol("book", books1.defVar, books1.useVars(0))
    )()
    val child1 = scope(
      nodeSymbol("root", roots.defVar),
      nodeSymbol("book", books2)
    )(child11)
    val child2 = scope(
      nodeSymbol("book", books1.defVar, books1.useVars(1))
    )()
    val given = scope()(child1, child2)

    given.allSymbolDefinitions should equal(Map(
      "name" -> Set(names.definition),
      "root" -> Set(roots.definition),
      "tag" -> Set(tags.definition),
      "book" -> Set(books1.definition, books2.definition)
    ))
  }

  test("Should compute declarations and dependencies correctly for simple scope") {
    val names = symbolUses("name", 1)
    val roots = symbolUses("root", 2)
    val tags = symbolUses("tag", 1)
    val books1 = symbolUses("book", 1)
    val books2 = symbolUses("book", 1)

    val child = scope(
      stringSymbol("name", names), // new declaration
      nodeSymbol("root", roots), // same as in parent
      nodeSymbol("tag", tags), // new declaration
      nodeSymbol("book", books1) // new declaration
    )()
    val parent = scope(
      nodeSymbol("root", roots), // dependency brought in to child scope
      nodeSymbol("book", books2) // different location to book1 so not a dependency
    )(child)
    val childScopeLocation = parent.location.down.get

    val DeclarationsAndDependencies(declarations, dependencies) = childScopeLocation.declarationsAndDependencies

    declarations should equal(Set(names.definition, tags.definition, books1.definition))
    dependencies should equal(Set(roots.definition))
  }

  test("Should build variable map for simple scope tree") {
    val as = symbolUses("a", 1)
    val bs = symbolUses("b", 0)
    val given = scope(
      intSymbol("a", as),
      intSymbol("b", bs)
    )()

    val actual = given.variableDefinitions

    actual should equal(
      as.mapToDefinition ++
        bs.mapToDefinition
    )
  }

  test("Should build variable map for complex scope tree with shadowing") {
    val names = symbolUses("name", 1)
    val roots = symbolUses("root", 1)
    val tags = symbolUses("tag", 0)
    val books1 = symbolUses("book", 2)
    val books2 = symbolUses("book", 1)

    val given = scope()(
      scope(
        nodeSymbol("root", roots.defVar),
        nodeSymbol("book", books1.defVar, books1.useVars(0))
      )(
        scope(
          stringSymbol("name", names),
          nodeSymbol("root", roots),
          nodeSymbol("tag", tags.defVar),
          nodeSymbol("book", books1.defVar, books1.useVars(1))
        )()
      ),
      scope(
        nodeSymbol("book", books2)
      )()
    )

    val actual = given.allVariableDefinitions

    actual should equal(
      roots.mapToDefinition ++
        books1.mapToDefinition ++
        books2.mapToDefinition ++
        names.mapToDefinition ++
        tags.mapToDefinition
    )
  }
}
