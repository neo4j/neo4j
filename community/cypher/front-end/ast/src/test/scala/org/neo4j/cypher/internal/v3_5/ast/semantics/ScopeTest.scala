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

import org.neo4j.cypher.internal.v3_5.ast.semantics.ScopeTestHelper._
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite

class ScopeTest extends CypherFunSuite {

  test("Should retrieve local symbol definitions") {
    val given = scope(
      intSymbol("a", 9, 45),
      intSymbol("b", 28, 49)
    )()

    given.symbolDefinitions should equal(Set(symUse("a", 9), symUse("b", 28)))
  }

  test("Should find all scopes") {
    val child11 = scope(stringSymbol("name", 31, 93), nodeSymbol("root", 6, 69), nodeSymbol("tag", 83), nodeSymbol("book", 18, 111))()
    val child1 = scope(nodeSymbol("root", 6), nodeSymbol("book", 18, 124))(child11)
    val child2 = scope(nodeSymbol("book", 18, 124))()
    val given = scope()(child1, child2)

    given.allScopes should equal(Seq(given, child1, child11, child2))
  }

  test("Should find all definitions") {
    val child11 = scope(stringSymbol("name", 31, 93), nodeSymbol("root", 6, 69), nodeSymbol("tag", 83), nodeSymbol("book", 18, 111))()
    val child1 = scope(nodeSymbol("root", 6), nodeSymbol("book", 24, 124))(child11)
    val child2 = scope(nodeSymbol("book", 18, 124))()
    val given = scope()(child1, child2)

    given.allSymbolDefinitions should equal(Map(
      "name" -> Set(symUse("name", 31)),
      "root" -> Set(symUse("root", 6)),
      "tag" -> Set(symUse("tag", 83)),
      "book" -> Set(symUse("book", 18), symUse("book", 24))
    ))
  }

  test("Should build variable map for simple scope tree") {
    val given = scope(nodeSymbol("a", 1, 2), nodeSymbol("b", 2))()

    val actual = given.variableDefinitions

    actual should equal(Map(
      symUse("a", 1) -> symUse("a", 1),
      symUse("a", 2) -> symUse("a", 1),
      symUse("b", 2) -> symUse("b", 2)
    ))
  }

  test("Should build variable map for complex scope tree with shadowing") {
    val given = scope()(
      scope(nodeSymbol("root", 6), nodeSymbol("book", 18, 111))(
        scope(stringSymbol("name", 31, 93), nodeSymbol("root", 6, 69), nodeSymbol("tag", 83), nodeSymbol("book", 18, 124))()
      ),
      scope(nodeSymbol("book", 200, 300))()
    )

    val actual = given.allVariableDefinitions

    actual should equal(Map(
      symUse("root", 6) -> symUse("root", 6),
      symUse("root", 69) -> symUse("root", 6),
      symUse("book", 18) -> symUse("book", 18),
      symUse("book", 111) -> symUse("book", 18),
      symUse("book", 124) -> symUse("book", 18),
      symUse("book", 200) -> symUse("book", 200),
      symUse("book", 300) -> symUse("book", 200),
      symUse("name", 31) -> symUse("name", 31),
      symUse("name", 93) -> symUse("name", 31),
      symUse("tag", 83) -> symUse("tag", 83)
    ))
  }
}


