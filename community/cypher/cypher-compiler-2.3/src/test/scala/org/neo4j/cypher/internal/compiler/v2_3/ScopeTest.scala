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
package org.neo4j.cypher.internal.compiler.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ScopeTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v2_3.helpers.ScopeTestHelper._

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

  test("Should build identifier map for simple scope tree") {
    val given = scope(nodeSymbol("a", 1, 2), nodeSymbol("b", 2))()

    val actual = given.identifierDefinitions

    actual should equal(Map(
      symUse("a", 1) -> symUse("a", 1),
      symUse("a", 2) -> symUse("a", 1),
      symUse("b", 2) -> symUse("b", 2)
    ))
  }

  test("Should build identifier map for complex scope tree with shadowing") {
    val given = scope()(
      scope(nodeSymbol("root", 6), nodeSymbol("book", 18, 111))(
        scope(stringSymbol("name", 31, 93), nodeSymbol("root", 6, 69), nodeSymbol("tag", 83), nodeSymbol("book", 18, 124))()
      ),
      scope(nodeSymbol("book", 200, 300))()
    )

    val actual = given.allIdentifierDefinitions

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
