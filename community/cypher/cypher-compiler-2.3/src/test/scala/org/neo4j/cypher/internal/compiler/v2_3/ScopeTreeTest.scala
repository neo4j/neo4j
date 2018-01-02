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
import org.neo4j.cypher.internal.frontend.v2_3.symbols.TypeSpec

class ScopeTreeTest extends CypherFunSuite {

  import org.neo4j.cypher.internal.compiler.v2_3.helpers.ScopeTestHelper._
  import org.neo4j.cypher.internal.compiler.v2_3.helpers.StatementHelper._
  import org.neo4j.cypher.internal.compiler.v2_3.parser.ParserFixture.parser

  /*
   * NOTE: when computing the scopeTree the normalization of return and with clauses has already taken place, so when
   * writing tests here please remember to add always aliases in return and with clauses.
   */

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match n return n as m => { { match n return n } { return n as m } }") {
    val ast = parser.parse("match n return n as m")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("n", 6, 15))(),
      scope(nodeSymbol("m", 20))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match a with a as b return b as b => { { match a with a } { as b return b } { return b as b } }") {
    val ast = parser.parse("match a with a as b return b as b")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(nodeSymbol("b", 18, 27))(),
      scope(nodeSymbol("b", 18, 27, 32))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match a with a as a order by a.name limit 1 match a-->b return a as a => { { match a with a } { as a order by a.name limit 1 match a-->b return a } { return a as a } }") {
    val ast = parser.parse("match a with a as a order by a.name limit 1 match a-->b return a as a")
    val scopeTree = ast.scope

    // TODO This looks suspicious; since we only use aliased items for identifierNamespacing, it should be ok though

    // Would rewrite to match a6 with a13 order by a13.name limit 1 match a13-->b49 return a13 as a63, which is wrong

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(
        nodeSymbol("a", 6, 13, 18, 29, 50, 63),
        nodeSymbol("b", 54)
      )(),
      scope(nodeSymbol("a", 6, 13, 18, 29, 50, 63, 68))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match (a:Party) return a as a union match (a:Animal) return a as a => { { match (a:Party) return a } { } union { match (a:Animal) return a } { } }") {
    val ast = parser.parse("match (a:Party) return a as a union match (a:Animal) return a as a")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope()(
        scope(nodeSymbol("a", 7, 23))(),
        scope(nodeSymbol("a", 7, 23, 28))()
      ),
      scope()(
        scope(nodeSymbol("a", 43, 60))(),
        scope(nodeSymbol("a", 43, 60, 65))()
      )
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match a with a as a where a:Foo with a as a return a as a => { { match a with a } { as a where a:Foo with a } { as a return a } { return a as a } }") {
    val ast = parser.parse("match a with a as a where a:Foo with a as a return a as a")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(nodeSymbol("a", 6, 13, 18, 26, 37))(),
      scope(nodeSymbol("a", 6, 13, 18, 26, 37, 42, 51))(),
      scope(nodeSymbol("a", 6, 13, 18, 26, 37, 42, 51, 56))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match a with a as a optional match b with b as b return b as b => { { match a with a } { as a optional match b with b } { as b return b } { return b as b } }") {
    val ast = parser.parse("match a with a as a optional match b with b as b return b as b")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 13))(),
      scope(
        nodeSymbol("a", 6, 13, 18),
        nodeSymbol("b", 35, 42))(),
      scope(nodeSymbol("b", 35, 42, 47, 56))(),
      scope(nodeSymbol("b", 35, 42, 47, 56, 61))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("return [ a in [1, 2, 3] | a ] as r => { { return { [ a in [1, 2, 3] | a ] } } { as r } }") {
    val ast = parser.parse("return [ a in [1, 2, 3] | a ] as r")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope()(
        scope(intSymbol("a", 9, 26))()
      ),
      scope(intCollectionSymbol("r", 33))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("with 1 as c return [ a in [1, 2, 3] | a + c ] as r => { { with 1 } { as c return { [ a in [1, 2, 3] | a + c ] } } { } }") {
    val ast = parser.parse("with 1 as c return [ a in [1, 2, 3] | a + c ] as r")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope()(),
      scope(
        intSymbol("c", 10))(
        scope(
          intSymbol("a", 21, 38),
          intSymbol("c", 10, 42)
        )()
      ),
      scope(intCollectionSymbol("r", 49))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("return [ a in [1, 2, 3] | [ b in [4, 5, 6] | a + b ] ] as r => { { return { [ a in [1, 2, 3] | { [ b in [4, 5, 6] | a + b ] } ] } } { }") {
    val ast = parser.parse("return [ a in [1, 2, 3] | [ b in [4, 5, 6] | a + b ] ] as r")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope()(
        scope(intSymbol("a", 9))(
          scope(
            intSymbol("a", 9, 45),
            intSymbol("b", 28, 49)
          )()
        )
      ),
      scope(intCollectionCollectionSymbol("r", 58))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match a where not a-->() return a as a => { { match a where not a-->() return a } { return a as a } }") {
    val ast = parser.parse("match a where not a-->() return a as a")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 6, 18, 32))(),
      scope(nodeSymbol("a", 6, 18, 32, 37))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("MATCH root CREATE book FOREACH(name in ['a','b','c'] | CREATE UNIQUE root-[:tag]->(tag {name:name})<-[:tagged]-book) RETURN book as book") {
    val ast = parser.parse("MATCH root CREATE book FOREACH(name in ['a','b','c'] | CREATE UNIQUE root-[:tag]->(tag {name:name})<-[:tagged]-book) RETURN book as book")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("root", 6), nodeSymbol("book", 18, 124))(
        scope(stringSymbol("name", 31, 93), nodeSymbol("root", 6, 69), nodeSymbol("tag", 83), nodeSymbol("book", 18, 111))()
      ),
      scope(nodeSymbol("book", 18, 124, 132))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("MATCH (liker) WITH liker AS `liker`, (liker)-[]-() AS isNew WITH isNew as `isNew`, liker.time AS `freshId` ORDER BY `freshId` RETURN isNew as `isNew`") {
    val ast = parser.parse("MATCH (liker) WITH liker AS `liker`, (liker)-[]-() AS isNew WITH isNew as `isNew`, liker.time AS `freshId` ORDER BY `freshId` RETURN isNew as `isNew`")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("liker", 7, 19, 38))(),
      scope(pathCollectionSymbol("isNew", 54, 65), nodeSymbol("liker", 83, 19, 38, 7, 28))(),
      scope(allSymbol("freshId", 97, 116), pathCollectionSymbol("isNew", 54, 65, 74, 133))(),
      scope(pathCollectionSymbol("isNew", 54, 74, 65, 142, 133))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match n, x with n as x match n, x return n as n, x as x => { { match n, x with n } { with n as x match n, x return n, x } { return n, x } }") {
    val ast = parser.parse("match n, x with n as x match n, x return n as n, x as x")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("n", 6, 16), nodeSymbol("x", 9))(),
      scope(nodeSymbol("n", 29, 41), nodeSymbol("x", 21, 32, 49))(),
      scope(nodeSymbol("n", 29, 41, 46), nodeSymbol("x", 21, 32, 49, 54))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("with 1 as p, count(*) as rng return p order by rng ==> { {} { with 1 as p, count(*) as rng return p } { order by rng } }") {
    val ast = parser.parse("with 1 as p, count(*) as rng return p order by rng")

    val actual = ast.scope
    val expected = scope()(
      scope()(),
      scope(intSymbol("p", 10, 36), intSymbol("rng", 25))(),
      scope(intSymbol("p", 10, 36, 37), typedSymbol("rng", TypeSpec.all, 25, 47))()
    )

    actual should equal(expected)
  }
}

