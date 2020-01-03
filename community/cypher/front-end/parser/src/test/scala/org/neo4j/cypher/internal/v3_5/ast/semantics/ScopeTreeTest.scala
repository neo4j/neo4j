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

import org.neo4j.cypher.internal.v3_5.ast.StatementHelper._
import org.neo4j.cypher.internal.v3_5.ast.semantics.ScopeTestHelper._
import org.neo4j.cypher.internal.v3_5.parser.ParserFixture.parse
import org.neo4j.cypher.internal.v3_5.util.test_helpers.CypherFunSuite


/*
ScopeTree is tested here because we want to be able to use the parser for the testing
 */
class ScopeTreeTest extends CypherFunSuite {
  /*
   * NOTE: when computing the scopeTree the normalization of return and with clauses has already taken place, so when
   * writing tests here please remember to add always aliases in return and with clauses.
   */

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match (n)return n as m => { { match (n)return n } { return n as m } }") {
    val ast = parse("match (n) return n as m")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("n", 7, 17))(),
      scope(nodeSymbol("m", 22))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789N012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match (a) with a as b return b as b => { { match (a) with a } { as b return b } { return b as b } }") {
    val ast = parse("match (a) with a as b return b as b")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 7, 15))(),
      scope(nodeSymbol("b", 20, 29))(),
      scope(nodeSymbol("b", 20, 29, 34))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match (a) with a as a order by a.name limit 1 match a-->b return a as a => { { match (a) with a } { as a order by a.name limit 1 match a-->b return a } { return a as a } }") {
    val ast = parse("match (a) with a as a order by a.name limit 1 match (a)-->(b) return a as a")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 7, 15))(
        scope(nodeSymbol("a", 7, 15, 20, 31))()
      ),
      scope(
        nodeSymbol("a", 7, 15, 20, 53, 69),
        nodeSymbol("b", 59)
      )(),
      scope(nodeSymbol("a", 7, 15, 20, 53, 69, 74))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match (a:Party) return a as a union match (a:Animal) return a as a => { { match (a:Party) return a } { } union { match (a:Animal) return a } { } }") {
    val ast = parse("match (a:Party) return a as a union match (a:Animal) return a as a")
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
  test("match (a) with a as a where a:Foo with a as a return a as a => { { match (a) with a } { as a where a:Foo with a } { as a return a } { return a as a } }") {
    val ast = parse("match (a) with a as a where a:Foo with a as a return a as a")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 7, 15))(
        scope(nodeSymbol("a", 7, 15, 20, 28))()
      ),
      scope(nodeSymbol("a", 7, 15, 20, 39))(),
      scope(nodeSymbol("a", 7, 15, 20, 39, 44, 53))(),
      scope(nodeSymbol("a", 7, 15, 20, 39, 44, 53, 58))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match (a) with a as a optional match (b) with b as b return b as b => { { match (a) with a } { as a optional match (b) with b } { as b return b } { return b as b } }") {
    val ast = parse("match (a) with a as a optional match (b) with b as b return b as b")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 7, 15))(),
      scope(
        nodeSymbol("a", 7, 15, 20),
        nodeSymbol("b", 38, 46))(),
      scope(nodeSymbol("b", 38, 46, 51, 60))(),
      scope(nodeSymbol("b", 38, 46, 51, 60, 65))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("return [ a in [1, 2, 3] | a ] as r => { { return { [ a in [1, 2, 3] | a ] } } { as r } }") {
    val ast = parse("return [ a in [1, 2, 3] | a ] as r")
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
    val ast = parse("with 1 as c return [ a in [1, 2, 3] | a + c ] as r")
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
    val ast = parse("return [ a in [1, 2, 3] | [ b in [4, 5, 6] | a + b ] ] as r")
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
  test("match (a) where not a-->() return a as a => { { match (a) where not a-->() return a } { return a as a } }") {
    val ast = parse("match (a) where not (a)-->() return a as a")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", 7, 21, 36))(),
      scope(nodeSymbol("a", 7, 21, 36, 41))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("MATCH (liker) WITH liker AS `liker`, (liker)-[]-() AS isNew WITH isNew as `isNew`, liker.time AS `freshId` ORDER BY `freshId` RETURN isNew as `isNew`") {
    val ast = parse("MATCH (liker) WITH liker AS `liker`, (liker)-[]-() AS isNew WITH isNew as `isNew`, liker.time AS `freshId` ORDER BY `freshId` RETURN isNew as `isNew`")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("liker", 7, 19, 38))(),
      scope(pathCollectionSymbol("isNew", 54, 65), nodeSymbol("liker", 83, 19, 38, 7, 28))(
        scope(allSymbol("freshId", 97, 116), pathCollectionSymbol("isNew", 54, 65, 74))()
      ),
      scope(allSymbol("freshId", 97), pathCollectionSymbol("isNew", 54, 65, 74, 133))(),
      scope(pathCollectionSymbol("isNew", 54, 74, 65, 142, 133))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("match n, x with n as x match n, x return n as n, x as x => { { match n, x with n } { with n as x match n, x return n, x } { return n, x } }") {
    val ast = parse("match (n), (x) with n as x match (n), (x) return n as n, x as x")
    val scopeTree = ast.scope

    scopeTree should equal(scope()(
      scope(nodeSymbol("n", 7, 20), nodeSymbol("x", 12))(),
      scope(nodeSymbol("n", 34, 49), nodeSymbol("x", 25, 39, 57))(),
      scope(nodeSymbol("n", 34, 49, 54), nodeSymbol("x", 25, 39, 57, 62))()
    ))
  }

  //////00000000001111111111222222222233333333334444444444555555555566666666667777777777888888888899999999990000000000111111111122222222223333333333444444444455555555556666666666
  //////01234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789
  test("with 1 as p, count(*) as rng return p order by rng ==> { {} { with 1 as p, count(*) as rng return p } { order by rng } }") {
    val ast = parse("with 1 as p, count(*) as rng return p order by rng")

    val actual = ast.scope
    val expected = scope()(
      scope()(),
      scope(intSymbol("p", 10, 36), intSymbol("rng", 25))(
        scope(intSymbol("p", 10, 36, 37), intSymbol("rng", 25, 47))()
      ),
      scope(intSymbol("p", 10, 36, 37))()
    )

    actual should equal(expected)
  }
}

