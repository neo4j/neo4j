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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.StatementHelper.RichStatement
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.intCollectionCollectionSymbol
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.intCollectionSymbol
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.intSymbol
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.nodeSymbol
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.pathCollectionSymbol
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.scope
import org.neo4j.cypher.internal.ast.semantics.ScopeTestHelper.typedSymbol
import org.neo4j.cypher.internal.frontend.phases.Namespacer
import org.neo4j.cypher.internal.parser.v5.ast.factory.Cypher5AstParser
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.symbols.StorableType
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

/**
 * ScopeTree is tested here because we want to be able to use the parser for the testing
 */
//noinspection ZeroIndexToHead
class ScopeTreeTest extends CypherFunSuite {
  /*
   * NOTE: when computing the scopeTree the normalization of return and with clauses has already taken place, so when
   * writing tests here please remember to always add aliases in return and with clauses.
   */

  test("match (n) return n as m => { { match (n) return n } { return n as m } }") {
    val ast = parse("match (n) return n as m")
    val scopeTree = ast.scope
    val nAt = ast.varAt("n") _
    val mAt = ast.varAt("m") _

    scopeTree should equal(scope()(
      scope(nodeSymbol("n", nAt(7), nAt(17)))(),
      scope(nodeSymbol("m", mAt(22)))()
    ))
  }

  test("match (a) with a as b return b as b => { { match (a) with a } { as b return b } { return b as b } }") {
    val ast = parse("match (a) with a as b return b as b")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _
    val bAt = ast.varAt("b") _

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", aAt(7), aAt(15)))(),
      scope(nodeSymbol("b", bAt(20), bAt(29)))(),
      scope(nodeSymbol("b", bAt(20), bAt(29), bAt(34)))()
    ))
  }

  test(
    "match (a) with a as a order by a.name limit 1 match a-->b return a as a => { { match (a) with a } { as a order by a.name limit 1 match a-->b return a } { return a as a } }"
  ) {
    val ast = parse("match (a) with a as a order by a.name limit 1 match (a)-->(b) return a as a")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _
    val bAt = ast.varAt("b") _

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", aAt(7), aAt(15)))(
        scope(nodeSymbol("a", aAt(7), aAt(15), aAt(20), aAt(31)))()
      ),
      scope(
        nodeSymbol("a", aAt(7), aAt(15), aAt(20), aAt(53), aAt(69)),
        nodeSymbol("b", bAt(59))
      )(),
      scope(nodeSymbol("a", aAt(7), aAt(15), aAt(20), aAt(53), aAt(69), aAt(74)))()
    ))
  }

  test(
    "match (a:Party) return a as a union match (a:Animal) return a as a => { { match (a:Party) return a } { } union { match (a:Animal) return a } { } }"
  ) {
    val ast = parse("match (a:Party) return a as a union match (a:Animal) return a as a")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _

    scopeTree should equal(scope(nodeSymbol("a", aAt(30), unionVariable = true))(
      scope()(
        scope(nodeSymbol("a", aAt(7), aAt(23)))(),
        scope(nodeSymbol("a", aAt(7), aAt(23), aAt(28)))()
      ),
      scope()(
        scope(nodeSymbol("a", aAt(43), aAt(60)))(),
        scope(nodeSymbol("a", aAt(43), aAt(60), aAt(65)))()
      )
    ))
  }

  test(
    "match (a) with a as a where a:Foo with a as a return a as a => { { match (a) with a } { as a where a:Foo with a } { as a return a } { return a as a } }"
  ) {
    val ast = parse("match (a) with a as a where a:Foo with a as a return a as a")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", aAt(7), aAt(15)))(
        scope(nodeSymbol("a", aAt(7), aAt(15), aAt(20), aAt(28)))()
      ),
      scope(nodeSymbol("a", aAt(7), aAt(15), aAt(20), aAt(39)))(),
      scope(nodeSymbol("a", aAt(7), aAt(15), aAt(20), aAt(39), aAt(44), aAt(53)))(),
      scope(nodeSymbol("a", aAt(7), aAt(15), aAt(20), aAt(39), aAt(44), aAt(53), aAt(58)))()
    ))
  }

  test(
    "match (a) with a as a optional match (b) with b as b return b as b => { { match (a) with a } { as a optional match (b) with b } { as b return b } { return b as b } }"
  ) {
    val ast = parse("match (a) with a as a optional match (b) with b as b return b as b")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _
    val bAt = ast.varAt("b") _

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", aAt(7), aAt(15)))(),
      scope(
        nodeSymbol("a", aAt(7), aAt(15), aAt(20)),
        nodeSymbol("b", bAt(38), bAt(46))
      )(),
      scope(nodeSymbol("b", bAt(38), bAt(46), bAt(51), bAt(60)))(),
      scope(nodeSymbol("b", bAt(38), bAt(46), bAt(51), bAt(60), bAt(65)))()
    ))
  }

  test("return [ a in [1, 2, 3] | a ] as r => { { return { [ a in [1, 2, 3] | a ] } } { as r } }") {
    val ast = parse("return [ a in [1, 2, 3] | a ] as r")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _
    val rAt = ast.varAt("r") _

    scopeTree should equal(scope()(
      scope()(
        scope(intSymbol("a", aAt(9), aAt(26)))()
      ),
      scope(intCollectionSymbol("r", rAt(33)))()
    ))
  }

  test(
    "with 1 as c return [ a in [1, 2, 3] | a + c ] as r => { { with 1 } { as c return { [ a in [1, 2, 3] | a + c ] } } { } }"
  ) {
    val ast = parse("with 1 as c return [ a in [1, 2, 3] | a + c ] as r")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _
    val cAt = ast.varAt("c") _
    val rAt = ast.varAt("r") _

    scopeTree should equal(scope()(
      scope()(),
      scope(
        intSymbol("c", cAt(10))
      )(
        scope(
          intSymbol("a", aAt(21), aAt(38)),
          intSymbol("c", cAt(10), cAt(42))
        )()
      ),
      scope(intCollectionSymbol("r", rAt(49)))()
    ))
  }

  test(
    "return [ a in [1, 2, 3] | [ b in [4, 5, 6] | a + b ] ] as r => { { return { [ a in [1, 2, 3] | { [ b in [4, 5, 6] | a + b ] } ] } } { }"
  ) {
    val ast = parse("return [ a in [1, 2, 3] | [ b in [4, 5, 6] | a + b ] ] as r")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _
    val bAt = ast.varAt("b") _
    val rAt = ast.varAt("r") _

    scopeTree should equal(scope()(
      scope()(
        scope(intSymbol("a", aAt(9)))(
          scope(
            intSymbol("a", aAt(9), aAt(45)),
            intSymbol("b", bAt(28), bAt(49))
          )()
        )
      ),
      scope(intCollectionCollectionSymbol("r", rAt(58)))()
    ))
  }

  test("match (a) where not a-->() return a as a => { { match (a) where not a-->() return a } { return a as a } }") {
    val ast = parse("match (a) where not (a)-->() return a as a")
    val scopeTree = ast.scope
    val aAt = ast.varAt("a") _

    scopeTree should equal(scope()(
      scope(nodeSymbol("a", aAt(7), aAt(36)))(
        scope(nodeSymbol("a", aAt(7), aAt(21)))()
      ),
      scope(nodeSymbol("a", aAt(7), aAt(36), aAt(41)))()
    ))
  }

  test(
    "MATCH (liker) WITH liker AS `liker`, (liker)-[]-() AS isNew WITH isNew as `isNew`, liker.time AS `freshId` ORDER BY `freshId` RETURN isNew as `isNew`"
  ) {
    val ast = parse(
      "MATCH (liker) WITH liker AS `liker`, (liker)-[]-() AS isNew WITH isNew as `isNew`, liker.time AS `freshId` ORDER BY `freshId` RETURN isNew as `isNew`"
    )
    val actual = ast.scope
    val lAt = ast.varAt("liker") _
    val iAt = ast.varAt("isNew") _
    val fAt = ast.varAt("freshId") _

    val expected = scope()(
      scope(nodeSymbol("liker", lAt(7), lAt(19)))(
        scope(nodeSymbol("liker", lAt(7), lAt(19), lAt(38)))()
      ),
      scope(
        pathCollectionSymbol("isNew", iAt(54), iAt(65)),
        nodeSymbol("liker", lAt(7), lAt(19), lAt(28), lAt(83))
      )(
        scope(
          typedSymbol("freshId", StorableType.storableType, fAt(97), fAt(116)),
          pathCollectionSymbol("isNew", iAt(54), iAt(65), iAt(74))
        )()
      ),
      scope(
        typedSymbol("freshId", StorableType.storableType, fAt(97)),
        pathCollectionSymbol("isNew", iAt(54), iAt(65), iAt(74), iAt(133))
      )(),
      scope(pathCollectionSymbol("isNew", iAt(54), iAt(74), iAt(65), iAt(142), iAt(133)))()
    )

    actual should equal(expected)
  }

  test(
    "match n, x with n as x match n, x return n as n, x as x => { { match n, x with n } { with n as x match n, x return n, x } { return n, x } }"
  ) {
    val ast = parse("match (n), (x) with n as x match (n), (x) return n as n, x as x")
    val scopeTree = ast.scope
    val nAt = ast.varAt("n") _
    val xAt = ast.varAt("x") _

    scopeTree should equal(scope()(
      scope(
        nodeSymbol("n", nAt(7), nAt(20)),
        nodeSymbol("x", xAt(12))
      )(),
      scope(
        nodeSymbol("n", nAt(34), nAt(49)),
        nodeSymbol("x", xAt(25), xAt(39), xAt(57))
      )(),
      scope(
        nodeSymbol("n", nAt(34), nAt(49), nAt(54)),
        nodeSymbol("x", xAt(25), xAt(39), xAt(57), xAt(62))
      )()
    ))
  }

  test(
    "with 1 as p, count(*) as rng return p as p order by rng ==> { {} { with 1 as p, count(*) as rng return p } { order by rng } }"
  ) {
    val ast = parse("with 1 as p, count(*) as rng return p as p order by rng")
    val pAt = ast.varAt("p") _
    val rAt = ast.varAt("rng") _

    val actual = ast.scope
    val expected = scope()(
      scope()(),
      scope(
        intSymbol("p", pAt(10), pAt(36)),
        intSymbol("rng", rAt(25))
      )(
        scope(
          intSymbol("p", pAt(10), pAt(36), pAt(41)),
          intSymbol("rng", rAt(25), rAt(52))
        )()
      ),
      scope(intSymbol("p", pAt(10), pAt(36), pAt(41)))()
    )

    actual should equal(expected)
  }

  test("Scope.toString should render nicely") {
    val ast = parse("match (n) return n as m")
    val scopeTree = ast.scope
    val nAt = ast.varAt("n") _
    val mAt = ast.varAt("m") _

    normalizeNewLines(scopeTree.toString) should equal(
      normalizeNewLines(
        s"""${scopeTree.toIdString} {
           |  ${scopeTree.children(0).toIdString} {
           |    n: 7(${Ref(nAt(7)).toIdString}) 17(${Ref(nAt(17)).toIdString})
           |  }
           |  ${scopeTree.children(1).toIdString} {
           |    m: 22(${Ref(mAt(22)).toIdString})
           |  }
           |}
           |""".stripMargin
      )
    )
  }

  def parse(queryText: String): Statement = {
    val statement = new Cypher5AstParser(queryText, OpenCypherExceptionFactory(None), None).singleStatement()
    // We have to project unions to materialize the UnionMappings so that we can find the Variables in them.
    statement.endoRewrite(Namespacer.projectUnions)
  }
}
