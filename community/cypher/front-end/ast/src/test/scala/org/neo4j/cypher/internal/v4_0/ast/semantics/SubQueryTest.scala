/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.ast.semantics

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SubQueryTest extends CypherFunSuite with AstConstructionHelp {

  private val clean = SemanticState.clean.withFeature(SemanticFeature.SubQueries)

  test("subqueries require semantic feature") {

    val q =
      query(
        with_(i("1") -> v("a")),
        subQuery(
          return_(i("1") -> v("b"))
        ),
        return_(v("a"))
      )

    val result = SemanticChecker.check(q, SemanticState.clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should (include("CALL") and include("subqueries"))
  }

  test("returned variables are added to scope after sub-query") {

    val q =
      query(
        with_(i("1") -> v("a")),
        subQuery(
          with_(i("1") -> v("b")),
          return_(
            v("b"), i("1") -> v("c"))
        ),
        return_(
          v("a"), v("b"), v("c"))
      )

    val result = SemanticChecker.check(q, clean)

    result.errors.size shouldEqual 0
  }

  test("outer scope is not seen in uncorrelated sub-query") {

    val marker = InputPosition(-100, -100, -100)

    val q =
      query(
        with_(i("1") -> v("a")),
        subQuery(
          return_(v("a")(marker) -> v("b"))
        ),
        return_(v("a"))
      )

    val result = SemanticChecker.check(q, clean)

    result.errors.size shouldEqual 1
    result.errors.head.position shouldEqual marker
  }

  test("subquery scoping works with order by") {

    val q =
      query(
        with_(i("1") -> v("a")),
        subQuery(
          with_(i("1") -> v("b")),
          return_(
            orderBy(v("b"), v("c")),
            v("b"), i("1") -> v("c"))
        ),
        return_(
          orderBy(v("a"), v("b"), v("c")),
          v("a"), v("b"), v("b") -> v("c"))
      )

    val result = SemanticChecker.check(q, clean)

    result.errors.size shouldEqual 0
  }

  test("fails on variable name collision") {

    val marker = InputPosition(-100, -100, -100)

    val sq = singleQuery(
      with_(i("1") -> v("x")),
      subQuery(
        return_(i("2") -> v("x")(marker))
      ),
      return_(i("1") -> v("y"))
    )

    val result = sq.semanticCheck(clean)

    result.errors.size shouldEqual 1
    result.errors.head.position shouldEqual marker
    result.errors.head.msg should include ("Variable `x` already declared")
  }

}
