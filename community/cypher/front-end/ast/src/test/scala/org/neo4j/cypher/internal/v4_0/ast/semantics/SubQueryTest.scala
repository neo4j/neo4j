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
import org.neo4j.cypher.internal.v4_0.expressions.NodePattern
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class SubQueryTest extends CypherFunSuite with AstConstructionTestSupport {

  test("returned variables are added to scope after sub-query") {

    val q =
      query(
        with_(literal(1).as("a")),
        subQuery(
          with_(literal(1).as("b")),
          return_(varFor("b").as("b"), literal(1).as("c"))
        ),
        return_(
          varFor("a").as("a"), varFor("b").as("b"), varFor("c").as("c"))
      )

    val result = SemanticChecker.check(q, SemanticState.clean)

    result.errors.size shouldEqual 0
  }

  test("outer scope is not seen in uncorrelated sub-query") {

    val q =
      query(
        with_(literal(1).as("a")),
        subQuery(
          return_(varFor("a").as("b"))
        ),
        return_(varFor("a").as("a"))
      )

    val result = SemanticChecker.check(q, SemanticState.clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should include ("Variable `a` not defined")
  }

  // This is subject to change when we implement correlated subqueries
  test("outer scope is not seen in subquery beginning with with") {

    val q =
      query(
        with_(literal(1).as("a")),
        subQuery(
          with_(varFor("a").as("b")),
          return_(varFor("b").as("c"))
        ),
        return_(varFor("a").as("a"))
      )

    val result = SemanticChecker.check(q, SemanticState.clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should include ("Variable `a` not defined")
  }

  test("subquery scoping works with order by") {

    val q =
      query(
        with_(literal(1).as("a")),
        subQuery(
          with_(literal(1).as("b")),
          return_(
            orderBy(varFor("b").asc, varFor("c").asc),
            varFor("b").as("b"), literal(1).as("c"))
        ),
        return_(
          orderBy(varFor("a").asc, varFor("b").asc, varFor("c").asc),
          varFor("a").as("a"), varFor("b").as("b"), varFor("b").as("c"))
      )

    val result = SemanticChecker.check(q, SemanticState.clean)

    result.errors.size shouldEqual 0
  }

  test("fails on variable name collision") {

    val sq = singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        return_(literal(2).as("x"))
      ),
      return_(literal(1).as("y"))
    )

    val result = sq.semanticCheck(SemanticState.clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should include ("Variable `x` already declared")
  }

  test("subquery allows union with valid return statements at the end") {

    val sq = singleQuery(
      subQuery(
        union(
          singleQuery(return_(literal(2).as("x"))),
          singleQuery(return_(literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"))
    )

    val result = sq.semanticCheck(SemanticState.clean)

    result.errors.size shouldEqual 0
  }

  test("subquery allows union with valid return columns in different order at the end") {

    val sq = singleQuery(
      subQuery(
        union(
          singleQuery(return_(literal(2).as("x"), literal(2).as("y"))),
          singleQuery(return_(literal(2).as("y"), literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"), varFor("y").as("y"))
    )

    val result = sq.semanticCheck(SemanticState.clean)

    result.errors.size shouldEqual 0
  }

  test("subquery allows union without return statement at the end") {

    val sq = singleQuery(
      subQuery(
        union(
          singleQuery(create(NodePattern(Some(varFor("a")), Seq.empty, None)(pos))),
          singleQuery(create(NodePattern(Some(varFor("a")), Seq.empty, None)(pos)))
        )
      ),
      return_(countStar().as("count"))
    )

    val result = sq.semanticCheck(SemanticState.clean)

    result.errors.size shouldEqual 0
  }

  test("subquery disallows union with different return columns at the end") {

    val sq = singleQuery(
      subQuery(
        union(
          singleQuery(return_(literal(2).as("x"), literal(2).as("y"), literal(2).as("z"))),
          singleQuery(return_(literal(2).as("y"), literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"), varFor("y").as("y"))
    )

    val result = sq.semanticCheck(SemanticState.clean)

    result.errors.size shouldEqual 1
    result.errors.head.msg should include ("All sub queries in an UNION must have the same column names")
  }

}
