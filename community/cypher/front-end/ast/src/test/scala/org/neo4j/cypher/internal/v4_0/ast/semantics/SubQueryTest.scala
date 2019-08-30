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

  private val clean =
    SemanticState.clean
      .withFeature(SemanticFeature.CorrelatedSubQueries)

  test("correlated subqueries require semantic feature") {

    val q =
      query(
        with_(literal(1).as("a")),
        subQuery(
          with_(varFor("a").aliased),
          return_(literal(1).as("b"))
        ),
        return_(varFor("a").as("a"))
      )
        .semanticCheck(SemanticState.clean)
        .tap(_.errors.size.shouldEqual(1))
        .tap(_.errors.head.msg.should(include("Importing") and include("correlated subqueries")))
  }

  test("returned variables are added to scope after sub-query") {

    query(
      with_(literal(1).as("a")),
      subQuery(
        with_(literal(1).as("b")),
        return_(varFor("b").as("b"), literal(1).as("c"))
      ),
      return_(
        varFor("a").as("a"), varFor("b").as("b"), varFor("c").as("c"))
    )
      .semanticCheck(clean)
      .tap(_.errors.foreach(println))
      .errors.size.shouldEqual(0)

  }

  test("outer scope is not seen in uncorrelated sub-query") {

    query(
      with_(literal(1).as("a")),
      subQuery(
        return_(varFor("a").as("b"))
      ),
      return_(varFor("a").as("a"))
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("Variable `a` not defined")))
  }

  test("subquery scoping works with order by") {

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
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery can't return variable that already exists outside") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        return_(literal(2).as("x"))
      ),
      return_(literal(1).as("y"))
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("Variable `x` already declared")))

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

  test("subquery with union") {

    query(
      with_(literal(1).as("a")),
      subQuery(unionDistinct(
        singleQuery(
          return_(literal(2).as("b"))
        ),
        singleQuery(
          return_(literal(3).as("b"))
        )
      )),
      return_(varFor("a").aliased, varFor("b").aliased)
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery with invalid union") {

    query(
      with_(literal(1).as("a")),
      subQuery(unionDistinct(
        singleQuery(
          return_(literal(2).as("b"))
        ),
        singleQuery(
          return_(literal(3).as("c"))
        )
      )),
      return_(varFor("a").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("All sub queries in an UNION must have the same column names")))
  }

  test("correlated subquery importing variables using leading WITH") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        with_(varFor("x").aliased),
        return_(varFor("x").as("y"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("correlated subquery with union") {

    query(
      with_(literal(1).as("a")),
      subQuery(unionDistinct(
        singleQuery(
          with_(varFor("a").aliased),
          return_(varFor("a").as("b"))
        ),
        singleQuery(
          with_(varFor("a").aliased),
          return_(varFor("a").as("b"))
        )
      )),
      return_(varFor("a").aliased, varFor("b").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(0))
  }

  test("correlated subquery with union with different imports") {

    query(
      with_(literal(1).as("a"), literal(1).as("b"), literal(1).as("c")),
      subQuery(unionDistinct(
        singleQuery(
          with_(varFor("a").aliased),
          return_(varFor("a").as("x"))
        ),
        singleQuery(
          with_(varFor("b").aliased),
          return_(varFor("b").as("x"))
        ),
        singleQuery(
          with_(varFor("c").aliased),
          return_(varFor("c").as("x"))
        )
      )),
      return_(varFor("a").aliased, varFor("b").aliased, varFor("c").aliased, varFor("x").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(0))
  }

  test("importing WITH must be first clause") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        unwind(listOf(literal(1)), varFor("a")),
        with_(varFor("x").aliased),
        return_(varFor("x").as("y"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("Variable `x` not defined")))
  }

  test("importing WITH must be clean pass-through") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        with_(varFor("x").aliased, literal(2).as("y")),
        return_(varFor("x").as("z"))
      ),
      return_(varFor("x").aliased, varFor("z").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("Variable `x` not defined")))
  }

  test("importing variables using pass-through WITH and then introducing more") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        with_(varFor("x").aliased),
        with_(varFor("x").aliased, literal(2).as("y")),
        return_(varFor("x").as("z"), varFor("y").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("nested uncorrelated subquery") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        with_(literal(1).as("y")),
        subQuery(
          with_(literal(1).as("z")),
          return_(varFor("z").aliased)
        ),
        return_(varFor("y").aliased, varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("nested uncorrelated subquery can't access any outer scopes") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        with_(literal(1).as("y")),
        subQuery(
          with_(literal(1).as("z")),
          with_(varFor("x").as("a"), varFor("y").as("b"), varFor("z").aliased),
          return_(varFor("z").aliased)
        ),
        return_(varFor("y").aliased, varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(2))
      .tap(_.errors(0).msg.should(include("Variable `x` not defined")))
      .tap(_.errors(1).msg.should(include("Variable `y` not defined")))
  }

  test("nested correlated subquery") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        with_(varFor("x").aliased),
        with_(varFor("x").as("y")),
        subQuery(
          with_(varFor("y").aliased),
          with_(varFor("y").as("z")),
          return_(varFor("z").aliased)
        ),
        return_(varFor("y").aliased, varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery inside correlated subquery can't import from outer scope") {

    singleQuery(
      with_(literal(1).as("x"), literal(2).as("y")),
      subQuery(
        with_(varFor("x").aliased),
        subQuery(
          with_(varFor("y").aliased),
          return_(literal(3).as("z"))
        ),
        return_(varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors(0).msg.should(include("Variable `y` not defined")))
  }

  test("subquery ending in update exports nothing") {

    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        with_(literal(2).as("y")),
        create(nodePat("n"))
      ),
      return_(varFor("x").aliased, varFor("n").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(2))
      .tap(_.errors(0).msg.should(include("Variable `n` not defined")))
      .tap(_.errors(1).msg.should(include("Variable `y` not defined")))
  }

  test("extracting imported variables from leading WITH") {

    singleQuery(
      return_(varFor("x").as("y"))
    ).importColumns
      .shouldEqual(Seq())

    singleQuery(
      with_(literal(1).as("x")),
      with_(varFor("x").aliased),
      return_(varFor("x").as("y"))
    ).importColumns
      .shouldEqual(Seq())

    singleQuery(
      with_(literal(1).as("x"), varFor("x").aliased),
      return_(varFor("x").as("y"))
    ).importColumns
      .shouldEqual(Seq())

    singleQuery(
      with_(varFor("x").aliased),
      return_(varFor("x").as("y"))
    ).importColumns
      .shouldEqual(Seq("x"))

    singleQuery(
      from(varFor("foo")),
      with_(varFor("x").aliased),
      return_(varFor("x").as("y"))
    ).importColumns
      .shouldEqual(Seq())
  }

  implicit class AnyOps[A](a: A) {
    def tap[X](e: A => X): A = {
      e(a)
      a
    }
  }

}
