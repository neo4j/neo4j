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
package org.neo4j.cypher.internal.v4_0.ast.semantics

import org.neo4j.cypher.internal.v4_0.ast._
import org.neo4j.cypher.internal.v4_0.expressions.NodePattern
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

//noinspection ZeroIndexToHead
class SubQueryTest extends CypherFunSuite with AstConstructionTestSupport {

  private val clean =
    SemanticState.clean
      .withFeature(SemanticFeature.CorrelatedSubQueries)
      .withFeature(SemanticFeature.MultipleGraphs)
      .withFeature(SemanticFeature.FromGraphSelector)
      .withFeature(SemanticFeature.UseGraphSelector)
      .withFeature(SemanticFeature.ExpressionsInViewInvocations)

  test("returned variables are added to scope after subquery") {
    // WITH 1 AS a
    // CALL {
    //   WITH 1 AS b
    //   RETURN b, 1 AS c
    // }
    // RETURN a, b, c
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
      .errors.size.shouldEqual(0)

  }

  test("outer scope is not seen in uncorrelated subquery") {
    // WITH 1 AS a
    // CALL {
    //   RETURN a AS b
    // }
    // RETURN a
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
    // WITH 1 AS a
    // CALL {
    //   WITH 1 AS b
    //   RETURN b, 1 AS c ORDER BY b, c
    // }
    // RETURN a, b, b AS c ORDER BY a, b, c
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
    // WITH 1 AS x
    // CALL {
    //   RETURN 2 AS x
    // }
    // RETURN 1 AS y
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
    // CALL {
    //   RETURN 2 AS x
    //     UNION
    //   RETURN 2 AS x
    // }
    // RETURN x
    singleQuery(
      subQuery(
        union(
          singleQuery(return_(literal(2).as("x"))),
          singleQuery(return_(literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"))
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery allows union with valid return columns in different order at the end") {
    // CALL {
    //   RETURN 2 AS x, 2 AS y
    //     UNION
    //   RETURN 2 AS y, 2 AS x
    // }
    // RETURN x, y
    singleQuery(
      subQuery(
        union(
          singleQuery(return_(literal(2).as("x"), literal(2).as("y"))),
          singleQuery(return_(literal(2).as("y"), literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"), varFor("y").as("y"))
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery does not allow union without return statements at the end") {
    // CALL {
    //   CREATE (a)
    //     UNION
    //   CREATE (a)
    // }
    // RETURN count(*) AS count
    singleQuery(
      subQuery(
        union(
          singleQuery(create(NodePattern(Some(varFor("a")), Seq.empty, None)(pos))),
          singleQuery(create(NodePattern(Some(varFor("a")), Seq.empty, None)(pos)))
        )
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(2))
      .tap(_.errors(0).msg.should(include("CALL subquery cannot conclude with CREATE (must be RETURN)")))
      .tap(_.errors(1).msg.should(include("CALL subquery cannot conclude with CREATE (must be RETURN)")))
  }

  test("subquery allows union with create and return statement at the end") {
    // CALL {
    //   CREATE (a) RETURN a
    //     UNION
    //   CREATE (a) RETURN a
    // }
    // RETURN count(*) AS count
    singleQuery(
      subQuery(
        union(
          singleQuery(create(NodePattern(Some(varFor("a")), Seq.empty, None)(pos)), return_(varFor("a").as("a"))),
          singleQuery(create(NodePattern(Some(varFor("a")), Seq.empty, None)(pos)), return_(varFor("a").as("a")))
        )
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery does not allow single query without return statement at the end") {
    // CALL {
    //   MERGE (a)
    // }
    // RETURN count(*) AS count
    singleQuery(
      subQuery(
        singleQuery(merge(NodePattern(Some(varFor("a")), Seq.empty, None)(pos)))
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("CALL subquery cannot conclude with MERGE (must be RETURN)")))
  }

  test("subquery allows single query with create and return statement at the end") {
    // CALL {
    //   MERGE (a) RETURN a
    // }
    // RETURN count(*) AS count
    singleQuery(
      subQuery(
        singleQuery(merge(NodePattern(Some(varFor("a")), Seq.empty, None)(pos)), return_(varFor("a").as("a")))
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery disallows union with different return columns at the end") {
    // CALL {
    //   RETURN 2 AS x, 2 AS y, 2 AS z
    //     UNION
    //   RETURN 2 AS y, 2 AS x
    // }
    // RETURN x, y
    singleQuery(
      subQuery(
        union(
          singleQuery(return_(literal(2).as("x"), literal(2).as("y"), literal(2).as("z"))),
          singleQuery(return_(literal(2).as("y"), literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"), varFor("y").as("y"))
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("All sub queries in an UNION must have the same column names")))
  }

  test("correlated subquery importing variables using leading WITH") {
    // WITH 1 AS x
    // CALL {
    //   WITH x
    //   RETURN x AS y
    // }
    // RETURN x, y
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

  test("correlated subquery may only reference imported variables") {
    // WITH 1 AS x, 1 AS y
    // CALL {
    //   WITH x
    //   RETURN y AS z
    // }
    // RETURN x
    singleQuery(
      with_(literal(1).as("x"), literal(1).as("y")),
      subQuery(
        with_(varFor("x").aliased),
        return_(varFor("y").as("z"))
      ),
      return_(varFor("x").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("Variable `y` not defined")))
  }

  test("correlated subqueries require semantic feature") {
    // WITH 1 AS a
    // CALL {
    //   WITH a
    //   RETURN 1 AS b
    // }
    // RETURN a
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

  test("correlated subquery with union") {
    // WITH 1 AS a
    // CALL {
    //   WITH a
    //   RETURN a AS b
    //     UNION
    //   WITH a
    //   RETURN a AS b
    // }
    // RETURN a, b
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
    // WITH 1 AS a, 1 AS b, 1 AS c
    // CALL {
    //   WITH a
    //   RETURN a AS x
    //     UNION
    //   WITH b
    //   RETURN b AS x
    //     UNION
    //   WITH c
    //   RETURN c AS x
    // }
    // RETURN a, b, c, x
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

  test("union without subquery") {
    //   WITH a
    //   RETURN a AS x
    //     UNION
    //   WITH b
    //   RETURN b AS x
    //     UNION
    //   WITH c
    //   RETURN c AS x
    query(
      unionDistinct(
        singleQuery(
          with_(literal(1).as("a")),
          return_(varFor("a").as("x"))
        ),
        singleQuery(
          with_(literal(1).as("b")),
          return_(varFor("b").as("x"))
        ),
        singleQuery(
          with_(literal(1).as("c")),
          return_(varFor("c").as("x"))
        )
      )
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(0))
  }

  test("importing WITH without FROM must be first clause") {
    // WITH 1 AS x
    // CALL {
    //   UNWIND [1] AS a
    //   WITH x
    //   RETURN x AS y
    // }
    // RETURN x, y
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

  test("importing WITH may appear after FROM") {
    // WITH 1 AS x
    // CALL {
    //   FROM g
    //   WITH x
    //   RETURN x AS y
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        from(varFor("g")),
        with_(varFor("x").aliased),
        return_(varFor("x").as("y"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(0))
  }

  test("importing WITH may appear after USE") {
    // WITH 1 AS x
    // CALL {
    //   USE g
    //   WITH x
    //   RETURN x AS y
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x")),
      subQuery(
        use(varFor("g")),
        with_(varFor("x").aliased),
        return_(varFor("x").as("y"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(0))
  }

  test("importing WITH must be clean pass-through") {
    // WITH 1 AS x
    // CALL {
    //   WITH x, 2 AS y
    //   RETURN x AS z
    // }
    // RETURN x, z
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
    // WITH 1 AS x
    // CALL {
    //   WITH x
    //   WITH x, 2 AS y
    //   RETURN x AS z, y
    // }
    // RETURN x, y
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

  test("subquery leading FROM may reference outer variables") {
    // WITH 1 AS x, 2 AS y
    // CALL {
    //   FROM g(x, y)
    //   WITH x
    //   RETURN 1 AS z
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x"), literal(2).as("y")),
      subQuery(
        from(function("g", varFor("x"), varFor("y"))),
        with_(varFor("x").aliased),
        return_(literal(1).as("z"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(0))
  }

  test("subquery leading USE may reference outer variables") {
    // WITH 1 AS x, 2 AS y
    // CALL {
    //   USE g(x, y)
    //   WITH x
    //   RETURN 1 AS z
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x"), literal(2).as("y")),
      subQuery(
        use(function("g", varFor("x"), varFor("y"))),
        with_(varFor("x").aliased),
        return_(literal(1).as("z"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(0))
  }

  test("subquery FROM after imports may only reference imported variables") {
    // WITH 1 AS x, 2 AS y
    // CALL {
    //   WITH x
    //   FROM g(x, y)
    //   RETURN 3 AS z
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x"), literal(2).as("y")),
      subQuery(
        with_(varFor("x").aliased),
        from(function("g", varFor("x"), varFor("y"))),
        return_(literal(3).as("z"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("Variable `y` not defined")))
  }

  test("subquery USE after imports may only reference imported variables") {
    // WITH 1 AS x, 2 AS y
    // CALL {
    //   WITH x
    //   USE g(x, y)
    //   RETURN 3 AS z
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x"), literal(2).as("y")),
      subQuery(
        with_(varFor("x").aliased),
        use(function("g", varFor("x"), varFor("y"))),
        return_(literal(3).as("z"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors.head.msg.should(include("Variable `y` not defined")))
  }

  test("nested uncorrelated subquery") {
    // WITH 1 AS x
    // CALL {
    //   WITH 1 AS y
    //   CALL {
    //     WITH 1 AS z
    //     RETURN z
    //   }
    //   RETURN y, z
    // }
    // RETURN x, y, z
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
    // WITH 1 AS x
    // CALL {
    //   WITH 1 AS y
    //   CALL {
    //     WITH 1 AS z
    //     WITH x AS a, y AS b, z
    //     RETURN z
    //   }
    //   RETURN y, z
    // }
    // RETURN x, y, z
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
    // WITH 1 AS x
    // CALL {
    //   WITH x
    //   WITH x AS y
    //   CALL {
    //     WITH y
    //     WITH y AS z
    //     RETURN z
    //   }
    //   RETURN y, z
    // }
    // RETURN x, y, z
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
    // WITH 1 AS x, 2 AS y
    // CALL {
    //   WITH x
    //   CALL {
    //     WITH y
    //     WITH y AS z
    //     RETURN 3 AS z
    //   }
    //   RETURN z
    // }
    // RETURN x, y, z
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
      .shouldEqual(Seq("x"))

    singleQuery(
      use(varFor("foo")),
      with_(varFor("x").aliased),
      return_(varFor("x").as("y"))
    ).importColumns
      .shouldEqual(Seq("x"))
  }

  test("subquery RETURN should not allow un-aliased general expressions") {
    // CALL {
    //   WITH {title: 1} AS m
    //   RETURN m.title
    // }
    // RETURN 1 AS x
    singleQuery(
      subQuery(
        with_(mapOfInt("title" -> 1).as("m")),
        return_(UnaliasedReturnItem(prop(varFor("m"), "title"), "m.title")(pos))
      ),
      return_(literalInt(1).as("x"))
    )
      .semanticCheck(clean)
      .tap(_.errors.size.shouldEqual(1))
      .tap(_.errors(0).msg.should(include("Expression in CALL { RETURN ... } must be aliased (use AS)")))
  }

  /** https://github.com/scala/scala/blob/v2.13.0/src/library/scala/util/ChainingOps.scala#L37 */
  implicit class AnyOps[A](a: A) {
    def tap[X](e: A => X): A = {
      e(a)
      a
    }
  }

}
