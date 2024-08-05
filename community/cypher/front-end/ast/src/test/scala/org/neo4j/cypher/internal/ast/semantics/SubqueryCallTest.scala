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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SemanticCheckInTest.SemanticCheckWithDefaultContext
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.LoneElement.convertToCollectionLoneElementWrapper

//noinspection ZeroIndexToHead
class SubqueryCallTest extends CypherFunSuite with AstConstructionTestSupport {

  private val clean =
    SemanticState.clean
      .withFeature(SemanticFeature.MultipleGraphs)
      .withFeature(SemanticFeature.UseAsMultipleGraphsSelector)

  test("returned variables are added to scope after subquery") {
    // WITH 1 AS a
    // CALL {
    //   WITH 1 AS b
    //   RETURN b, 1 AS c
    // }
    // RETURN a, b, c
    singleQuery(
      with_(literal(1).as("a")),
      importingWithSubqueryCall(
        with_(literal(1).as("b")),
        return_(varFor("b").as("b"), literal(1).as("c"))
      ),
      return_(
        varFor("a").as("a"),
        varFor("b").as("b"),
        varFor("c").as("c")
      )
    )
      .semanticCheck.run(clean)
      .errors.size.shouldEqual(0)

  }

  test("outer scope is not seen in uncorrelated subquery") {
    // WITH 1 AS a
    // CALL {
    //   RETURN a AS b
    // }
    // RETURN a
    singleQuery(
      with_(literal(1).as("a")),
      importingWithSubqueryCall(
        return_(varFor("a").as("b"))
      ),
      return_(varFor("a").as("a"))
    )
      .semanticCheck.run(clean)
      .errors.loneElement.msg should include("Variable `a` not defined")
  }

  test("subquery scoping works with order by") {
    // WITH 1 AS a
    // CALL {
    //   WITH 1 AS b
    //   RETURN b, 1 AS c ORDER BY b, c
    // }
    // RETURN a, b, b AS c ORDER BY a, b, c
    singleQuery(
      with_(literal(1).as("a")),
      importingWithSubqueryCall(
        with_(literal(1).as("b")),
        return_(
          orderBy(varFor("b").asc, varFor("c").asc),
          varFor("b").as("b"),
          literal(1).as("c")
        )
      ),
      return_(
        orderBy(varFor("a").asc, varFor("b").asc, varFor("c").asc),
        varFor("a").as("a"),
        varFor("b").as("b"),
        varFor("b").as("c")
      )
    )
      .semanticCheck.run(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery can't return variable that already exists outside") {
    // WITH 1 AS x
    // CALL {
    //   RETURN 2 AS x
    // }
    // RETURN 1 AS y
    val varPos = InputPosition(1, 2, 3)
    val error = singleQuery(
      with_(literal(1).as("x")),
      importingWithSubqueryCall(
        return_(AliasedReturnItem(literal(2), Variable("x")(varPos))(pos))
      ),
      return_(literal(1).as("y"))
    )
      .semanticCheck.run(clean)
      .errors.loneElement

    error.msg.shouldEqual("Variable `x` already declared in outer scope")
    error.position.shouldEqual(varPos)
  }

  test("subquery can't implicitly return variable that already exists outside") {
    // WITH 1 AS x
    // CALL {
    //   WITH 2 AS x
    //   RETURN *
    // }
    // RETURN 1 AS y
    val itemsPos = InputPosition(1, 2, 3)
    val error = singleQuery(
      with_(literal(1).as("x")),
      importingWithSubqueryCall(
        with_(literal(1).as("x")),
        Return(ReturnItems(includeExisting = true, Seq())(itemsPos))(pos)
      ),
      return_(literal(1).as("y"))
    )
      .semanticCheck.run(clean)
      .errors.loneElement

    error.msg.shouldEqual("Variable `x` already declared in outer scope")
    error.position.shouldEqual(itemsPos)
  }

  test("subquery can't implicitly return variable that already exists outside, from a union") {
    // WITH 1 AS x
    // CALL {
    //   WITH 2 AS x
    //   RETURN *
    //     UNION
    //   WITH 3 AS x
    //   RETURN *
    // }
    // RETURN 1 AS y
    val itemsPos1 = InputPosition(1, 2, 3)
    val itemsPos2 = InputPosition(4, 5, 6)
    singleQuery(
      with_(literal(1).as("x")),
      importingWithSubqueryCall(union(
        singleQuery(
          with_(literal(2).as("x")),
          Return(ReturnItems(includeExisting = true, Seq())(itemsPos1))(pos)
        ),
        singleQuery(
          with_(literal(3).as("x")),
          Return(ReturnItems(includeExisting = true, Seq())(itemsPos2))(pos)
        )
      )),
      return_(literal(1).as("y"))
    )
      .semanticCheck.run(clean)
      .errors
      .map(e => e.msg -> e.position).shouldEqual(Seq(
        "Variable `x` already declared in outer scope" -> itemsPos1,
        "Variable `x` already declared in outer scope" -> itemsPos2
      ))
  }

  test("subquery without return does not export variables to enclosing query") {
    // CALL {
    //   CREATE (a)
    // }
    // RETURN a
    singleQuery(
      importingWithSubqueryCall(
        create(NodePattern(Some(varFor("a")), None, None, None)(pos))
      ),
      return_(varFor("a").as("a"))
    )
      .semanticCheck.run(clean)
      .errors
      .loneElement
      .msg.shouldEqual("Variable `a` not defined")
  }

  test("subquery with union without return does not export variables to enclosing query") {
    // CALL {
    //   CREATE (a)
    //     UNION
    //   CREATE (a)
    // }
    // RETURN a
    singleQuery(
      importingWithSubqueryCall(
        union(
          singleQuery(create(NodePattern(Some(varFor("a")), None, None, None)(pos))),
          singleQuery(create(NodePattern(Some(varFor("a")), None, None, None)(pos)))
        )
      ),
      return_(varFor("a").as("a"))
    )
      .semanticCheck.run(clean)
      .errors.loneElement
      .msg.shouldEqual("Variable `a` not defined")
  }

  test("subquery allows union with valid return statements at the end") {
    // CALL {
    //   RETURN 2 AS x
    //     UNION
    //   RETURN 2 AS x
    // }
    // RETURN x
    singleQuery(
      importingWithSubqueryCall(
        union(
          singleQuery(return_(literal(2).as("x"))),
          singleQuery(return_(literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"))
    )
      .semanticCheck.run(clean)
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
      importingWithSubqueryCall(
        union(
          singleQuery(return_(literal(2).as("x"), literal(2).as("y"))),
          singleQuery(return_(literal(2).as("y"), literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"), varFor("y").as("y"))
    )
      .semanticCheck.run(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery allows union with update clauses at the end") {
    // CALL {
    //   CREATE (a)
    //     UNION
    //   CREATE (a)
    // }
    // RETURN count(*) AS count
    singleQuery(
      importingWithSubqueryCall(
        union(
          singleQuery(create(NodePattern(Some(varFor("a")), None, None, None)(pos))),
          singleQuery(create(NodePattern(Some(varFor("a")), None, None, None)(pos)))
        )
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck.run(clean)
      .errors shouldBe empty
  }

  test("subquery allows union with create and return statement at the end") {
    // CALL {
    //   CREATE (a) RETURN a
    //     UNION
    //   CREATE (a) RETURN a
    // }
    // RETURN count(*) AS count
    singleQuery(
      importingWithSubqueryCall(
        union(
          singleQuery(create(NodePattern(Some(varFor("a")), None, None, None)(pos)), return_(varFor("a").as("a"))),
          singleQuery(create(NodePattern(Some(varFor("a")), None, None, None)(pos)), return_(varFor("a").as("a")))
        )
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck.run(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery allows single query with update clause at the end") {
    // CALL {
    //   MERGE (a)
    // }
    // RETURN count(*) AS count
    singleQuery(
      importingWithSubqueryCall(
        singleQuery(merge(nodePat(Some("a"))))
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck.run(clean)
      .errors shouldBe empty
  }

  test("query allows unit subquery at the end") {
    // WITH 1 AS x
    // CALL {
    //   MERGE (a)
    // }
    singleQuery(
      with_(literalInt(1).as("x")),
      importingWithSubqueryCall(
        singleQuery(merge(nodePat(Some("a"))))
      )
    )
      .semanticCheck.run(clean)
      .errors shouldBe empty
  }

  test("query allows nested unit subquery at the end") {
    // WITH 1 AS x
    // CALL {
    //   CALL {
    //     MERGE (a)
    //   }
    // }
    singleQuery(
      with_(literalInt(1).as("x")),
      importingWithSubqueryCall(
        singleQuery(importingWithSubqueryCall(
          singleQuery(merge(nodePat(Some("a"))))
        ))
      )
    )
      .semanticCheck.run(clean)
      .errors shouldBe empty
  }

  test("query allows nested union unit subquery at the end") {
    // WITH 1 AS x
    // CALL {
    //   CALL {
    //     MERGE (a)
    //       UNION
    //     MERGE (a)
    //   }
    // }
    singleQuery(
      with_(literalInt(1).as("x")),
      importingWithSubqueryCall(
        singleQuery(importingWithSubqueryCall(
          union(
            singleQuery(merge(nodePat(Some("a")))),
            singleQuery(merge(nodePat(Some("a"))))
          )
        ))
      )
    )
      .semanticCheck.run(clean)
      .errors shouldBe empty
  }

  test("subquery disallows single query ending in clause that is neither update nor return") {
    // CALL {
    //   MERGE (a)
    //   WITH a AS a
    //   UNWIND [1] AS x
    // }
    // RETURN count(*) AS count
    val unwindPos = InputPosition(1, 2, 3)
    val error = singleQuery(
      importingWithSubqueryCall(
        singleQuery(
          merge(
            nodePat(Some("a"))
          ),
          with_(varFor("a").aliased),
          unwind(listOfInt(1), varFor("x")).copy()(unwindPos)
        )
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck.run(clean)
      .errors.loneElement

    error.msg.should(include("Query cannot conclude with UNWIND"))
    error.position.shouldEqual(unwindPos)
  }

  test("subquery allows single query with create and return statement at the end") {
    // CALL {
    //   MERGE (a) RETURN a
    // }
    // RETURN count(*) AS count
    singleQuery(
      importingWithSubqueryCall(
        singleQuery(merge(nodePat(Some("a"))), return_(varFor("a").as("a")))
      ),
      return_(countStar().as("count"))
    )
      .semanticCheck.run(clean)
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
      importingWithSubqueryCall(
        union(
          singleQuery(return_(literal(2).as("x"), literal(2).as("y"), literal(2).as("z"))),
          singleQuery(return_(literal(2).as("y"), literal(2).as("x")))
        )
      ),
      return_(varFor("x").as("x"), varFor("y").as("y"))
    )
      .semanticCheck.run(clean)
      .errors.loneElement
      .msg.should(include("All sub queries in an UNION must have the same return column names"))
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
      importingWithSubqueryCall(
        with_(varFor("x").aliased),
        return_(varFor("x").as("y"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck.run(clean)
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
      importingWithSubqueryCall(
        with_(varFor("x").aliased),
        return_(varFor("y").as("z"))
      ),
      return_(varFor("x").aliased)
    )
      .semanticCheck.run(clean)
      .errors.loneElement
      .msg.should(include("Variable `y` not defined"))
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
    singleQuery(
      with_(literal(1).as("a")),
      importingWithSubqueryCall(unionDistinct(
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
      .semanticCheck.run(clean)
      .errors shouldBe empty
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
    singleQuery(
      with_(literal(1).as("a"), literal(1).as("b"), literal(1).as("c")),
      importingWithSubqueryCall(unionDistinct(
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
      .semanticCheck.run(clean)
      .errors shouldBe empty
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
      .semanticCheck.run(clean)
      .errors shouldBe empty
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
      importingWithSubqueryCall(
        unwind(listOf(literal(1)), varFor("a")),
        with_(varFor("x").aliased),
        return_(varFor("x").as("y"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck.run(clean)
      .errors.loneElement
      .msg.should(include("Variable `x` not defined"))
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
      importingWithSubqueryCall(
        use(List("g")),
        with_(varFor("x").aliased),
        return_(varFor("x").as("y"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck.run(clean)
      .errors shouldBe empty
  }

  test("importing WITH must be clean pass-through") {
    // WITH 1 AS x
    // CALL {
    //   WITH x, 2 AS y
    //   RETURN x AS z
    // }
    // RETURN x, z
    val errors = singleQuery(
      with_(literal(1).as("x")),
      importingWithSubqueryCall(
        with_(varFor("x").aliased, literal(2).as("y")),
        return_(varFor("x").as("z"))
      ),
      return_(varFor("x").aliased, varFor("z").aliased)
    )
      .semanticCheck.run(clean)
      .errors
    errors should have size 2

    errors(0).msg.should(include(
      "Importing WITH should consist only of simple references to outside variables. Aliasing or expressions are not supported."
    ))
    errors(1).msg.should(include("Variable `x` not defined"))
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
      importingWithSubqueryCall(
        with_(varFor("x").aliased),
        with_(varFor("x").aliased, literal(2).as("y")),
        return_(varFor("x").as("z"), varFor("y").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck.run(clean)
      .errors.size.shouldEqual(0)
  }

  test("subquery leading USE may reference outer variables") {
    // WITH 1 AS x, 2 AS y
    // CALL {
    //   USE graph.byName(x, y)
    //   WITH x
    //   RETURN 1 AS z
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x"), literal(2).as("y")),
      importingWithSubqueryCall(
        use(function("graph.byName", varFor("x"), varFor("y"))),
        with_(varFor("x").aliased),
        return_(literal(1).as("z"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck.run(clean)
      .errors shouldBe empty
  }

  test("subquery USE after imports may only reference imported variables") {
    // WITH 1 AS x, 2 AS y
    // CALL {
    //   WITH x
    //   USE graph.byName(x, y)
    //   RETURN 3 AS z
    // }
    // RETURN x, y
    singleQuery(
      with_(literal(1).as("x"), literal(2).as("y")),
      importingWithSubqueryCall(
        with_(varFor("x").aliased),
        use(function("graph.byName", varFor("x"), varFor("y"))),
        return_(literal(3).as("z"))
      ),
      return_(varFor("x").aliased, varFor("y").aliased)
    )
      .semanticCheck.run(clean)
      .errors.loneElement
      .msg.should(include("Variable `y` not defined"))
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
      importingWithSubqueryCall(
        with_(literal(1).as("y")),
        importingWithSubqueryCall(
          with_(literal(1).as("z")),
          return_(varFor("z").aliased)
        ),
        return_(varFor("y").aliased, varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck.run(clean)
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
    val errors = singleQuery(
      with_(literal(1).as("x")),
      importingWithSubqueryCall(
        with_(literal(1).as("y")),
        importingWithSubqueryCall(
          with_(literal(1).as("z")),
          with_(varFor("x").as("a"), varFor("y").as("b"), varFor("z").aliased),
          return_(varFor("z").aliased)
        ),
        return_(varFor("y").aliased, varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck.run(clean)
      .errors
    errors should have size 2
    errors(0).msg.should(include("Variable `x` not defined"))
    errors(1).msg.should(include("Variable `y` not defined"))
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
      importingWithSubqueryCall(
        with_(varFor("x").aliased),
        with_(varFor("x").as("y")),
        importingWithSubqueryCall(
          with_(varFor("y").aliased),
          with_(varFor("y").as("z")),
          return_(varFor("z").aliased)
        ),
        return_(varFor("y").aliased, varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck.run(clean)
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
      importingWithSubqueryCall(
        with_(varFor("x").aliased),
        importingWithSubqueryCall(
          with_(varFor("y").aliased),
          return_(literal(3).as("z"))
        ),
        return_(varFor("z").aliased)
      ),
      return_(varFor("x").aliased, varFor("y").aliased, varFor("z").aliased)
    )
      .semanticCheck.run(clean)
      .errors.loneElement
      .msg.should(include("Variable `y` not defined"))
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
      use(List("foo")),
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
      importingWithSubqueryCall(
        with_(mapOfInt("title" -> 1).as("m")),
        return_(UnaliasedReturnItem(prop(varFor("m"), "title"), "m.title")(pos))
      ),
      return_(literalInt(1).as("x"))
    )
      .semanticCheck.run(clean)
      .errors.loneElement.msg should include("Expression in CALL { RETURN ... } must be aliased (use AS)")
  }

  test("subquery should allow multiple import WITH clauses") {
    // MATCH (x)
    // CALL {
    //   WITH x
    //   CREATE ()
    //   WITH x
    //   MATCH (n)
    //   RETURN n
    // }
    // RETURN n

    val x = singleQuery(
      match_(nodePat(Some("x"))),
      importingWithSubqueryCall(
        with_(varFor("x").aliased),
        create(nodePat()),
        with_(varFor("x").aliased),
        match_(nodePat(Some("n"))),
        return_(varFor("n").aliased)
      ),
      return_(varFor("n").aliased)
    )
      .semanticCheck.run(clean)

    x.errors.size.shouldEqual(0)
  }
}
