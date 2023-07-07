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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  case class Scenario(returnItems: Seq[ReturnItem], invalidExpr: Seq[String])

  test("should forbid aliased projections collisions, e.g., projecting more than one value to the same id") {
    val item1 = AliasedReturnItem(literalString("a"), varFor("n"))(pos)
    val item2 = AliasedReturnItem(literalString("b"), varFor("n"))(pos)

    val items = ReturnItems(includeExisting = false, Seq(item1, item2)) _

    val result = items.semanticCheck(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.msg should startWith("Multiple result columns with the same name are not supported")
  }

  test("should forbid unaliased projections collisions, e.g., projecting more than one value to the same id") {
    val item1 = UnaliasedReturnItem(literalString("a"), "a") _
    val item2 = UnaliasedReturnItem(literalString("a"), "a") _

    val items = ReturnItems(includeExisting = false, Seq(item1, item2)) _

    val result = items.semanticCheck(SemanticState.clean)

    result.errors should have size 1
    result.errors.head.msg should startWith("Multiple result columns with the same name are not supported")
  }

  test("should not forbid aliased projections of the same expression with different names") {
    val item1 = AliasedReturnItem(literalString("a"), varFor("n"))(pos)
    val item2 = AliasedReturnItem(literalString("a"), varFor("m"))(pos)

    val items = ReturnItems(includeExisting = false, Seq(item1, item2)) _

    val result = items.semanticCheck(SemanticState.clean)

    result.errors shouldBe empty
  }

  test("should not report ambiguous aggregation expressions") {
    val paramX = parameter("x", CTInteger)
    val tests = Seq(
      // RETURN 1 + count(*)
      Seq(autoAliasedReturnItem(add(literalInt(1), countStar()))),
      // RETURN 1, 1 + count(*)
      Seq(
        autoAliasedReturnItem(literalInt(1)),
        autoAliasedReturnItem(add(literalInt(1), countStar()))
      ),
      // RETURN $x + count($x)
      Seq(autoAliasedReturnItem(add(paramX, count(paramX)))),
      // RETURN 1 + count($x) + $x * 7 + sum($x) + 'cake'
      Seq(
        autoAliasedReturnItem(
          add(
            literalInt(1),
            add(count(paramX), add(multiply(paramX, literalInt(7)), add(sum(paramX), literalString("cake"))))
          )
        )
      ),
      // RETURN nx, 1 + count(nx)
      Seq(
        autoAliasedReturnItem(varFor("nx")),
        autoAliasedReturnItem(add(literalInt(1), count(varFor("nx"))))
      ),
      // RETURN nx, nx - 1 + count(nx)
      Seq(
        autoAliasedReturnItem(varFor("nx")),
        autoAliasedReturnItem(subtract(varFor("nx"), add(literalInt(1), count(varFor("nx")))))
      ),
      // RETURN nx, nx + count(nx)
      Seq(
        autoAliasedReturnItem(varFor("nx")),
        autoAliasedReturnItem(add(varFor("nx"), count(varFor("nx"))))
      ),
      // RETURN nx, nx + count(*) + nx
      Seq(
        autoAliasedReturnItem(varFor("nx")),
        autoAliasedReturnItem(add(varFor("nx"), add(count(varFor("nx")), varFor("nx"))))
      ),
      // RETURN map, map.a + count(map.b)
      Seq(
        autoAliasedReturnItem(varFor("map")),
        autoAliasedReturnItem(add(prop(varFor("map"), "a"), count(prop(varFor("map"), "b"))))
      ),
      // RETURN n.x, n.y, n.z, n.x + n.y + count(n.x) + n.z
      Seq(
        autoAliasedReturnItem(prop("n", "x")),
        autoAliasedReturnItem(prop("n", "y")),
        autoAliasedReturnItem(prop("n", "z")),
        autoAliasedReturnItem(
          add(prop("n", "x"), add(prop("n", "y"), add(count(prop("n", "x")), prop("n", "z"))))
        )
      ),
      // RETURN a, count(*) + size([ x IN range(1, 10) | x ])
      Seq(
        autoAliasedReturnItem(varFor("a")),
        autoAliasedReturnItem(add(
          countStar(),
          Size(listComprehension(
            varFor("x"),
            function("range", literalInt(1), literalInt(10)),
            None,
            Some(varFor("x"))
          ))(pos)
        ))
      )
    )

    tests.foreach { returnItems =>
      val result = ReturnItems.checkAmbiguousGrouping(
        ReturnItems(includeExisting = false, returnItems)(InputPosition.NONE)
      )(SemanticState.clean)

      withClue(s"returnItems threw unexpected error: $returnItems") {
        result.errors should have size 0
      }
    }
  }

  test("should report ambiguous aggregation expressions") {
    val tests: Seq[Scenario] = Seq(
      // --- implicit grouping keys ---
      // RETURN n.x + count(*)
      Scenario(
        returnItems = Seq(autoAliasedReturnItem(add(prop("n", "x"), countStar()))),
        invalidExpr = Seq("n.x")
      ),
      // RETURN n.x + count(*) + n.y
      Scenario(
        returnItems = Seq(autoAliasedReturnItem(add(prop("n", "x"), add(countStar(), prop("n", "y"))))),
        invalidExpr = Seq("n.x", "n.y")
      ),
      // RETURN nx, count(nx) + ny
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(varFor("nx")),
          autoAliasedReturnItem(add(count(varFor("nx")), varFor("ny")))
        ),
        invalidExpr = Seq("ny")
      ),
      // length(path) + count(n)
      Scenario(
        returnItems = Seq(autoAliasedReturnItem(add(function("length", varFor("path")), count(varFor("n"))))),
        invalidExpr = Seq("path")
      ),
      // RETURN a, count(*) + size([ x IN range(1, 10) | x + b.prop ])
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(varFor("a")),
          autoAliasedReturnItem(add(
            countStar(),
            Size(
              listComprehension(
                varFor("x"),
                function("range", literalInt(1), literalInt(10)),
                None,
                Some(add(varFor("x"), prop("b", "prop")))
              )
            )(pos)
          ))
        ),
        invalidExpr = Seq("b.prop")
      ),
      // RETURN n.x + n.y, n.x + collect(n.x)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(add(prop("n", "x"), prop("n", "y"))),
          autoAliasedReturnItem(add(prop("n", "x"), collect(prop("n", "x"))))
        ),
        invalidExpr = Seq("n.x")
      ),
      // RETURN n.x * n.x, n.x + collect(n.x)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(multiply(prop("n", "x"), prop("n", "x"))),
          autoAliasedReturnItem(add(prop("n", "x"), collect(prop("n", "x"))))
        ),
        invalidExpr = Seq("n.x")
      ),

      // --- semantically correct, but semantic checking can not figure that out ---
      // -- user can rewrite to --> WITH n.x AS nx RETURN nx, nx + count(nx)
      // WITH n, n.x AS nx RETURN nx, n.x + count(n.x)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(varFor("nx")),
          autoAliasedReturnItem(add(prop("n", "x"), count(prop("n", "x"))))
        ),
        invalidExpr = Seq("n.x")
      ),
      // -- user can rewrite to --> RETURN 1 + count(*)
      // WITH 1 AS x RETURN x + count(x)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(add(varFor("x"), count(varFor("x"))))
        ),
        invalidExpr = Seq("x")
      ),
      // -- user can rewrite to --> MATCH (n) WITH n.x + 1 AS nx RETURN nx, nx + count(nx)
      // MATCH (n) RETURN n.x + 1, n.x + 1 + count(n.x)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(add(prop("n", "x"), literalInt(1))),
          autoAliasedReturnItem(add(add(prop("n", "x"), literalInt(1)), count(prop("n", "x"))))
        ),
        invalidExpr = Seq("n.x")
      ),
      // RETURN n.x + 1, n.x + count(n.x) + 1
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(add(prop("n", "x"), literalInt(1))),
          autoAliasedReturnItem(add(add(prop("n", "x"), count(prop("n", "x"))), literalInt(1)))
        ),
        invalidExpr = Seq("n.x")
      ),
      // -- user can rewrite to --> MATCH (n) WITH n.a + n.b + n.c AS group RETURN group, group + count(n)
      // RETURN n.x + n.y + n.z, count(n) + n.x + n.y + n.z
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(add(add(prop("n", "x"), prop("n", "y")), prop("n", "z"))),
          autoAliasedReturnItem(add(add(add(count(varFor("n")), prop("n", "x")), prop("n", "y")), prop("n", "z")))
        ),
        invalidExpr = Seq("n.x", "n.y", "n.z")
      ),
      // RETURN n.x + n.y + n.z, count(n) + (n.x + n.y + n.z)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(add(add(prop("n", "x"), prop("n", "y")), prop("n", "z"))),
          autoAliasedReturnItem(add(count(varFor("n")), add(add(prop("n", "x"), prop("n", "y")), prop("n", "z"))))
        ),
        invalidExpr = Seq("n.x", "n.y", "n.z")
      ),
      // RETURN n.x + n.y + n.z, n.x + n.y + n.z + count(n)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(add(add(prop("n", "x"), prop("n", "y")), prop("n", "z"))),
          autoAliasedReturnItem(add(add(add(prop("n", "x"), prop("n", "y")), prop("n", "z")), count(varFor("n"))))
        ),
        invalidExpr = Seq("n.x", "n.y", "n.z")
      ),
      // -- user can rewrite to --> WITH map.b.c AS c RETURN c, c + count(*)
      // "RETURN map.b.c, map.b.c + count(*)",
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(prop(prop("map", "b"), "c")),
          autoAliasedReturnItem(add(prop(prop("map", "b"), "c"), countStar()))
        ),
        invalidExpr = Seq("map.b")
      ),
      // RETURN m, n.x + n.y, (n.x + n.y) + count(*)
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(varFor("m")),
          autoAliasedReturnItem(add(prop("n", "x"), prop("n", "y"))),
          autoAliasedReturnItem(add(add(prop("n", "x"), prop("n", "y")), countStar()))
        ),
        invalidExpr = Seq("n.x", "n.y")
      ), // RETURN m, (n.x + n.y) + count(*), n.x + n.y
      Scenario(
        returnItems = Seq(
          autoAliasedReturnItem(varFor("m")),
          autoAliasedReturnItem(add(add(prop("n", "x"), prop("n", "y")), countStar())),
          autoAliasedReturnItem(add(prop("n", "x"), prop("n", "y")))
        ),
        invalidExpr = Seq("n.x", "n.y")
      )
    )

    tests.foreach { case Scenario(returnItems, invalidExpr) =>
      val result = ReturnItems.checkAmbiguousGrouping(
        ReturnItems(includeExisting = false, returnItems)(InputPosition.NONE)
      )(SemanticState.clean)
      val expectedErrorMessage = ReturnItems.implicitGroupingExpressionInAggregationColumnErrorMessage(invalidExpr)

      withClue(
        s"returnItems [${returnItems.map(_.asCanonicalStringVal).mkString(", ")}] did not throw expected error. "
      ) {
        result.errors should have size 1
        result.errors.head.msg shouldBe expectedErrorMessage
      }
    }
  }

  test("ambiguous aggregation expressions: should use correct position if there are multiple return items") {
    val returnItems = Seq(
      autoAliasedReturnItem(varFor("nx", InputPosition(1, 2, 3))),
      autoAliasedReturnItem(add(count(varFor("nx")), varFor("ny", InputPosition(2, 3, 4)), InputPosition(3, 4, 5)))
    )
    val result = ReturnItems.checkAmbiguousGrouping(
      ReturnItems(includeExisting = false, returnItems)(InputPosition.NONE)
    )(SemanticState.clean)
    result.errors should equal(Seq(
      // Reports all offending return items.
      // Uses position of the first offending return item.
      SemanticError(
        ReturnItems.implicitGroupingExpressionInAggregationColumnErrorMessage(Seq("ny")),
        InputPosition(2, 3, 4)
      )
    ))
  }
}
