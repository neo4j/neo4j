/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.Order.ambiguousAggregationMessage
import org.neo4j.cypher.internal.ast.Order.notProjectedAggregations
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class OrderTest extends CypherFunSuite with AstConstructionTestSupport {

  case class TestOrderBy(
    sortItems: Seq[SortItem],
    returnItems: Seq[ReturnItem],
    clause: String = "RETURN",
    invalidGroupingKeys: Seq[String] = Seq.empty
  )

  test("should not report ambiguous aggregation expressions") {
    val tests = Seq(
      // "MATCH (n) RETURN n AS a, count(n.y) ORDER BY n",
      TestOrderBy(
        sortItems = Seq(sortItem(varFor("n"))),
        returnItems = Seq(aliasedReturnItem("n", "a"), autoAliasedReturnItem(count(prop("n", "y"))))
      ),
      // "MATCH (n) RETURN n AS a, count(n.y) ORDER BY a",
      TestOrderBy(
        sortItems = Seq(sortItem(varFor("a"))),
        returnItems = Seq(aliasedReturnItem("n", "a"), autoAliasedReturnItem(count(prop("n", "y"))))
      ),
      // "MATCH (n) RETURN n.num AS num, count(*) AS cnt ORDER BY num"
      TestOrderBy(
        sortItems = Seq(sortItem(varFor("num"))),
        returnItems = Seq(aliasedReturnItem(prop("n", "num"), "num"), aliasedReturnItem(countStar(), "cnt"))
      ),
      // "MATCH (n) RETURN n.num AS num, count(*) AS cnt ORDER BY n.num"
      TestOrderBy(
        sortItems = Seq(sortItem(prop("n", "num"))),
        returnItems = Seq(aliasedReturnItem(prop("n", "num"), "num"), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n, count(*) AS cnt ORDER BY n.num
      TestOrderBy(
        sortItems = Seq(sortItem(prop("n", "num"))),
        returnItems = Seq(autoAliasedReturnItem(varFor("n")), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n, n.num, count(*) AS cnt ORDER BY n.num
      TestOrderBy(
        sortItems = Seq(sortItem(prop("n", "num"))),
        returnItems = Seq(
          autoAliasedReturnItem(varFor("n")),
          autoAliasedReturnItem(prop("n", "num")),
          autoAliasedReturnItem(countStar())
        )
      ),
      // MATCH (n) RETURN n.num AS num, count(*) AS cnt ORDER BY num + 2
      TestOrderBy(
        sortItems = Seq(sortItem(add(varFor("num"), literalInt(2)))),
        returnItems = Seq(aliasedReturnItem(prop("n", "num"), "num"), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n.num AS num, count(*) AS cnt ORDER BY 2 + num
      TestOrderBy(
        sortItems = Seq(sortItem(add(literalInt(2), varFor("num")))),
        returnItems = Seq(aliasedReturnItem(prop("n", "num"), "num"), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n, count(*) AS cnt ORDER BY 2 + n.num
      TestOrderBy(
        sortItems = Seq(sortItem(add(literalInt(2), prop("n", "num")))),
        returnItems = Seq(autoAliasedReturnItem(varFor("n")), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n.num + 2 AS num, count(*) AS cnt ORDER BY num
      TestOrderBy(
        sortItems = Seq(sortItem(varFor("num"))),
        returnItems =
          Seq(aliasedReturnItem(add(prop("n", "num"), literalInt(2)), "num"), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n.num + n.x AS num, count(*) AS cnt ORDER BY num
      TestOrderBy(
        sortItems = Seq(sortItem(varFor("num"))),
        returnItems =
          Seq(aliasedReturnItem(add(prop("n", "num"), prop("n", "x")), "num"), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n, count(*) AS cnt ORDER BY n.num + n.x
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "num"), prop("n", "x")))),
        returnItems = Seq(autoAliasedReturnItem(varFor("n")), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) WITH n.x AS nx, count(*) AS cnt ORDER BY nx RETURN nx, cnt
      TestOrderBy(
        clause = "WITH",
        sortItems = Seq(sortItem(varFor("nx"))),
        returnItems = Seq(aliasedReturnItem(prop("n", "x"), "nx"), aliasedReturnItem(countStar(), "cnt"))
      ),
      // MATCH (n) RETURN n.x ORDER BY n.y - no aggregation
      TestOrderBy(
        sortItems = Seq(sortItem(prop("n", "y"))),
        returnItems = Seq(autoAliasedReturnItem(prop("n", "x")))
      ),
      // MATCH (n) RETURN n.x, n.y AS ny, count(*) ORDER BY n.x
      TestOrderBy(
        sortItems = Seq(sortItem(prop("n", "x"))),
        returnItems = Seq(
          autoAliasedReturnItem(prop("n", "x")),
          aliasedReturnItem(prop("n", "y"), "ny"),
          autoAliasedReturnItem(countStar())
        )
      ),
      // MATCH (n) RETURN n.num AS num, count(*) AS cnt ORDER BY n.num
      TestOrderBy(
        sortItems = Seq(sortItem(prop("n", "num"))),
        returnItems = Seq(
          aliasedReturnItem(prop("n", "num"), "num"),
          aliasedReturnItem(countStar(), "cnt")
        )
      ),
      // MATCH (n) RETURN n.num AS num, count(*) AS cnt ORDER BY n.num + 2
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "num"), literalInt(2)))),
        returnItems = Seq(
          aliasedReturnItem(prop("n", "num"), "num"),
          aliasedReturnItem(countStar(), "cnt")
        )
      ),
      // MATCH (n) RETURN n.num AS num, count(*) AS cnt ORDER BY 2 + n.num
      TestOrderBy(
        sortItems = Seq(sortItem(add(literalInt(2), prop("n", "num")))),
        returnItems = Seq(
          aliasedReturnItem(prop("n", "num"), "num"),
          aliasedReturnItem(countStar(), "cnt")
        )
      ),
      // MATCH (n) WITH n.x AS nx, count(*) AS cnt ORDER BY n.x RETURN nx, cnt
      TestOrderBy(
        clause = "WITH",
        sortItems = Seq(sortItem(prop("n", "x"))),
        returnItems = Seq(
          aliasedReturnItem(prop("n", "x"), "nx"),
          aliasedReturnItem(countStar(), "cnt")
        )
      )
    )

    tests.foreach { test =>
      val orderBy = OrderBy(test.sortItems)(InputPosition.NONE)
      val result = orderBy.checkIllegalOrdering(
        ReturnItems(includeExisting = false, test.returnItems)(InputPosition.NONE)
      )(SemanticState.clean)

      result.errors should have size 0
    }
  }

  test("should report ambiguous aggregation expressions") {
    val tests = Seq(
      // MATCH (n) RETURN n.num + 2 AS num, count(*) AS cnt ORDER BY n.num + 2
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "num"), literalInt(2)))),
        returnItems = Seq(
          aliasedReturnItem(add(prop("n", "num"), literalInt(2)), "num"),
          aliasedReturnItem(countStar(), "cnt")
        ),
        invalidGroupingKeys = Seq("n.num")
      ),
      // MATCH (n) RETURN n.num + n.x AS num, count(*) AS cnt ORDER BY n.num + n.x
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "num"), prop("n", "x")))),
        returnItems = Seq(
          aliasedReturnItem(add(prop("n", "num"), prop("n", "x")), "num"),
          aliasedReturnItem(countStar(), "cnt")
        ),
        invalidGroupingKeys = Seq("n.num", "n.x")
      ),
      // WITH {a: {b: 2}} AS map RETURN map.a.b, count(*) AS cnt ORDER BY map.a.b
      TestOrderBy(
        sortItems = Seq(sortItem(prop(prop(varFor("map"), "a"), "b"))),
        returnItems = Seq(
          autoAliasedReturnItem(prop(prop(varFor("map"), "a"), "b")),
          aliasedReturnItem(countStar(), "cnt")
        ),
        invalidGroupingKeys = Seq("map.a")
      ),
      // RETURN n.x + n.y, count(*) ORDER BY n.x + n.y
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "x"), prop("n", "y")))),
        returnItems = Seq(
          autoAliasedReturnItem(add(prop("n", "x"), prop("n", "y"))),
          autoAliasedReturnItem(countStar())
        ),
        invalidGroupingKeys = Seq("n.x", "n.y")
      ),
      // ----- TEST CACHED PLAN ------ RETURN n.x + n.y AS `n.x + n.y`, count(*) ORDER BY n.x + n.y
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "x"), prop("n", "y")))),
        returnItems = Seq(
          aliasedReturnItem(add(prop("n", "x"), prop("n", "y")), "n.x + n.y"),
          autoAliasedReturnItem(countStar())
        ),
        invalidGroupingKeys = Seq("n.x", "n.y")
      ),
      // WITH n.x + n.y AS nxy, count(*) AS cnt ORDER BY n.x + n.y
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "x"), prop("n", "y")))),
        returnItems = Seq(
          aliasedReturnItem(add(prop("n", "x"), prop("n", "y")), "nxy"),
          aliasedReturnItem(countStar(), "cnt")
        ),
        invalidGroupingKeys = Seq("n.x", "n.y")
      ),
      // MATCH (n) RETURN n.x + n.y AS nxy, n.z, count(*) AS cnt ORDER BY n.x + n.y
      TestOrderBy(
        sortItems = Seq(sortItem(add(prop("n", "x"), prop("n", "y")))),
        returnItems = Seq(
          aliasedReturnItem(add(prop("n", "x"), prop("n", "y")), "nxy"),
          autoAliasedReturnItem(prop("n", "z")),
          aliasedReturnItem(countStar(), "cnt")
        ),
        invalidGroupingKeys = Seq("n.x", "n.y")
      ),
      // RETURN n.x + n.y, m.x + m.y, count(*) ORDER BY (n.x + n.y) + (m.x + m.y)
      TestOrderBy(
        sortItems = Seq(sortItem(add(add(prop("n", "x"), prop("n", "y")), add(prop("m", "x"), prop("m", "y"))))),
        returnItems = Seq(
          autoAliasedReturnItem(add(prop("n", "x"), prop("n", "y"))),
          autoAliasedReturnItem(add(prop("m", "x"), prop("m", "y"))),
          autoAliasedReturnItem(countStar())
        ),
        invalidGroupingKeys = Seq("n.x", "n.y", "m.x", "m.y")
      ),
      // RETURN n.x + n.y, m.x + m.y, count(*) ORDER BY (n.x + n.y), (m.x + m.y) DESC
      TestOrderBy(
        sortItems = Seq(
          sortItem(add(prop("n", "x"), prop("n", "y"))),
          sortItem(add(prop("m", "x"), prop("m", "y")), ascending = false)
        ),
        returnItems = Seq(
          autoAliasedReturnItem(add(prop("n", "x"), prop("n", "y"))),
          autoAliasedReturnItem(add(prop("m", "x"), prop("m", "y"))),
          autoAliasedReturnItem(countStar())
        ),
        invalidGroupingKeys = Seq("n.x", "n.y", "m.x", "m.y")
      )
    )

    tests.foreach { test: TestOrderBy =>
      val orderBy = OrderBy(test.sortItems)(InputPosition.NONE)
      val result = orderBy.checkIllegalOrdering(
        ReturnItems(includeExisting = false, test.returnItems)(InputPosition.NONE)
      )(SemanticState.clean)
      val expectedErrorMessage = ambiguousAggregationMessage(test.invalidGroupingKeys)

      withClue(
        s"orderBy expressions [${test.sortItems.map(_.asCanonicalStringVal).mkString(",")}] " +
          s"with returnItems [${test.returnItems.map(_.asCanonicalStringVal).mkString(", ")}] did not throw expected error. "
      ) {
        result.errors should have size 1
        result.errors.head.msg shouldBe expectedErrorMessage
      }
    }
  }

  test("should report both ambiguous expression and aggregation not in preceding with/return clause") {
    // RETURN n.prop1, 1 + count(*)     AS cnt ORDER BY n.prop2, count(*) + 1
    val sortItems = Seq(
      sortItem(prop("n", "prop2")),
      sortItem(add(literalInt(1), countStar()), ascending = false)
    )
    val returnItems = Seq(
      autoAliasedReturnItem(prop("n", "prop1")),
      autoAliasedReturnItem(add(countStar(), literalInt(1)))
    )
    val orderBy = OrderBy(sortItems)(InputPosition.NONE)
    val result = orderBy.checkIllegalOrdering(ReturnItems(includeExisting = false, returnItems)(InputPosition.NONE))(
      SemanticState.clean
    )
    val expectedErrorMessage1 = ambiguousAggregationMessage(Seq("n.prop2"))
    val expectedErrorMessage2 = notProjectedAggregations(Seq("count(*)"))

    withClue(s"orderBy expressions [${sortItems.map(_.asCanonicalStringVal).mkString(",")}] " +
      s"with returnItems [${returnItems.map(_.asCanonicalStringVal).mkString(", ")}] did not throw expected error. ") {
      result.errors should have size 2
      val msgs = result.errors.map(_.msg)
      assert(msgs.contains(expectedErrorMessage1))
      assert(msgs.contains(expectedErrorMessage2))
    }
  }
}
