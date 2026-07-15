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

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.p
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N07
import org.neo4j.gqlstatus.GqlHelper.getGql42001_42N57

abstract class SubqueryExpressionSemanticAnalysisTest(exprType: String, postFix: String)
    extends CypherFunSuite
    with NameBasedSemanticAnalysisTestSuite {

  private val keyword: String = exprType.toUpperCase
  private val article = if (exprType.equals("Exists")) "An" else "A"

  test(s"RETURN $keyword { MATCH (n) RETURN n }") {
    run().hasNoErrors
  }

  test(s"""MATCH (m)
          |WHERE $keyword { OPTIONAL MATCH (a)-[r]->(b) RETURN a.prop }$postFix
          |RETURN m
          |""".stripMargin) {
    run().hasNoErrors
  }

  test(s"""MATCH (m)
          |WHERE $keyword { MATCH (a:A)-[r]->(b) USING SCAN a:A RETURN a.prop }$postFix
          |RETURN m
          |""".stripMargin) {
    run().hasNoErrors
  }

  test(s"""MATCH (a)
          |RETURN $keyword { CREATE (b) RETURN b}
          |""".stripMargin) {
    run().hasError(
      getGql42001_42N57(exprType, 17, 2, 8),
      s"$article $exprType Expression cannot contain any updates",
      p(17, 2, 8)
    )
  }

  test(s"""MATCH (a)
          |RETURN $keyword { SET a.name = 1 RETURN a }
          |""".stripMargin) {
    run().hasError(
      getGql42001_42N57(exprType, 17, 2, 8),
      s"$article $exprType Expression cannot contain any updates",
      p(17, 2, 8)
    )
  }

  test(s"""MATCH (a)
          |RETURN $keyword { MATCH (b) WHERE b.a = a.a DETACH DELETE b RETURN b }
          |""".stripMargin) {
    run().hasError(
      getGql42001_42N57(exprType, 17, 2, 8),
      s"$article $exprType Expression cannot contain any updates",
      p(17, 2, 8)
    )
  }

  test(s"""MATCH (a)
          |RETURN $keyword { MATCH (b) MERGE (b)-[:FOLLOWS]->(:Person) RETURN b }
          |""".stripMargin) {
    run().hasError(
      getGql42001_42N57(exprType, 17, 2, 8),
      s"$article $exprType Expression cannot contain any updates",
      p(17, 2, 8)
    )
  }

  test(s"""MATCH (a)
          |RETURN $keyword { CALL db.labels() YIELD label RETURN label }
          |""".stripMargin) {
    run().hasNoErrors
  }

  test(s"""MATCH (a)
          |RETURN $keyword {
          |   MATCH (a)-[:KNOWS]->(b)
          |   RETURN b.name as name
          |   UNION ALL
          |   MATCH (a)-[:LOVES]->(b)
          |   RETURN b.name as name
          |}""".stripMargin) {
    run().hasNoErrors
  }

  test(s"""MATCH (a)
          |RETURN $keyword { MATCH (m)-[r]->(p), (a)-[r2]-(c) RETURN m.prop }
          |""".stripMargin) {
    run().hasNoErrors
  }

  test(
    s"""MERGE p=(a)-[:T]->()
       |WITH *
       |WHERE $keyword { WITH p AS n RETURN 1 }$postFix
       |RETURN 1
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    s"""MATCH p=(a)-[:T]->()
       |WITH *
       |WHERE $keyword { RETURN p }$postFix
       |RETURN 1
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(s"""MATCH p=(a)-[]-()
          |WITH p
          |WHERE $keyword { WITH a RETURN 1 }$postFix
          |RETURN 1
          |""".stripMargin) {
    run().hasNoErrors
  }

  test(
    s"""MATCH p=()-[]->()
       |RETURN * ORDER BY $keyword {
       |  WITH p
       |  RETURN 1
       |}
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(s"""WITH 5 as aNum
          |MATCH (a)
          |RETURN $keyword {
          |  WITH 6 as aNum
          |  MATCH (a)-->(b) WHERE b.prop = aNum
          |  RETURN a
          |}
          |""".stripMargin) {
    run().hasError(
      getGql42001_42N07("aNum", 47 + keyword.length, 4, 13),
      "The variable `aNum` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      p(47 + keyword.length, 4, 13)
    )
  }

  test(s"""WITH 5 as aNum
          |MATCH (a)
          |RETURN $keyword {
          |  MATCH (a)-->(b) WHERE b.prop = aNum
          |  WITH 6 as aNum
          |  MATCH (b)-->(c) WHERE c.prop = aNum
          |  RETURN a
          |}
          |""".stripMargin) {
    run().hasError(
      getGql42001_42N07("aNum", 85 + keyword.length, 5, 13),
      "The variable `aNum` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      p(85 + keyword.length, 5, 13)
    )
  }

  test(s"""MATCH (a)
          |RETURN $keyword {
          |  MATCH (a)-->(b)
          |  WITH b as a
          |  MATCH (b)-->(c)
          |  RETURN a
          |}
          |""".stripMargin) {
    run().hasError(
      getGql42001_42N07("a", 50 + keyword.length, 4, 13),
      "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      p(50 + keyword.length, 4, 13)
    )
  }

  test(
    s"""MATCH (a)
       |RETURN $keyword {
       |  MATCH (b)
       |  RETURN b AS a
       |  UNION
       |  MATCH (a)
       |  RETURN a
       |}
       |""".stripMargin
  ) {
    run().hasAtLeastOneGqlErrorIn(_ =>
      Seq(
        // Semantic Analysis
        (
          getGql42001_42N07("a", 46 + keyword.length, 4, 15),
          "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
          p(46 + keyword.length, 4, 15)
        ),
        // Variable Checker
        (
          getGql42001_42N07("a", 34 + keyword.length, 4, 3),
          "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
          p(34 + keyword.length, 4, 3)
        )
      )
    )
  }

  test(
    s"""MATCH (a)
       |RETURN $keyword {
       |  MATCH (a)
       |  RETURN a
       |  UNION ALL
       |  MATCH ()-->(a)
       |  RETURN a
       |  UNION ALL
       |  MATCH (b)
       |  RETURN b AS a
       |}
       |""".stripMargin
  ) {
    run().hasAtLeastOneGqlErrorIn(_ =>
      Seq(
        // Semantic Analysis
        (
          getGql42001_42N07("a", 121 + keyword.length, 10, 15),
          "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
          p(121 + keyword.length, 10, 15)
        ),
        // Variable Checker
        (
          getGql42001_42N07("a", 109 + keyword.length, 10, 3),
          "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
          p(109 + keyword.length, 10, 3)
        )
      )
    )
  }

  test(
    s"""MATCH (a) ((n)-->() WHERE $keyword { RETURN 1 AS a })+
       |RETURN *
       |""".stripMargin
  ) {
    run().hasAtLeastOneGqlErrorIn(_ =>
      Seq(
        // Semantic Analysis
        (
          getGql42001_42N07("a", 41 + keyword.length, 1, 42 + keyword.length),
          "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
          p(41 + keyword.length, 1, 42 + keyword.length)
        ),
        // Variable Checker
        (
          getGql42001_42N07("a", 29 + keyword.length, 1, 30 + keyword.length),
          "The variable `a` is shadowing a variable with the same name from the outer scope and needs to be renamed",
          p(29 + keyword.length, 1, 30 + keyword.length)
        )
      )
    )
  }

  test(s"""MATCH (a)
          |RETURN $keyword {
          |  MATCH (a)-->(b)
          |  WITH b as c
          |  MATCH (c)-->(d)
          |  RETURN a
          |}
          |""".stripMargin) {
    run().hasNoErrors
  }

  test(
    s"""MATCH (a)
       |RETURN $keyword {
       |  MATCH (a)
       |  RETURN a
       |  UNION
       |  MATCH (a)
       |  RETURN a
       |  UNION
       |  MATCH (a)
       |  RETURN a
       |}
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    s"""MATCH (a)
       |RETURN $keyword {
       |  MATCH (a)
       |  RETURN a
       |  UNION ALL
       |  MATCH (a)
       |  RETURN a
       |}
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    s"""MATCH (person:Person)
       |WHERE $keyword {
       |    MATCH (n)
       |    RETURN n as a
       |    UNION ALL
       |    MATCH (m)
       |    RETURN m as a
       |}$postFix
       |RETURN person.name
     """.stripMargin
  ) {
    run().hasNoErrors
  }

  test(s"""MATCH (person:Person)
          |WHERE $keyword {
          |    RETURN CASE
          |       WHEN true THEN 1
          |       ELSE 2
          |    END
          |}$postFix
          |RETURN person.name
     """.stripMargin) {
    run().hasNoErrors
  }

  test(
    s"""MATCH (n)
       |RETURN $keyword {
       |   CALL {
       |     MATCH (n)
       |     RETURN 1 AS a
       |   }
       |   RETURN a
       |}
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    s"""
       |MATCH (n)
       |RETURN $keyword {
       |   CALL {
       |     MATCH (n)
       |     RETURN $keyword { CALL { MATCH (n) RETURN n AS a } RETURN a } AS a
       |   }
       |   RETURN a
       |}
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    s"""UNWIND [1, 2, 3] AS x
       |CALL {
       |    WITH x
       |    RETURN x * 10 AS y
       |}
       |RETURN $keyword {
       |   WITH 10 as x
       |   MATCH (n) WHERE n.prop = x
       |   RETURN n.prop
       |}
       |""".stripMargin
  ) {
    run().hasError(
      getGql42001_42N07("x", 89 + keyword.length, 7, 15),
      "The variable `x` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      InputPosition(89 + keyword.length, 7, 15)
    )
  }

  test(
    s"""WITH 1 AS x, 2 AS y
       |RETURN $keyword {
       |   CALL {
       |     WITH y
       |     WITH y, 3 AS x
       |     MATCH (n) WHERE n.prop = x
       |     RETURN 1 AS a
       |   }
       |   RETURN a
       |}
       |""".stripMargin
  ) {
    run().hasNoErrors
  }

  test(
    s"""WITH 5 AS y
       |RETURN $keyword {
       |    UNWIND [0, 1, 2] AS x
       |    CALL {
       |        WITH x
       |        RETURN x * 10 AS y
       |    }
       |    RETURN y
       |}
       |""".stripMargin
  ) {
    run().hasErrors(
      getGql42001_42N07("y", 99 + keyword.length, 6, 26),
      "The variable `y` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      InputPosition(99 + keyword.length, 6, 26),
      getGql42001_42N07("y", 82 + keyword.length, 6, 9),
      "The variable `y` is shadowing a variable with the same name from the outer scope and needs to be renamed",
      InputPosition(82 + keyword.length, 6, 9)
    )
  }
}
