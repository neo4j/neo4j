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
package org.neo4j.cypher.internal.frontend.scoping.checker

import org.neo4j.cypher.internal.frontend.scoping.E42N67
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for
 * - 42N67 - Duplicate parameter
 */
class GQL_42N67_DuplicateParameterTest extends VariableCheckingWithLocalCallablesTestSuite
    with LocalCallableGenHelpers {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] = {
    (for {
      (sig, dupOpt, args) <- Seq(
        // negative test
        ("a, a", Some("a"), "1, 2"),
        ("a :: INT, a :: INT", Some("a"), "1, 2"),
        ("a :: INT, a :: INT = 0", Some("a"), "1"),
        ("a :: INT, a :: INT = null", Some("a"), "1"),
        ("a :: INT NOT NULL, a :: INT", Some("a"), "1, 2"),
        ("a :: INT, a :: STRING", Some("a"), "1, '2'"),
        ("a :: ANY, a :: STRING", Some("a"), "1, '2'"),
        ("a, b, a", Some("a"), "1, 2, 3"),
        ("a, b, c, d, e, f, a, g, h", Some("a"), "1, 2, 3, 4, 5, 6, 7, 8, 9"),
        ("a, b, c, d, e, f, d, g, h", Some("d"), "1, 2, 3, 4, 5, 6, 7, 8, 9"),
        (
          "a :: INT, b :: INT, c :: ANY, d :: LIST<ANY>, e :: INT, f :: INT, d :: LIST<INT>, g :: STRING, h :: STRING",
          Some("d"),
          "1, 2, 3, [4], 5, 6, [7], '8', '9'"
        ),
        // positive test
        ("a, b", None, "1, 2"),
        ("a :: INT, b :: INT", None, "1, 2"),
        ("a :: INT, b :: INT = 0", None, "1"),
        ("a :: INT, b :: INT = null", None, "1"),
        ("a, b, c", None, "1, 2, 3"),
        ("a, b, c, d, e, f, g, h, i", None, "1, 2, 3, 4, 5, 6, 7, 8, 9"),
        (
          "a :: INT, b :: INT, c :: ANY, d :: LIST<ANY>, e :: INT, f :: INT, g :: LIST<INT>, h :: STRING, i :: STRING",
          None,
          "1, 2, 3, [4], 5, 6, [7], '8', '9'"
        )
      )
      (kind, call) <- Seq(
        ("PROCEDURE", s"CALL foo($args)"),
        ("FUNCTION", s"LET a = foo($args)")
      )
      definition =
        s"""DEFINE $kind foo($sig) {
           |  RETURN a LIMIT 1
           |}""".stripMargin
      queryWithDefinition =
        s"""$definition
           |
           |$call
           |RETURN a""".stripMargin
      outcome = dupOpt.map(dup => E42N67(dup)).getOrElse(Passes)
    } yield {
      Seq(
        TestQuery(
          s"""$definition
             |
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq.empty
        ),
        TestQuery(
          queryWithDefinition,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""DEFINE PROCEDURE bar() {
             |  $queryWithDefinition
             |}
             |
             |RETURN 1 AS a""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""DEFINE FUNCTION bar() {
             |  $queryWithDefinition
             |}
             |
             |RETURN 1 AS a""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""USE foo
             |{
             |  $queryWithDefinition
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""{
             |  $queryWithDefinition
             |}
             |NEXT
             |
             |RETURN 123 AS foo""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("foo")
        ),
        TestQuery(
          s"""RETURN 123 AS foo
             |
             |NEXT
             |{
             |  $queryWithDefinition
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""{
             |  $queryWithDefinition
             |}
             |UNION
             |
             |RETURN 123 AS a""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""RETURN 123 AS a
             |
             |UNION
             |{
             |  $queryWithDefinition
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""LET x = 123
             |CALL (x) {
             |  $queryWithDefinition
             |}
             |RETURN a AS x
             |
             |NEXT
             |
             |RETURN x AS foo""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("foo")
        ),
        TestQuery(
          s"""WHEN true THEN {
             |  $queryWithDefinition
             |}
             |ELSE RETURN 1 AS a""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        ),
        TestQuery(
          s"""WHEN true THEN RETURN 1 AS a
             |ELSE {
             |  $queryWithDefinition
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome),
          Seq("a")
        )
      ) ++ (
        for {
          scalarSubquery <- Seq("EXISTS", "COUNT", "COLLECT")
        } yield Seq(
          TestQuery(
            s"""LET x = $scalarSubquery {
               |  $queryWithDefinition
               |}
               |FINISH""".stripMargin,
            ignoreBeforeCypher25(outcome),
            Seq.empty
          ),
          TestQuery(
            s"""RETURN $scalarSubquery {
               |  $queryWithDefinition
               |} AS foo""".stripMargin,
            ignoreBeforeCypher25(outcome),
            Seq("foo")
          ),
          TestQuery(
            s"""CALL proc($scalarSubquery {
               |  $queryWithDefinition
               |})
               |FINISH""".stripMargin,
            ignoreBeforeCypher25(outcome),
            Seq.empty
          )
        )
      ).flatten
    }).flatten
  }
}
