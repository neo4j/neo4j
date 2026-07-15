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

import org.neo4j.cypher.internal.frontend.scoping.E42I77
import org.neo4j.cypher.internal.frontend.scoping.Passes
import org.neo4j.cypher.internal.frontend.scoping.Versioned.ignoreBeforeCypher25

/**
 * Test for 42I77 - Local Callable Already Defined
 */
class GQL_42I77_LocalCallableAlreadyDefinedTest extends VariableCheckingWithLocalCallablesTestSuite
    with LocalCallableGenHelpers {
  VariableCheckingWithLocalCallablesTestSuite.register(() => testCases())

  override def testCases(): Seq[TestQuery] =
    (
      for {
        firstDefinition <- Seq(
          (sig: String) => s"""DEFINE PROCEDURE $sig { RETURN "foo" AS foo }""",
          (sig: String) => s"""DEFINE FUNCTION $sig { RETURN "foo" AS foo }""",
          (sig: String) => s"""DEFINE FUNCTION $sig = "foo""""
        )
        (secondDefinition, kind) <- Seq(
          ((sig: String) => s"""DEFINE PROCEDURE $sig { RETURN "bar" AS foo }""") -> Procedure,
          ((sig: String) => s"""DEFINE FUNCTION $sig { RETURN "bar" AS foo }""") -> Function,
          ((sig: String) => s"""DEFINE FUNCTION $sig = "bar"""") -> Function
        )
        (namePart, outcome) <- Seq(
          // Negative tests
          "foo" -> ((name: String) => E42I77(name)),
          // Positive tests
          "bar" -> ((_: String) => Passes)
        )
        call = (args: String) =>
          kind match {
            case Procedure => s"CALL $namePart($args)"
            case Function  => s"LET foo = $namePart($args)"
          }
      } yield Seq(
        TestQuery(
          s"""${firstDefinition(s"foo()")}
             |${secondDefinition(s"$namePart()")}
             |
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition(s"foo()")}
             |${secondDefinition(s"$namePart()")}
             |
             |${call("")}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition(s"foo.abc.def()")}
             |${secondDefinition(s"$namePart.abc.def()")}
             |
             |${call("")}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo.abc.def")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition(s"abc.def.foo()")}
             |${secondDefinition(s"abc.def.$namePart()")}
             |
             |${call("")}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("abc.def.foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition("foo()")}
             |${secondDefinition(s"$namePart(x)")}
             |
             |${call("'bar'")}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition("foo(y)")}
             |${secondDefinition(s"$namePart(x)")}
             |
             |${call("'bar'")}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition("foo(y :: INT)")}
             |${secondDefinition(s"$namePart(x :: STRING)")}
             |
             |${call("'bar'")}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition("foo()")}
             |${secondDefinition(s"$namePart()")}
             |
             |USE myGraph
             |${call("'bar'")}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""{
             |  ${firstDefinition("foo()")}
             |
             |  FINISH
             |}
             |NEXT
             |{
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  FINISH
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""{
             |  ${firstDefinition("foo()")}
             |
             |  FINISH
             |}
             |NEXT
             |
             |RETURN 1 AS x
             |
             |NEXT
             |
             |{
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  FINISH
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""{
             |  ${firstDefinition("foo()")}
             |
             |  FINISH
             |}
             |NEXT
             |USE myGraph {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  FINISH
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""{
             |  ${firstDefinition("foo()")}
             |
             |  RETURN "foo" AS foo
             |}
             |UNION
             |{
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq("foo")
        ),
        TestQuery(
          s"""${firstDefinition("foo()")}
             |
             |CALL () {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""CALL () {
             |  ${firstDefinition("foo()")}
             |
             |  FINISH
             |}
             |CALL () {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""CALL (*) {
             |  ${firstDefinition("foo()")}
             |
             |  FINISH
             |}
             |CALL (*) {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""CALL {
             |  ${firstDefinition("foo()")}
             |
             |  FINISH
             |}
             |CALL {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}
             |FINISH""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition("foo()")}
             |
             |WHEN true THEN {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}
             |ELSE RETURN 1 AS foo""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""${firstDefinition("foo()")}
             |
             |WHEN true THEN RETURN 1 AS foo
             |ELSE {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""WHEN true THEN {
             |  ${firstDefinition("foo()")}
             |
             |  RETURN 1 AS foo
             |}
             |WHEN false THEN {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}
             |ELSE RETURN 1 AS foo""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        ),
        TestQuery(
          s"""WHEN EXISTS {
             |  ${firstDefinition("foo()")}
             |
             |  FILTER false
             |} THEN RETURN 1 AS foo
             |WHEN false THEN {
             |  ${secondDefinition(s"$namePart()")}
             |
             |  ${call("'bar'")}
             |  RETURN foo
             |}
             |ELSE RETURN 1 AS foo""".stripMargin,
          ignoreBeforeCypher25(outcome("foo")),
          Seq.empty
        )
      ) ++ (
        for {
          scalarSubquery <- Seq("EXISTS", "COUNT", "COLLECT")
        } yield Seq(
          TestQuery(
            s"""LET x = $scalarSubquery {
               |  ${firstDefinition("foo()")}
               |
               |  RETURN "foo"
               |}
               |CALL () {
               |  ${secondDefinition(s"$namePart()")}
               |
               |  ${call("'bar'")}
               |  RETURN foo
               |}
               |FINISH""".stripMargin,
            ignoreBeforeCypher25(outcome("foo")),
            Seq.empty
          ),
          TestQuery(
            s"""${firstDefinition("foo()")}
               |
               |LET x = $scalarSubquery {
               |  ${secondDefinition(s"$namePart()")}
               |
               |  ${call("'bar'")}
               |  RETURN foo
               |}
               |FINISH""".stripMargin,
            ignoreBeforeCypher25(outcome("foo")),
            Seq.empty
          ),
          TestQuery(
            s"""LET x = $scalarSubquery {
               |  ${firstDefinition("foo()")}
               |
               |  RETURN "foo"
               |}
               |LET y = $scalarSubquery {
               |  ${secondDefinition(s"$namePart()")}
               |
               |  ${call("'bar'")}
               |  RETURN foo
               |}
               |FINISH""".stripMargin,
            ignoreBeforeCypher25(outcome("foo")),
            Seq.empty
          ),
          TestQuery(
            s"""CALL proc(EXISTS {
               |  ${firstDefinition("foo()")}
               |
               |  RETURN "foo"
               |}, $scalarSubquery {
               |  ${secondDefinition(s"$namePart()")}
               |
               |  ${call("'bar'")}
               |  RETURN foo
               |})
               |FINISH""".stripMargin,
            ignoreBeforeCypher25(outcome("foo")),
            Seq.empty
          )
        )
      ).flatten
    ).flatten
}
