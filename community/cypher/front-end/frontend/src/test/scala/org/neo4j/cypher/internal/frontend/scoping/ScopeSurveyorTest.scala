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
package org.neo4j.cypher.internal.frontend.scoping

import org.neo4j.cypher.internal.CypherVersion

class ScopeSurveyorTest extends VariableCheckingTestSuite {

  test("RETURN 1") {
    hasScope(
      ExpectedWorkingScope(
        Ast("RETURN 1"), // query level
        Outgoing(variables = Set("1")),
        ExpectedResult.TableResult("1"),
        ExpectedWorkingScope(
          Ast("RETURN 1"),
          Outgoing(variables = Set("1")),
          ExpectedResult.TableResult("1"),
          ExpectedWorkingScope.constExp("1")
        )
      )
    )
  }

  test("FINISH") {
    hasScope(
      ExpectedWorkingScope(
        Ast("FINISH"), // query level
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("FINISH"),
          ExpectedResult.OmittedResult
        )
      )
    )
  }

  test("""{
         |  RETURN 1
         |}""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("{ RETURN 1 }"), // query level
        Outgoing(variables = Set("1")),
        ExpectedResult.TableResult("1"),
        ExpectedWorkingScope(
          Ast("RETURN 1"), // query level
          Outgoing(variables = Set("1")),
          ExpectedResult.TableResult("1"),
          ExpectedWorkingScope(
            Ast("RETURN 1"),
            Outgoing(variables = Set("1")),
            ExpectedResult.TableResult("1"),
            ExpectedWorkingScope.constExp("1")
          )
        )
      )
    )
  }

  test("""RETURN 1 AS a
         |
         |NEXT
         |
         |RETURN a + 1 AS b""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""RETURN 1 AS a
              |
              |NEXT
              |
              |RETURN a + 1 AS b""".stripMargin),
        Outgoing(variables = Set("b")),
        ExpectedResult.TableResult("b"),
        ExpectedWorkingScope(
          Ast("RETURN 1 AS a"), // query level
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("RETURN 1 AS a"),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope.constExp("1")
          )
        ),
        ExpectedWorkingScope(
          Ast("YIELD a AS a"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a + 1 AS b"), // query level
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("b")),
          ExpectedResult.TableResult("b"),
          ExpectedWorkingScope(
            Ast("RETURN a + 1 AS b"),
            Incoming(variables = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("b")),
            ExpectedResult.TableResult("b"),
            ExpectedWorkingScope(
              Ast("a + 1"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedResult.ExpressionResult,
              ExpectedWorkingScope.varExp("a", Set("a"))
            )
          )
        )
      )
    )
  }

  test("""WITH 1 AS x
         |LET y = x + 1
         |RETURN x, y""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x
              |LET y = x + 1
              |RETURN x, y""".stripMargin),
        Outgoing(variables = Set("x", "y")),
        ExpectedResult.TableResult("x", "y"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("LET y = x + 1"),
          Incoming(variables = Set("x")),
          Declared(variables = Seq("y")),
          Referenced(Set("x")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope(
            Ast("x + 1"),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            ExpectedResult.ExpressionResult,
            ExpectedWorkingScope.varExp("x", Set("x"))
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y"),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedResult.TableResult("x", "y"),
          ExpectedWorkingScope.varExp("x", Set("x", "y")),
          ExpectedWorkingScope.varExp("y", Set("x", "y"))
        )
      )
    )
  }

  test("""WITH 1 AS x
         |CALL (*) {
         |  WITH *, 2 AS y
         |  RETURN y
         |}
         |RETURN x, y""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x
              |CALL (*) {
              |  WITH *, 2 AS y
              |  RETURN y
              |}
              |RETURN x, y""".stripMargin),
        Outgoing(variables = Set("x", "y")),
        ExpectedResult.TableResult("x", "y"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (*) {
                |  WITH *, 2 AS y
                |  RETURN y
                |}""".stripMargin),
          Incoming(variables = Set("x")),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope(
            Ast("""WITH *, 2 AS y
                  |RETURN y""".stripMargin),
            Incoming(constants = Set("x")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope(
              Ast("""WITH *, 2 AS y""".stripMargin),
              Incoming(constants = Set("x")),
              Declared(variables = Seq("y")),
              Outgoing(constants = Set("x"), variables = Set("y")),
              ExpectedWorkingScope.constExp("2", Set("x"))
            ),
            ExpectedWorkingScope(
              Ast("""RETURN y""".stripMargin),
              Incoming(constants = Set("x"), variables = Set("y")),
              Referenced(Set("y")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              ExpectedWorkingScope.varExp("y", Set("x", "y"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y"),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedResult.TableResult("x", "y"),
          ExpectedWorkingScope.varExp("x", Set("x", "y")),
          ExpectedWorkingScope.varExp("y", Set("x", "y"))
        )
      )
    )
  }

  test("""WITH 1 AS x
         |CALL (*) {
         |  WITH DISTINCT *
         |  WITH *, 2 AS y
         |  RETURN y
         |}
         |RETURN x, y""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x
              |CALL (*) {
              |  WITH DISTINCT *
              |  WITH *, 2 AS y
              |  RETURN y
              |}
              |RETURN x, y""".stripMargin),
        Outgoing(variables = Set("x", "y")),
        ExpectedResult.TableResult("x", "y"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (*) {
                |  WITH DISTINCT *
                |  WITH *, 2 AS y
                |  RETURN y
                |}""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope(
            Ast("""WITH DISTINCT *
                  |WITH *, 2 AS y
                  |RETURN y""".stripMargin),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope(
              Ast("""WITH DISTINCT *""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              Outgoing(constants = Set("x"))
            ),
            ExpectedWorkingScope(
              Ast("""WITH *, 2 AS y""".stripMargin),
              Incoming(constants = Set("x")),
              Declared(variables = Seq("y")),
              Outgoing(constants = Set("x"), variables = Set("y")),
              ExpectedWorkingScope.constExp("2", Set("x"))
            ),
            ExpectedWorkingScope(
              Ast("""RETURN y""".stripMargin),
              Incoming(constants = Set("x"), variables = Set("y")),
              Referenced(Set("y")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              ExpectedWorkingScope.varExp("y", Set("x", "y"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y"),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedResult.TableResult("x", "y"),
          ExpectedWorkingScope.varExp("x", Set("x", "y")),
          ExpectedWorkingScope.varExp("y", Set("x", "y"))
        )
      )
    )
  }

  test("""WITH 1 AS x
         |CALL (*) {
         |  WITH *, count(*) AS cnt
         |  WITH *, 2 AS y
         |  RETURN y
         |}
         |RETURN x, y""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x
              |CALL (*) {
              |  WITH *, count(*) AS cnt
              |  WITH *, 2 AS y
              |  RETURN y
              |}
              |RETURN x, y""".stripMargin),
        Outgoing(variables = Set("x", "y")),
        ExpectedResult.TableResult("x", "y"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (*) {
                |  WITH *, count(*) AS cnt
                |  WITH *, 2 AS y
                |  RETURN y
                |}""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope(
            Ast("""WITH *, count(*) AS cnt
                  |WITH *, 2 AS y
                  |RETURN y""".stripMargin),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope(
              Ast("""WITH *, count(*) AS cnt""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              Declared(variables = Seq("cnt")),
              Outgoing(constants = Set("x"), variables = Set("cnt")),
              ExpectedWorkingScope(
                Ast("count(*)"),
                ProjectionIncoming(constants = Set("x"))
              )
            ),
            ExpectedWorkingScope(
              Ast("""WITH *, 2 AS y""".stripMargin),
              Incoming(constants = Set("x"), variables = Set("cnt")),
              Declared(variables = Seq("y")),
              Outgoing(constants = Set("x"), variables = Set("y", "cnt")),
              ExpectedWorkingScope.constExp("2", Set("x", "cnt"))
            ),
            ExpectedWorkingScope(
              Ast("""RETURN y""".stripMargin),
              Incoming(constants = Set("x"), variables = Set("y", "cnt")),
              Referenced(Set("y")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              ExpectedWorkingScope.varExp("y", Set("x", "cnt", "y"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y"),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedResult.TableResult("x", "y"),
          ExpectedWorkingScope.varExp("x", Set("x", "y")),
          ExpectedWorkingScope.varExp("y", Set("x", "y"))
        )
      )
    )
  }

  test("""WITH 1 AS x
         |CALL (*) {
         |  RETURN 2 AS y
         |}
         |RETURN x, y""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x
              |CALL (*) {
              |  RETURN 2 AS y
              |}
              |RETURN x, y""".stripMargin),
        Outgoing(variables = Set("x", "y")),
        ExpectedResult.TableResult("x", "y"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (*) {
                |  RETURN 2 AS y
                |}""".stripMargin),
          Incoming(variables = Set("x")),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope(
            Ast("""RETURN 2 AS y""".stripMargin),
            Incoming(constants = Set("x")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope(
              Ast("""RETURN 2 AS y""".stripMargin),
              Incoming(constants = Set("x")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              ExpectedWorkingScope.constExp("2", Set("x"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y"),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedResult.TableResult("x", "y"),
          ExpectedWorkingScope.varExp("x", Set("x", "y")),
          ExpectedWorkingScope.varExp("y", Set("x", "y"))
        )
      )
    )
  }

  test("""RETURN 1 AS a
         |
         |NEXT
         |
         |RETURN a + 1 AS b
         |
         |NEXT
         |
         |RETURN 1 * b AS b""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""RETURN 1 AS a
              |
              |NEXT
              |
              |RETURN a + 1 AS b
              |
              |NEXT
              |
              |RETURN 1 * b AS b""".stripMargin),
        Outgoing(variables = Set("b")),
        ExpectedResult.TableResult("b"),
        ExpectedWorkingScope(
          Ast("RETURN 1 AS a"), // query level
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("RETURN 1 AS a"),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope.constExp("1")
          )
        ),
        ExpectedWorkingScope(
          Ast("YIELD a AS a"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a + 1 AS b"), // query level
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("b")),
          ExpectedResult.TableResult("b"),
          ExpectedWorkingScope(
            Ast("RETURN a + 1 AS b"),
            Incoming(variables = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("b")),
            ExpectedResult.TableResult("b"),
            ExpectedWorkingScope(
              Ast("a + 1"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedResult.ExpressionResult,
              ExpectedWorkingScope.varExp("a", Set("a"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("YIELD b AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("b"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN 1 * b AS b"), // query level
          Incoming(variables = Set("b")),
          Referenced(Set("b")),
          Outgoing(variables = Set("b")),
          ExpectedResult.TableResult("b"),
          ExpectedWorkingScope(
            Ast("RETURN 1 * b AS b"),
            Incoming(variables = Set("b")),
            Referenced(Set("b")),
            Outgoing(variables = Set("b")),
            ExpectedResult.TableResult("b"),
            ExpectedWorkingScope(
              Ast("1 * b"),
              Incoming(constants = Set("b")),
              Referenced(Set("b")),
              ExpectedResult.ExpressionResult,
              ExpectedWorkingScope.varExp("b", Set("b"))
            )
          )
        )
      )
    )
  }

  test("""LET x = 1
         |CALL (x) {
         |  RETURN x AS a
         |
         |  NEXT
         |
         |  RETURN x + 1 AS b,  a AS c
         |}
         |RETURN b, c""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET x = 1
              |CALL (x) {
              |  RETURN x AS a
              |
              |  NEXT
              |
              |  RETURN x + 1 AS b, a AS c
              |}
              |RETURN b, c""".stripMargin),
        Outgoing(variables = Set("b", "c")),
        ExpectedResult.TableResult("b", "c"),
        ExpectedWorkingScope(
          Ast("LET x = 1"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (x) {
                |  RETURN x AS a
                |
                |  NEXT
                |
                |  RETURN x + 1 AS b, a AS c
                |}""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("b", "c")),
          Outgoing(variables = Set("x", "b", "c")),
          ExpectedWorkingScope(
            Ast("""RETURN x AS a
                  |
                  |NEXT
                  |
                  |RETURN x + 1 AS b, a AS c""".stripMargin),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            Outgoing(variables = Set("b", "c")),
            ExpectedResult.TableResult("b", "c"),
            ExpectedWorkingScope(
              Ast("RETURN x AS a"), // query level
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              Outgoing(variables = Set("a")),
              ExpectedResult.TableResult("a"),
              ExpectedWorkingScope(
                Ast("RETURN x AS a"),
                Incoming(constants = Set("x")),
                Referenced(Set("x")),
                Outgoing(variables = Set("a")),
                ExpectedResult.TableResult("a"),
                ExpectedWorkingScope.varExp("x", Set("x"))
              )
            ),
            ExpectedWorkingScope(
              Ast("YIELD a AS a"),
              Incoming(constants = Set("x")),
              Declared(variables = Seq("a")),
              Outgoing(constants = Set("x"), variables = Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("RETURN x + 1 AS b, a AS c"), // query level
              Incoming(constants = Set("x"), variables = Set("a")),
              Referenced(Set("x", "a")),
              Outgoing(variables = Set("b", "c")),
              ExpectedResult.TableResult("b", "c"),
              ExpectedWorkingScope(
                Ast("RETURN x + 1 AS b, a AS c"),
                Incoming(constants = Set("x"), variables = Set("a")),
                Referenced(Set("x", "a")),
                Outgoing(variables = Set("b", "c")),
                ExpectedResult.TableResult("b", "c"),
                ExpectedWorkingScope(
                  Ast("x + 1"),
                  Incoming(constants = Set("x", "a")),
                  Referenced(Set("x")),
                  ExpectedResult.ExpressionResult,
                  ExpectedWorkingScope.varExp("x", Set("x", "a"))
                ),
                ExpectedWorkingScope.varExp("a", Set("x", "a"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN b, c"),
          Incoming(variables = Set("x", "b", "c")),
          Referenced(Set("b", "c")),
          Outgoing(variables = Set("b", "c")),
          ExpectedResult.TableResult("b", "c"),
          ExpectedWorkingScope.varExp("b", Set("x", "b", "c")),
          ExpectedWorkingScope.varExp("c", Set("x", "b", "c"))
        )
      )
    )
  }

  test("""RETURN 1 AS a
         |UNION
         |RETURN 2 AS a""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""RETURN 1 AS a
              |UNION
              |RETURN 2 AS a""".stripMargin),
        Declared(variables = Seq("a")),
        Outgoing(variables = Set("a")),
        ExpectedResult.TableResult("a"),
        ExpectedWorkingScope(
          Ast("RETURN 1 AS a"), // query level
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("RETURN 1 AS a"),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope.constExp("1")
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN 2 AS a"), // query level
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("RETURN 2 AS a"),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope.constExp("2")
          )
        )
      )
    )
  }

  test("""RETURN 1 AS a
         |UNION
         |RETURN 2 AS b""".stripMargin) {

    /**
     * This is an example of deliberately invalid Cypher to document how variable scoping deals with it.
     * Currently the scope of union adopts outgoing and result from the last child.
     * Alternative treatments are imaginable.
     * If an alternative is picked up, this test will probably need adjustment.
     */
    hasScope(
      ExpectedWorkingScope(
        Ast("""RETURN 1 AS a
              |UNION
              |RETURN 2 AS b""".stripMargin),
        Outgoing(variables = Set("a")),
        ExpectedResult.TableResult("a"),
        ExpectedWorkingScope(
          Ast("RETURN 1 AS a"), // query level
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("RETURN 1 AS a"),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope.constExp("1")
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN 2 AS b"), // query level
          Outgoing(variables = Set("b")),
          ExpectedResult.TableResult("b"),
          ExpectedWorkingScope(
            Ast("RETURN 2 AS b"),
            Outgoing(variables = Set("b")),
            ExpectedResult.TableResult("b"),
            ExpectedWorkingScope.constExp("2")
          )
        )
      ),
      skipVariableChecker = true
    )
  }

  test("""RETURN 1 AS a
         |
         |NEXT
         |{
         |  RETURN a + 1 AS a
         |  UNION
         |  RETURN a + 2 AS a
         |}""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""RETURN 1 AS a
              |
              |NEXT
              |{
              |  RETURN a + 1 AS a
              |  UNION
              |  RETURN a + 2 AS a
              |}""".stripMargin),
        Outgoing(variables = Set("a")),
        ExpectedResult.TableResult("a"),
        ExpectedWorkingScope(
          Ast("RETURN 1 AS a"), // query level
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("RETURN 1 AS a"),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope.constExp("1")
          )
        ),
        ExpectedWorkingScope(
          Ast("YIELD a AS a"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""{
                |  RETURN a + 1 AS a
                |  UNION
                |  RETURN a + 2 AS a
                |}""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("""RETURN a + 1 AS a
                  |UNION
                  |RETURN a + 2 AS a""".stripMargin),
            Incoming(variables = Set("a")),
            Referenced(Set("a")),
            // UnmappedUnion now declares its `unionVariable` at the union scope.
            Declared(variables = Seq("a")),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope(
              Ast("RETURN a + 1 AS a"), // query level
              Incoming(variables = Set("a")),
              Referenced(Set("a")),
              Outgoing(variables = Set("a")),
              ExpectedResult.TableResult("a"),
              ExpectedWorkingScope(
                Ast("RETURN a + 1 AS a"),
                Incoming(variables = Set("a")),
                Referenced(Set("a")),
                Outgoing(variables = Set("a")),
                ExpectedResult.TableResult("a"),
                ExpectedWorkingScope(
                  Ast("a + 1"),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  ExpectedResult.ExpressionResult,
                  ExpectedWorkingScope.varExp("a", Set("a"))
                )
              )
            ),
            ExpectedWorkingScope(
              Ast("RETURN a + 2 AS a"), // query level
              Incoming(variables = Set("a")),
              Referenced(Set("a")),
              Outgoing(variables = Set("a")),
              ExpectedResult.TableResult("a"),
              ExpectedWorkingScope(
                Ast("RETURN a + 2 AS a"),
                Incoming(variables = Set("a")),
                Referenced(Set("a")),
                Outgoing(variables = Set("a")),
                ExpectedResult.TableResult("a"),
                ExpectedWorkingScope(
                  Ast("a + 2"),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  ExpectedResult.ExpressionResult,
                  ExpectedWorkingScope.varExp("a", Set("a"))
                )
              )
            )
          )
        )
      )
    )
  }

  test("""WHEN 1 = $p THEN RETURN 1 AS x
         |ELSE RETURN 2 AS x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WHEN 1 = $p THEN RETURN 1 AS x
              |ELSE RETURN 2 AS x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("WHEN 1 = $p THEN RETURN 1 AS x"),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope.constExp("1 = $p"),
          ExpectedWorkingScope(
            Ast("RETURN 1 AS x"), // query level
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("RETURN 1 AS x"),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope.constExp("1")
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("ELSE RETURN 2 AS x"),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("RETURN 2 AS x"), // query level
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("RETURN 2 AS x"),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope.constExp("2")
            )
          )
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN x AS y
         |NEXT
         |WHEN y % 2 = 0 THEN RETURN y * -1 AS x
         |ELSE RETURN y AS x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN x AS y
              |NEXT
              |WHEN y % 2 = 0 THEN RETURN y * -1 AS x
              |ELSE RETURN y AS x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("""UNWIND [1, 2, 3] AS x
                |RETURN x AS y""".stripMargin),
          Outgoing(variables = Set("y")),
          ExpectedResult.TableResult("y"),
          ExpectedWorkingScope(
            Ast("UNWIND [1, 2, 3] AS x"),
            Declared(variables = Seq("x")),
            Outgoing(variables = Set("x")),
            ExpectedWorkingScope.constExp("[1, 2, 3]")
          ),
          ExpectedWorkingScope(
            Ast("RETURN x AS y"),
            Incoming(variables = Set("x")),
            Referenced(Set("x")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope.varExp("x", Set("x"))
          )
        ),
        ExpectedWorkingScope(
          Ast("YIELD y AS y"),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("y"))
        ),
        ExpectedWorkingScope(
          Ast(
            """WHEN y % 2 = 0 THEN RETURN y * -1 AS x
              |ELSE RETURN y AS x""".stripMargin
          ),
          Incoming(variables = Set("y")),
          Referenced(Set("y")),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("WHEN y % 2 = 0 THEN RETURN y * -1 AS x"),
            Incoming(constants = Set("y")),
            Referenced(Set("y")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("y % 2 = 0"),
              Incoming(constants = Set("y")),
              Referenced(Set("y")),
              ExpectedWorkingScope.varExp("y", Set("y"))
            ),
            ExpectedWorkingScope(
              Ast("RETURN y * -1 AS x"), // query level
              Incoming(constants = Set("y")),
              Referenced(Set("y")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("RETURN y * -1 AS x"),
                Incoming(constants = Set("y")),
                Referenced(Set("y")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("y * -1"),
                  Incoming(constants = Set("y")),
                  Referenced(Set("y")),
                  ExpectedWorkingScope.varExp("y", Set("y"))
                )
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("ELSE RETURN y AS x"),
            Incoming(constants = Set("y")),
            Referenced(Set("y")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("RETURN y AS x"), // query level
              Incoming(constants = Set("y")),
              Referenced(Set("y")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("RETURN y AS x"),
                Incoming(constants = Set("y")),
                Referenced(Set("y")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope.varExp("y", Set("y"))
              )
            )
          )
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN x AS y
         |NEXT
         |WHEN y % 2 = 0 THEN {
         |  LET x = -1
         |  RETURN y * x AS x
         |}
         |ELSE RETURN y AS x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN x AS y
              |NEXT
              |WHEN y % 2 = 0 THEN {
              |  LET x = -1
              |  RETURN y * x AS x
              |}
              |ELSE RETURN y AS x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("""UNWIND [1, 2, 3] AS x
                |RETURN x AS y""".stripMargin),
          Outgoing(variables = Set("y")),
          ExpectedResult.TableResult("y"),
          ExpectedWorkingScope(
            Ast("UNWIND [1, 2, 3] AS x"),
            Declared(variables = Seq("x")),
            Outgoing(variables = Set("x")),
            ExpectedWorkingScope.constExp("[1, 2, 3]")
          ),
          ExpectedWorkingScope(
            Ast("RETURN x AS y"),
            Incoming(variables = Set("x")),
            Referenced(Set("x")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope.varExp("x", Set("x"))
          )
        ),
        ExpectedWorkingScope(
          Ast("YIELD y AS y"),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("y"))
        ),
        ExpectedWorkingScope(
          Ast(
            """WHEN y % 2 = 0 THEN {
              |  LET x = -1
              |  RETURN y * x AS x
              |}
              |ELSE RETURN y AS x""".stripMargin
          ),
          Incoming(variables = Set("y")),
          Referenced(Set("y")),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("""WHEN y % 2 = 0 THEN {
                  |  LET x = -1
                  |  RETURN y * x AS x
                  |}""".stripMargin),
            Incoming(constants = Set("y")),
            Referenced(Set("y")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("y % 2 = 0"),
              Incoming(constants = Set("y")),
              Referenced(Set("y")),
              ExpectedWorkingScope.varExp("y", Set("y"))
            ),
            ExpectedWorkingScope(
              Ast("""{
                    |  LET x = -1
                    |  RETURN y * x AS x
                    |}""".stripMargin),
              Incoming(constants = Set("y")),
              Referenced(Set("y")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("""LET x = -1
                      |RETURN y * x AS x""".stripMargin), // query level
                Incoming(constants = Set("y")),
                Referenced(Set("y")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("LET x = -1"),
                  Incoming(constants = Set("y")),
                  Declared(variables = Seq("x")),
                  Outgoing(constants = Set("y"), variables = Set("x")),
                  ExpectedWorkingScope.constExp("-1", Set("y"))
                ),
                ExpectedWorkingScope(
                  Ast("RETURN y * x AS x"),
                  Incoming(constants = Set("y"), variables = Set("x")),
                  Referenced(Set("y", "x")),
                  Outgoing(variables = Set("x")),
                  ExpectedResult.TableResult("x"),
                  ExpectedWorkingScope(
                    Ast("y * x"),
                    Incoming(constants = Set("y", "x")),
                    Referenced(Set("y", "x")),
                    ExpectedWorkingScope.varExp("y", Set("y", "x")),
                    ExpectedWorkingScope.varExp("x", Set("y", "x"))
                  )
                )
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("ELSE RETURN y AS x"),
            Incoming(constants = Set("y")),
            Referenced(Set("y")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("RETURN y AS x"), // query level
              Incoming(constants = Set("y")),
              Referenced(Set("y")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("RETURN y AS x"),
                Incoming(constants = Set("y")),
                Referenced(Set("y")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope.varExp("y", Set("y"))
              )
            )
          )
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("RETURN x"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope.varExp("x", Set("x"))
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN x * x AS x2 ORDER BY x2 ASCENDING""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN x * x AS x2 ORDER BY x2 ASCENDING""".stripMargin),
        Outgoing(variables = Set("x2")),
        ExpectedResult.TableResult("x2"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("RETURN x * x AS x2 ORDER BY x2 ASCENDING"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("x2")),
          ExpectedResult.TableResult("x2"),
          ExpectedWorkingScope(
            Ast("x * x"),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x"))
          ),
          ExpectedWorkingScope.varProjExp("x2", Set("x", "x2"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS x
         |RETURN x * -1 AS a ORDER BY a ASCENDING""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS x
              |RETURN x * -1 AS a ORDER BY a ASCENDING""".stripMargin),
        Outgoing(variables = Set("a")),
        ExpectedResult.TableResult("a"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN x * -1 AS a ORDER BY a ASCENDING"),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope(
            Ast("x * -1"),
            Incoming(constants = Set("a", "x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("a", "x"))
          ),
          ExpectedWorkingScope.varProjExp("a", Set("a", "x"))
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |WITH x * x AS x2 ORDER BY x2 ASCENDING
         |RETURN x2""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |WITH x * x AS x2 ORDER BY x2 ASCENDING
              |RETURN x2""".stripMargin),
        Outgoing(variables = Set("x2")),
        ExpectedResult.TableResult("x2"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("WITH x * x AS x2 ORDER BY x2 ASCENDING"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("x2")),
          Outgoing(variables = Set("x2")),
          ExpectedWorkingScope(
            Ast("x * x"),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x"))
          ),
          ExpectedWorkingScope.varProjExp("x2", Set("x", "x2"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN x2"),
          Incoming(variables = Set("x2")),
          Referenced(Set("x2")),
          Outgoing(variables = Set("x2")),
          ExpectedResult.TableResult("x2"),
          ExpectedWorkingScope.varExp("x2", Set("x2"))
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |WITH x * x AS x2 WHERE x2 > 5
         |RETURN x2""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |WITH x * x AS x2 WHERE x2 > 5
              |RETURN x2""".stripMargin),
        Outgoing(variables = Set("x2")),
        ExpectedResult.TableResult("x2"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("WITH x * x AS x2 WHERE x2 > 5"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("x2")),
          Outgoing(variables = Set("x2")),
          ExpectedWorkingScope(
            Ast("x * x"),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x"))
          ),
          ExpectedWorkingScope(
            Ast("x2 > 5"),
            ProjectionIncoming(constants = Set("x", "x2")),
            Referenced(Set("x2")),
            ExpectedWorkingScope.varProjExp("x2", Set("x", "x2"))
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x2"),
          Incoming(variables = Set("x2")),
          Referenced(Set("x2")),
          Outgoing(variables = Set("x2")),
          ExpectedResult.TableResult("x2"),
          ExpectedWorkingScope.varExp("x2", Set("x2"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS b
         |RETURN a * b AS x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS b
              |RETURN a * b AS x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a * b AS x"),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a", "b")),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("a * b"),
            Incoming(constants = Set("a", "b")),
            Referenced(Set("a", "b")),
            ExpectedWorkingScope.varExp("a", Set("a", "b")),
            ExpectedWorkingScope.varExp("b", Set("a", "b"))
          )
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |LET a = 10, b = x * 2
         |RETURN round(toFloat(a) / b, x, "UP") AS r""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |LET a = 10, b = x * 2
              |RETURN round(toFloat(a) / b, x, "UP") AS r""".stripMargin),
        Outgoing(variables = Set("r")),
        ExpectedResult.TableResult("r"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("LET a = 10, b = x * 2"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("a", "b")),
          Outgoing(variables = Set("x", "a", "b")),
          ExpectedWorkingScope.constExp("10", Set("x")),
          ExpectedWorkingScope(
            Ast("x * 2"),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x"))
          )
        ),
        ExpectedWorkingScope(
          Ast("""RETURN round(toFloat(a) / b, x, "UP") AS r"""),
          Incoming(variables = Set("x", "a", "b")),
          Referenced(Set("x", "a", "b")),
          Outgoing(variables = Set("r")),
          ExpectedResult.TableResult("r"),
          ExpectedWorkingScope(
            Ast("""round(toFloat(a) / b, x, "UP")"""),
            Incoming(constants = Set("x", "a", "b")),
            Referenced(Set("x", "a", "b")),
            ExpectedWorkingScope(
              Ast("toFloat(a) / b"),
              Incoming(constants = Set("x", "a", "b")),
              Referenced(Set("a", "b")),
              ExpectedWorkingScope(
                Ast("toFloat(a)"),
                Incoming(constants = Set("x", "a", "b")),
                Referenced(Set("a")),
                ExpectedWorkingScope.varExp("a", Set("x", "a", "b"))
              ),
              ExpectedWorkingScope.varExp("b", Set("x", "a", "b"))
            ),
            ExpectedWorkingScope.varExp("x", Set("x", "a", "b")),
            ExpectedWorkingScope.constExp("\"UP\"", Set("x", "a", "b"))
          )
        )
      )
    )
  }

  test("""LET x = 10, y = 20
         |LET m = {a: 10 + x, b: {y: 1, x: y + x}}
         |RETURN m{.a, by: (m.b).y, bx: y + (m.b).x * m.a} AS r""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET x = 10, y = 20
              |LET m = {a: 10 + x, b: {y: 1, x: y + x}}
              |RETURN m{.a, by: (m.b).y, bx: y + (m.b).x * m.a} AS r""".stripMargin),
        Outgoing(variables = Set("r")),
        ExpectedResult.TableResult("r"),
        ExpectedWorkingScope(
          Ast("LET x = 10, y = 20"),
          Declared(variables = Seq("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope.constExp("10"),
          ExpectedWorkingScope.constExp("20")
        ),
        ExpectedWorkingScope(
          Ast("LET m = {a: 10 + x, b: {y: 1, x: y + x}}"),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Declared(variables = Seq("m")),
          Outgoing(variables = Set("x", "y", "m")),
          ExpectedWorkingScope(
            Ast("{a: 10 + x, b: {y: 1, x: y + x}}"),
            Incoming(constants = Set("x", "y")),
            Referenced(Set("x", "y")),
            ExpectedWorkingScope.varExp("x", Set("x", "y")),
            ExpectedWorkingScope.varExp("y", Set("x", "y")),
            ExpectedWorkingScope.varExp("x", Set("x", "y"))
          )
        ),
        ExpectedWorkingScope(
          Ast("""RETURN m{.a, by: (m.b).y, bx: y + (m.b).x * m.a} AS r"""),
          Incoming(variables = Set("x", "y", "m")),
          Referenced(Set("y", "m")),
          Outgoing(variables = Set("r")),
          ExpectedResult.TableResult("r"),
          ExpectedWorkingScope(
            Ast("""m{.a, by: (m.b).y, bx: y + (m.b).x * m.a}"""),
            Incoming(constants = Set("x", "y", "m")),
            Referenced(Set("y", "m")),
            ExpectedWorkingScope.varExp("m", Set("x", "y", "m")),
            ExpectedWorkingScope.varExp("m", Set("x", "y", "m")),
            ExpectedWorkingScope.varExp("y", Set("x", "y", "m")),
            ExpectedWorkingScope.varExp("m", Set("x", "y", "m")),
            ExpectedWorkingScope.varExp("m", Set("x", "y", "m"))
          )
        )
      )
    )
  }

  test("""LET a = 2
         |UNWIND [1, 2, 3] AS b
         |FILTER WHERE b % a = 1
         |RETURN a * b AS x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 2
              |UNWIND [1, 2, 3] AS b
              |FILTER WHERE b % a = 1
              |RETURN a * b AS x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("LET a = 2"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("2")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("FILTER WHERE b % a = 1"),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("b", "a")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope(
            Ast("b % a = 1"),
            ProjectionIncoming(constants = Set("a", "b")),
            Referenced(Set("b", "a")),
            ExpectedWorkingScope.varProjExp("b", Set("a", "b")),
            ExpectedWorkingScope.varProjExp("a", Set("a", "b"))
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a * b AS x"),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a", "b")),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("a * b"),
            Incoming(constants = Set("a", "b")),
            Referenced(Set("a", "b")),
            ExpectedWorkingScope.varExp("a", Set("a", "b")),
            ExpectedWorkingScope.varExp("b", Set("a", "b"))
          )
        )
      )
    )
  }

  test("""LET a = 10
         |WITH *, 1 AS b
         |RETURN *""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |WITH *, 1 AS b
              |RETURN *""".stripMargin),
        Outgoing(variables = Set("a", "b")),
        ExpectedResult.TableResult("a", "b"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("WITH *, 1 AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("1", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN *"),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a", "b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedResult.TableResult("a", "b")
        )
      )
    )
  }

  test("""LET a = 10
         |WITH a + 10 AS a
         |RETURN *""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |WITH a + 10 AS a
              |RETURN *""".stripMargin),
        Outgoing(variables = Set("a")),
        ExpectedResult.TableResult("a"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("WITH a + 10 AS a"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope(
            Ast("a + 10"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope.varExp("a", Set("a"))
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN *"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a")
        )
      )
    )
  }

  test("""LET a = 10
         |RETURN COLLECT { RETURN a }""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |RETURN COLLECT { RETURN a }""".stripMargin),
        Outgoing(variables = Set("COLLECT { RETURN a }")),
        ExpectedResult.TableResult("COLLECT { RETURN a }"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("RETURN COLLECT { RETURN a }"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("COLLECT { RETURN a }")),
          ExpectedResult.TableResult("COLLECT { RETURN a }"),
          ExpectedWorkingScope(
            Ast("COLLECT { RETURN a }"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("RETURN a"), // this is the query level
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              Outgoing(),
              ExpectedResult.TableResult("a"),
              ExpectedWorkingScope(
                Ast("RETURN a"),
                Incoming(constants = Set("a")),
                Referenced(Set("a")),
                Outgoing(),
                ExpectedResult.TableResult("a"),
                ExpectedWorkingScope.varExp("a", Set("a"))
              )
            )
          )
        )
      )
    )
  }

  test("""LET a = 10
         |LET l = [1, a, 3]
         |UNWIND [x IN l + a WHERE x > 2 | a * x] AS b
         |RETURN a * b AS x, l""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |LET l = [1, a, 3]
              |UNWIND [x IN l + a WHERE x > 2 | a * x] AS b
              |RETURN a * b AS x, l""".stripMargin),
        Outgoing(variables = Set("x", "l")),
        ExpectedResult.TableResult("x", "l"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("LET l = [1, a, 3]"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("l")),
          Outgoing(variables = Set("a", "l")),
          ExpectedWorkingScope(
            Ast("[1, a, 3]"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope.varExp("a", Set("a"))
          )
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [x IN l + a WHERE x > 2 | a * x] AS b"),
          Incoming(variables = Set("a", "l")),
          Referenced(Set("a", "l")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "l", "b")),
          ExpectedWorkingScope(
            Ast("[x IN l + a WHERE x > 2 | a * x]"),
            Incoming(constants = Set("a", "l")),
            Referenced(Set("a", "l")),
            ExpectedWorkingScope(
              Ast("l + a"),
              Incoming(constants = Set("a", "l")),
              Referenced(Set("a", "l")),
              ExpectedWorkingScope.varExp("l", Set("a", "l")),
              ExpectedWorkingScope.varExp("a", Set("a", "l"))
            ),
            ExpectedWorkingScope(
              Ast("[x WHERE x > 2 | a * x]"),
              Incoming(constants = Set("a", "l", "x")),
              Declared(constants = Seq("x")),
              Referenced(Set("a")),
              ExpectedWorkingScope(
                Ast("x > 2"),
                Incoming(constants = Set("a", "l", "x")),
                Referenced(Set("x")),
                ExpectedWorkingScope.varExp("x", Set("a", "l", "x"))
              ),
              ExpectedWorkingScope(
                Ast("a * x"),
                Incoming(constants = Set("a", "l", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "l", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "l", "x"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a * b AS x, l"),
          Incoming(variables = Set("a", "l", "b")),
          Referenced(Set("a", "b", "l")),
          Outgoing(variables = Set("x", "l")),
          ExpectedResult.TableResult("x", "l"),
          ExpectedWorkingScope(
            Ast("a * b"),
            Incoming(constants = Set("a", "l", "b")),
            Referenced(Set("a", "b")),
            ExpectedWorkingScope.varExp("a", Set("a", "l", "b")),
            ExpectedWorkingScope.varExp("b", Set("a", "l", "b"))
          ),
          ExpectedWorkingScope.varExp("l", Set("a", "l", "b"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [a IN [1, a, 3] WHERE a > 2 | a] AS b
         |RETURN b""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [a IN [1, a, 3] WHERE a > 2 | a] AS b
              |RETURN b""".stripMargin),
        Outgoing(variables = Set("b")),
        ExpectedResult.TableResult("b"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [a IN [1, a, 3] WHERE a > 2 | a] AS b"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope(
            Ast("[a IN [1, a, 3] WHERE a > 2 | a]"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("[1, a, 3]"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("[a WHERE a > 2 | a]"),
              Incoming(constants = Set("a")),
              Declared(constants = Seq("a")),
              ExpectedWorkingScope(
                Ast("a > 2"),
                Incoming(constants = Set("a")),
                Referenced(Set("a")),
                ExpectedWorkingScope.varExp("a", Set("a"))
              ),
              ExpectedWorkingScope.varExp("a", Set("a"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN b"),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("b")),
          Outgoing(variables = Set("b")),
          ExpectedResult.TableResult("b"),
          ExpectedWorkingScope.varExp("b", Set("a", "b"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [a IN [1, a, 3] | 1] AS b
         |RETURN b""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [a IN [1, a, 3] | 1] AS b
              |RETURN b""".stripMargin),
        Outgoing(variables = Set("b")),
        ExpectedResult.TableResult("b"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [a IN [1, a, 3] | 1] AS b"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope(
            Ast("[a IN [1, a, 3] | 1]"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("[1, a, 3]"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("[a | 1]"),
              Incoming(constants = Set("a")),
              Declared(constants = Seq("a")),
              ExpectedWorkingScope.constExp("1", Set("a"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN b"),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("b")),
          Outgoing(variables = Set("b")),
          ExpectedResult.TableResult("b"),
          ExpectedWorkingScope.varExp("b", Set("a", "b"))
        )
      )
    )
  }

  test("""LET a = 10
         |FOREACH ( a IN [1, a, 3] |
         |  CREATE (n)
         |  SET n.p = a
         |)
         |RETURN a""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |FOREACH ( a IN [1, a, 3] |
              |  CREATE (n)
              |  SET n.p = a
              |)
              |RETURN a""".stripMargin),
        Outgoing(variables = Set("a")),
        ExpectedResult.TableResult("a"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("""FOREACH ( a IN [1, a, 3] |
                |  CREATE (n)
                |  SET n.p = a
                |)""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(constants = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("[1, a, 3]"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope.varExp("a", Set("a"))
          ),
          ExpectedWorkingScope(
            Ast("""CREATE (n)
                  |SET n.p = a""".stripMargin),
            Incoming(variables = Set("a")),
            Referenced(Set("a")),
            Outgoing(constants = Set(), variables = Set("a", "n")),
            ExpectedResult.OmittedResult,
            ExpectedWorkingScope(
              Ast("CREATE (n)"),
              Incoming(variables = Set("a")),
              Declared(variables = Seq("n")),
              Outgoing(constants = Set(), variables = Set("a", "n")),
              ExpectedResult.OmittedResult,
              ExpectedWorkingScope(
                Ast("(n)"),
                PatternIncoming(topology = Set("a"), predicate = Set("a")),
                Declared(variables = Seq("n")),
                Outgoing(variables = Set("n")),
                ExpectedResult.TableResult("n")
              )
            ),
            ExpectedWorkingScope(
              Ast("SET n.p = a"),
              Incoming(constants = Set(), variables = Set("a", "n")),
              Referenced(Set("a", "n")),
              Outgoing(constants = Set(), variables = Set("a", "n")),
              ExpectedResult.OmittedResult,
              ExpectedWorkingScope(
                Ast("n.p"),
                Incoming(constants = Set("a", "n")),
                Referenced(Set("n")),
                ExpectedWorkingScope.varExp("n", Set("a", "n"))
              ),
              ExpectedWorkingScope.varExp("a", Set("a", "n"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope.varExp("a", Set("a"))
        )
      )
    )
  }

  test("""MATCH (n)
         |CREATE (n)-[r:R]->(n)
         |SET n.p = 1""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (n)
              |CREATE (n)-[r:R]->(n)
              |SET n.p = 1""".stripMargin),
        Outgoing(variables = Set("n", "r")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("MATCH (n)"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope(
            Ast("(n)"),
            PatternIncoming(predicate = Set("n")),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedResult.TableResult("n")
          )
        ),
        ExpectedWorkingScope(
          Ast("CREATE (n)-[r:R]->(n)"),
          Incoming(variables = Set("n")),
          Referenced(Set("n")),
          Declared(variables = Seq("r")),
          Outgoing(variables = Set("n", "r")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("(n)-[r:R]->(n)"),
            PatternIncoming(topology = Set("n"), predicate = Set("n")),
            Referenced(Set("n")),
            Declared(variables = Seq("r")),
            Outgoing(variables = Set("n", "r")),
            ExpectedResult.TableResult("n", "r"),
            ExpectedWorkingScope(
              Ast("(n)"),
              PatternIncoming(topology = Set("n"), predicate = Set("n")),
              Referenced(Set("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n")
            ),
            ExpectedWorkingScope(
              Ast("-[r:R]->"),
              PatternIncoming(topology = Set("n"), predicate = Set("n")),
              Declared(variables = Seq("r")),
              Outgoing(variables = Set("r")),
              ExpectedWorkingScope.constExp("R", Set("n")),
              ExpectedResult.TableResult("r")
            ),
            ExpectedWorkingScope(
              Ast("(n)"),
              PatternIncoming(topology = Set("n", "r"), predicate = Set("n")),
              Referenced(Set("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n")
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("SET n.p = 1"),
          Incoming(variables = Set("n", "r")),
          Referenced(Set("n")),
          Outgoing(variables = Set("n", "r")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("n.p"),
            Incoming(constants = Set("n", "r")),
            Referenced(Set("n")),
            ExpectedWorkingScope.varExp("n", Set("n", "r"))
          ),
          ExpectedWorkingScope.constExp("1", Set("n", "r"))
        )
      )
    )
  }

  test("""MATCH (movie:Movie)
         |  SEARCH movie IN (
         |    VECTOR INDEX moviePlots
         |    FOR [1, 2, 3]
         |    LIMIT 5
         |  ) SCORE AS score
         |RETURN movie.title AS title, score""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (movie:Movie)
              |  SEARCH movie IN (
              |    VECTOR INDEX moviePlots
              |    FOR [1, 2, 3]
              |    LIMIT 5
              |  ) SCORE AS score
              |RETURN movie.title AS title, score""".stripMargin),
        Outgoing(variables = Set("title", "score")),
        ExpectedResult.TableResult("title", "score"),
        ExpectedWorkingScope(
          Ast("""MATCH (movie:Movie)
                |  SEARCH movie IN (
                |    VECTOR INDEX moviePlots
                |    FOR [1, 2, 3]
                |    LIMIT 5
                |  ) SCORE AS score""".stripMargin),
          Outgoing(variables = Set("movie", "score")),
          Declared(variables = Seq("movie")),
          ExpectedWorkingScope(
            Ast("(movie:Movie)"),
            PatternIncoming(Set(), Set("movie"), Set()),
            Declared(variables = Seq("movie")),
            Outgoing(variables = Set("movie")),
            ExpectedResult.TableResult("movie"),
            ExpectedWorkingScope.constExp("Movie", Set("movie"))
          ),
          ExpectedWorkingScope(
            Ast("""SEARCH movie IN (
                  |  VECTOR INDEX moviePlots
                  |  FOR [1, 2, 3]
                  |  LIMIT 5
                  |) SCORE AS score""".stripMargin),
            Incoming(variables = Set("movie")),
            Referenced(Set("movie")),
            Declared(variables = Seq("score")),
            Outgoing(variables = Set("movie", "score")),
            ExpectedWorkingScope.varExp("movie", Set("movie")),
            ExpectedWorkingScope.constExp("[1, 2, 3]", Set("movie")),
            ExpectedWorkingScope.constExp("5", Set("movie"))
          )
        ),
        ExpectedWorkingScope(
          Ast("""RETURN movie.title AS title, score""".stripMargin),
          Incoming(variables = Set("movie", "score")),
          Referenced(Set("movie", "score")),
          Outgoing(variables = Set("title", "score")),
          ExpectedResult.TableResult("title", "score"),
          ExpectedWorkingScope(
            Ast("movie.title"),
            Incoming(constants = Set("movie", "score")),
            Referenced(Set("movie")),
            ExpectedWorkingScope.varExp("movie", Set("movie", "score"))
          ),
          ExpectedWorkingScope.varExp("score", Set("movie", "score"))
        )
      )
    )
  }

  test("""WITH 42 as x
         |MATCH (movie:Movie)
         |  SEARCH movie IN (
         |    VECTOR INDEX moviePlots
         |    FOR [1, 2, 3]
         |    WHERE movie.prop > x
         |    LIMIT 5
         |  ) SCORE AS score
         |RETURN movie.title AS title, score""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 42 AS x
              |MATCH (movie:Movie)
              |  SEARCH movie IN (
              |    VECTOR INDEX moviePlots
              |    FOR [1, 2, 3]
              |    WHERE movie.prop > x
              |    LIMIT 5
              |  ) SCORE AS score
              |RETURN movie.title AS title, score""".stripMargin),
        Outgoing(variables = Set("title", "score")),
        ExpectedResult.TableResult("title", "score"),
        ExpectedWorkingScope(
          Ast("WITH 42 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("42")
        ),
        ExpectedWorkingScope(
          Ast("""MATCH (movie:Movie)
                |  SEARCH movie IN (
                |    VECTOR INDEX moviePlots
                |    FOR [1, 2, 3]
                |    WHERE movie.prop > x
                |    LIMIT 5
                |  ) SCORE AS score""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("x", "movie", "score")),
          Declared(variables = Seq("movie")),
          ExpectedWorkingScope(
            Ast("(movie:Movie)"),
            PatternIncoming(Set("x"), Set("movie", "x"), Set()),
            Declared(variables = Seq("movie")),
            Outgoing(variables = Set("movie")),
            ExpectedResult.TableResult("movie"),
            ExpectedWorkingScope.constExp("Movie", Set("x", "movie"))
          ),
          ExpectedWorkingScope(
            Ast("""SEARCH movie IN (
                  |  VECTOR INDEX moviePlots
                  |  FOR [1, 2, 3]
                  |  WHERE movie.prop > x
                  |  LIMIT 5
                  |) SCORE AS score""".stripMargin),
            Incoming(variables = Set("x", "movie")),
            Referenced(Set("x", "movie")),
            Declared(variables = Seq("score")),
            Outgoing(variables = Set("x", "movie", "score")),
            ExpectedWorkingScope.varExp("movie", Set("x", "movie")),
            ExpectedWorkingScope.constExp("[1, 2, 3]", Set("x", "movie")),
            ExpectedWorkingScope(
              Ast("movie.prop > x"),
              Incoming(constants = Set("x", "movie")),
              Referenced(variables = Set("x", "movie")),
              ExpectedWorkingScope.varExp("movie", Set("x", "movie")),
              ExpectedWorkingScope.varExp("x", Set("x", "movie"))
            ),
            ExpectedWorkingScope.constExp("5", Set("x", "movie"))
          )
        ),
        ExpectedWorkingScope(
          Ast("""RETURN movie.title AS title, score""".stripMargin),
          Incoming(variables = Set("x", "movie", "score")),
          Referenced(Set("movie", "score")),
          Outgoing(variables = Set("title", "score")),
          ExpectedResult.TableResult("title", "score"),
          ExpectedWorkingScope(
            Ast("movie.title"),
            Incoming(constants = Set("x", "movie", "score")),
            Referenced(Set("movie")),
            ExpectedWorkingScope.varExp("movie", Set("x", "movie", "score"))
          ),
          ExpectedWorkingScope.varExp("score", Set("x", "movie", "score"))
        )
      )
    )
  }

  test("""LET a = 10
         |RETURN reduce(acc = a, x IN [1, a, 3] | acc * x + 5) AS red""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |RETURN reduce(acc = a, x IN [1, a, 3] | acc * x + 5) AS red""".stripMargin),
        Outgoing(variables = Set("red")),
        ExpectedResult.TableResult("red"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("RETURN reduce(acc = a, x IN [1, a, 3] | acc * x + 5) AS red"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("red")),
          ExpectedResult.TableResult("red"),
          ExpectedWorkingScope(
            Ast("reduce(acc = a, x IN [1, a, 3] | acc * x + 5)"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope.varExp("a", Set("a")),
            ExpectedWorkingScope(
              Ast("[1, a, 3]"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("reduceScope(acc, x | acc * x + 5)"),
              Incoming(constants = Set("a", "acc", "x")),
              Declared(constants = Seq("acc", "x")),
              ExpectedWorkingScope(
                Ast("acc * x + 5"),
                Incoming(constants = Set("a", "acc", "x")),
                Referenced(Set("acc", "x")),
                ExpectedWorkingScope.varExp("acc", Set("a", "acc", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "acc", "x"))
              )
            )
          )
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS b
         |CALL (a) {
         |  UNWIND [1, 2, 3] AS x
         |  RETURN a * x AS x
         |}
         |RETURN a, x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS b
              |CALL (a) {
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |}
              |RETURN a, x""".stripMargin),
        Outgoing(variables = Set("a", "x")),
        ExpectedResult.TableResult("a", "x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""CALL (a) {
                |  UNWIND [1, 2, 3] AS x
                |  RETURN a * x AS x
                |}""".stripMargin),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "b", "x")),
          ExpectedWorkingScope(
            Ast("""UNWIND [1, 2, 3] AS x
                  |RETURN a * x AS x""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("UNWIND [1, 2, 3] AS x"),
              Incoming(constants = Set("a")),
              Declared(variables = Seq("x")),
              Outgoing(constants = Set("a"), variables = Set("x")),
              ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("RETURN a * x AS x"),
              Incoming(constants = Set("a"), variables = Set("x")),
              Referenced(Set("a", "x")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("a * x"),
                Incoming(constants = Set("a", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "x"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, x"),
          Incoming(variables = Set("a", "b", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedResult.TableResult("a", "x"),
          ExpectedWorkingScope.varExp("a", Set("a", "b", "x")),
          ExpectedWorkingScope.varExp("x", Set("a", "b", "x"))
        )
      )
    )
  }

  test("""WITH true AS a
         |CALL (*) {
         |    WHEN false THEN {
         |        CALL (a) { RETURN 1 AS x } RETURN x
         |        UNION
         |        RETURN 0 AS x
         |  }
         |}
         |RETURN x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH true AS a
              |CALL (*) {
              |  WHEN false THEN {
              |      CALL (a) { RETURN 1 AS x } RETURN x
              |      UNION
              |      RETURN 0 AS x
              |  }
              |}
              |RETURN x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("WITH true AS a"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("true")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (*) {
                |  WHEN false THEN {
                |    CALL (a) { RETURN 1 AS x } RETURN x
                |    UNION
                |    RETURN 0 AS x
                |  }
                |}""".stripMargin),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("x")),
          Referenced(Set("a")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope(
            Ast("""WHEN false THEN {
                  |  CALL (a) { RETURN 1 AS x } RETURN x
                  |  UNION
                  |  RETURN 0 AS x
                  |  }""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("""WHEN false THEN {
                    |  CALL (a) { RETURN 1 AS x } RETURN x
                    |  UNION
                    |  RETURN 0 AS x
                    |  }""".stripMargin),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              Outgoing(variables = Set("x")),
              ExpectedWorkingScope.constExp("false", Set("a")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("""{
                      |  CALL (a) { RETURN 1 AS x } RETURN x
                      |    UNION
                      |  RETURN 0 AS x
                      |}""".stripMargin),
                Incoming(constants = Set("a")),
                Referenced(Set("a")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("""CALL (a) { RETURN 1 AS x } RETURN x
                        |  UNION
                        |RETURN 0 AS x""".stripMargin),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  Declared(variables = Seq("x")),
                  Outgoing(variables = Set("x")),
                  ExpectedResult.TableResult("x"),
                  ExpectedWorkingScope(
                    Ast("""CALL (a) { RETURN 1 AS x } RETURN x""".stripMargin),
                    Incoming(constants = Set("a")),
                    Referenced(Set("a")),
                    Outgoing(variables = Set("x")),
                    ExpectedResult.TableResult("x"),
                    ExpectedWorkingScope(
                      Ast("""CALL (a) { RETURN 1 AS x }""".stripMargin),
                      Incoming(constants = Set("a")),
                      Referenced(Set("a")),
                      Declared(variables = Seq("x")),
                      Outgoing(constants = Set("a"), variables = Set("x")),
                      ExpectedWorkingScope(
                        Ast("""RETURN 1 AS x""".stripMargin),
                        Incoming(constants = Set("a")),
                        Outgoing(variables = Set("x")),
                        ExpectedResult.TableResult("x"),
                        ExpectedWorkingScope(
                          Ast("""RETURN 1 AS x""".stripMargin),
                          Incoming(constants = Set("a")),
                          Outgoing(variables = Set("x")),
                          ExpectedResult.TableResult("x"),
                          ExpectedWorkingScope.constExp("1", Set("a"))
                        )
                      )
                    ),
                    ExpectedWorkingScope(
                      Ast("""RETURN x""".stripMargin),
                      Incoming(constants = Set("a"), variables = Set("x")),
                      Referenced(Set("x")),
                      Outgoing(variables = Set("x")),
                      ExpectedResult.TableResult("x"),
                      ExpectedWorkingScope.varExp("x", Set("a", "x"))
                    )
                  ),
                  ExpectedWorkingScope(
                    Ast("""RETURN 0 AS x""".stripMargin),
                    Incoming(constants = Set("a")),
                    Outgoing(variables = Set("x")),
                    ExpectedResult.TableResult("x"),
                    ExpectedWorkingScope(
                      Ast("""RETURN 0 AS x""".stripMargin),
                      Incoming(constants = Set("a")),
                      Outgoing(variables = Set("x")),
                      ExpectedResult.TableResult("x"),
                      ExpectedWorkingScope.constExp("0", Set("a"))
                    )
                  )
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x"),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope.varExp("x", Set("a", "x"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS b
         |CALL {
         |  WITH a
         |  UNWIND [1, 2, 3] AS x
         |  RETURN a * x AS x
         |}
         |RETURN a, x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS b
              |CALL {
              |  WITH a
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |}
              |RETURN a, x""".stripMargin),
        Outgoing(variables = Set("a", "x")),
        ExpectedResult.TableResult("a", "x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  WITH a
                |  UNWIND [1, 2, 3] AS x
                |  RETURN a * x AS x
                |}""".stripMargin),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "b", "x")),
          ExpectedWorkingScope(
            Ast("""WITH a
                  |UNWIND [1, 2, 3] AS x
                  |RETURN a * x AS x""".stripMargin),
            Incoming(variables = Set("a", "b")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            InImportingWith(),
            ExpectedWorkingScope(
              Ast("WITH a"),
              Incoming(variables = Set("a", "b")),
              Referenced(Set("a")),
              Outgoing(variables = Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a", "b")),
              ExpectedWorkingScope(
                Ast("UNWIND [1, 2, 3] AS x"),
                Incoming(variables = Set("a")),
                Declared(variables = Seq("x")),
                Outgoing(variables = Set("a", "x")),
                ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
              ),
              ExpectedWorkingScope(
                Ast("RETURN a * x AS x"),
                Incoming(variables = Set("a", "x")),
                Referenced(Set("a", "x")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("a * x"),
                  Incoming(constants = Set("a", "x")),
                  Referenced(Set("a", "x")),
                  ExpectedWorkingScope.varExp("a", Set("a", "x")),
                  ExpectedWorkingScope.varExp("x", Set("a", "x"))
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, x"),
          Incoming(variables = Set("a", "b", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedResult.TableResult("a", "x"),
          ExpectedWorkingScope.varExp("a", Set("a", "b", "x")),
          ExpectedWorkingScope.varExp("x", Set("a", "b", "x"))
        )
      )
    )
  }

  test("""LET a = 10
         |CALL (a) {
         |  LET b = a
         |  WITH b, a
         |  RETURN a * b AS x
         |}
         |RETURN a, x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |CALL (a) {
              |  LET b = a
              |  WITH b, a
              |  RETURN a * b AS x
              |}
              |RETURN a, x""".stripMargin),
        Outgoing(variables = Set("a", "x")),
        ExpectedResult.TableResult("a", "x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (a) {
                |  LET b = a
                |  WITH b, a
                |  RETURN a * b AS x
                |}""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope(
            Ast("""LET b = a
                  |WITH b, a
                  |RETURN a * b AS x""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("LET b = a"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              Declared(variables = Seq("b")),
              Outgoing(constants = Set("a"), variables = Set("b")),
              ExpectedWorkingScope.varExp("a", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("WITH b, a"),
              Incoming(constants = Set("a"), variables = Set("b")),
              Referenced(Set("a", "b")),
              Outgoing(constants = Set("a"), variables = Set("b")),
              ExpectedWorkingScope.varExp("b", Set("a", "b")),
              ExpectedWorkingScope.varExp("a", Set("a", "b"))
            ),
            ExpectedWorkingScope(
              Ast("RETURN a * b AS x"),
              Incoming(constants = Set("a"), variables = Set("b")),
              Referenced(Set("a", "b")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("a * b"),
                Incoming(constants = Set("a", "b")),
                Referenced(Set("a", "b")),
                ExpectedWorkingScope.varExp("a", Set("a", "b")),
                ExpectedWorkingScope.varExp("b", Set("a", "b"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, x"),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedResult.TableResult("a", "x"),
          ExpectedWorkingScope.varExp("a", Set("a", "x")),
          ExpectedWorkingScope.varExp("x", Set("a", "x"))
        )
      )
    )
  }

  test("""LET a = 10
         |CALL {
         |  WITH a
         |  LET b = a
         |  WITH b, a
         |  RETURN a * b AS x
         |}
         |RETURN a, x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |CALL {
              |  WITH a
              |  LET b = a
              |  WITH b, a
              |  RETURN a * b AS x
              |}
              |RETURN a, x""".stripMargin),
        Outgoing(variables = Set("a", "x")),
        ExpectedResult.TableResult("a", "x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  WITH a
                |  LET b = a
                |  WITH b, a
                |  RETURN a * b AS x
                |}""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope(
            Ast("""WITH a
                  |LET b = a
                  |WITH b, a
                  |RETURN a * b AS x""".stripMargin),
            Incoming(variables = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            InImportingWith(),
            ExpectedWorkingScope(
              Ast("WITH a"),
              Incoming(variables = Set("a")),
              Referenced(Set("a")),
              Outgoing(variables = Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a")),
              ExpectedWorkingScope(
                Ast("LET b = a"),
                Incoming(variables = Set("a")),
                Referenced(Set("a")),
                Declared(variables = Seq("b")),
                Outgoing(variables = Set("a", "b")),
                ExpectedWorkingScope.varExp("a", Set("a"))
              ),
              ExpectedWorkingScope(
                Ast("WITH b, a"),
                Incoming(variables = Set("a", "b")),
                Referenced(Set("a", "b")),
                Outgoing(variables = Set("a", "b")),
                ExpectedWorkingScope.varExp("b", Set("a", "b")),
                ExpectedWorkingScope.varExp("a", Set("a", "b"))
              ),
              ExpectedWorkingScope(
                Ast("RETURN a * b AS x"),
                Incoming(variables = Set("a", "b")),
                Referenced(Set("a", "b")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("a * b"),
                  Incoming(constants = Set("a", "b")),
                  Referenced(Set("a", "b")),
                  ExpectedWorkingScope.varExp("a", Set("a", "b")),
                  ExpectedWorkingScope.varExp("b", Set("a", "b"))
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, x"),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedResult.TableResult("a", "x"),
          ExpectedWorkingScope.varExp("a", Set("a", "x")),
          ExpectedWorkingScope.varExp("x", Set("a", "x"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS b
         |CALL (a) {
         |  UNWIND [1, 2, 3] AS x
         |  RETURN a * x AS x
         |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL
         |RETURN a, x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS b
              |CALL (a) {
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL
              |RETURN a, x""".stripMargin),
        Outgoing(variables = Set("a", "x")),
        ExpectedResult.TableResult("a", "x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""CALL (a) {
                |  UNWIND [1, 2, 3] AS x
                |  RETURN a * x AS x
                |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL""".stripMargin),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "b", "x")),
          ExpectedWorkingScope(
            Ast("""UNWIND [1, 2, 3] AS x
                  |RETURN a * x AS x""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("UNWIND [1, 2, 3] AS x"),
              Incoming(constants = Set("a")),
              Declared(variables = Seq("x")),
              Outgoing(constants = Set("a"), variables = Set("x")),
              ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("RETURN a * x AS x"),
              Incoming(constants = Set("a"), variables = Set("x")),
              Referenced(Set("a", "x")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("a * x"),
                Incoming(constants = Set("a", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "x"))
              )
            )
          ),
          ExpectedWorkingScope.constExp("2 + 3", Set("a", "b")),
          ExpectedWorkingScope.constExp("2.5", Set("a", "b"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, x"),
          Incoming(variables = Set("a", "b", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedResult.TableResult("a", "x"),
          ExpectedWorkingScope.varExp("a", Set("a", "b", "x")),
          ExpectedWorkingScope.varExp("x", Set("a", "b", "x"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS b
         |CALL {
         |  WITH a
         |  UNWIND [1, 2, 3] AS x
         |  RETURN a * x AS x
         |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL
         |RETURN a, x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS b
              |CALL {
              |  WITH a
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL
              |RETURN a, x""".stripMargin),
        Outgoing(variables = Set("a", "x")),
        ExpectedResult.TableResult("a", "x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  WITH a
                |  UNWIND [1, 2, 3] AS x
                |  RETURN a * x AS x
                |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL""".stripMargin),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "b", "x")),
          ExpectedWorkingScope(
            Ast("""WITH a
                  |UNWIND [1, 2, 3] AS x
                  |RETURN a * x AS x""".stripMargin),
            Incoming(variables = Set("a", "b")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            InImportingWith(),
            ExpectedWorkingScope(
              Ast("WITH a"),
              Incoming(variables = Set("a", "b")),
              Referenced(Set("a")),
              Outgoing(variables = Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a", "b")),
              ExpectedWorkingScope(
                Ast("UNWIND [1, 2, 3] AS x"),
                Incoming(variables = Set("a")),
                Declared(variables = Seq("x")),
                Outgoing(variables = Set("a", "x")),
                ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
              ),
              ExpectedWorkingScope(
                Ast("RETURN a * x AS x"),
                Incoming(variables = Set("a", "x")),
                Referenced(Set("a", "x")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("a * x"),
                  Incoming(constants = Set("a", "x")),
                  Referenced(Set("a", "x")),
                  ExpectedWorkingScope.varExp("a", Set("a", "x")),
                  ExpectedWorkingScope.varExp("x", Set("a", "x"))
                )
              )
            )
          ),
          ExpectedWorkingScope.constExp("2 + 3", Set("a", "b")),
          ExpectedWorkingScope.constExp("2.5", Set("a", "b"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, x"),
          Incoming(variables = Set("a", "b", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedResult.TableResult("a", "x"),
          ExpectedWorkingScope.varExp("a", Set("a", "b", "x")),
          ExpectedWorkingScope.varExp("x", Set("a", "b", "x"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS b
         |CALL (a) {
         |  UNWIND [1, 2, 3] AS x
         |  RETURN a * x AS x
         |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL REPORT STATUS AS r
         |RETURN a, r""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS b
              |CALL (a) {
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL REPORT STATUS AS r
              |RETURN a, r""".stripMargin),
        Outgoing(variables = Set("a", "r")),
        ExpectedResult.TableResult("a", "r"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast(
            """CALL (a) {
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL REPORT STATUS AS r""".stripMargin
          ),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a")),
          Declared(variables = Seq("x", "r")),
          Outgoing(variables = Set("a", "b", "x", "r")),
          ExpectedWorkingScope(
            Ast("""UNWIND [1, 2, 3] AS x
                  |RETURN a * x AS x""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("UNWIND [1, 2, 3] AS x"),
              Incoming(constants = Set("a")),
              Declared(variables = Seq("x")),
              Outgoing(constants = Set("a"), variables = Set("x")),
              ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("RETURN a * x AS x"),
              Incoming(constants = Set("a"), variables = Set("x")),
              Referenced(Set("a", "x")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("a * x"),
                Incoming(constants = Set("a", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "x"))
              )
            )
          ),
          ExpectedWorkingScope.constExp("2 + 3", Set("a", "b")),
          ExpectedWorkingScope.constExp("2.5", Set("a", "b"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, r"),
          Incoming(variables = Set("a", "b", "x", "r")),
          Referenced(Set("a", "r")),
          Outgoing(variables = Set("a", "r")),
          ExpectedResult.TableResult("a", "r"),
          ExpectedWorkingScope.varExp("a", Set("a", "b", "x", "r")),
          ExpectedWorkingScope.varExp("r", Set("a", "b", "x", "r"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS b
         |CALL {
         |  WITH a
         |  UNWIND [1, 2, 3] AS x
         |  RETURN a * x AS x
         |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL REPORT STATUS AS r
         |RETURN a, r""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS b
              |CALL {
              |  WITH a
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL REPORT STATUS AS r
              |RETURN a, r""".stripMargin),
        Outgoing(variables = Set("a", "r")),
        ExpectedResult.TableResult("a", "r"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS b"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast(
            """CALL {
              |  WITH a
              |  UNWIND [1, 2, 3] AS x
              |  RETURN a * x AS x
              |} IN TRANSACTIONS OF 2 + 3 ROWS ON ERROR RETRY 2.5 SECONDS THEN FAIL REPORT STATUS AS r""".stripMargin
          ),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a")),
          Declared(variables = Seq("x", "r")),
          Outgoing(variables = Set("a", "b", "x", "r")),
          ExpectedWorkingScope(
            Ast("""WITH a
                  |UNWIND [1, 2, 3] AS x
                  |RETURN a * x AS x""".stripMargin),
            Incoming(variables = Set("a", "b")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            InImportingWith(),
            ExpectedWorkingScope(
              Ast("WITH a"),
              Incoming(variables = Set("a", "b")),
              Referenced(Set("a")),
              Outgoing(variables = Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a", "b")),
              ExpectedWorkingScope(
                Ast("UNWIND [1, 2, 3] AS x"),
                Incoming(variables = Set("a")),
                Declared(variables = Seq("x")),
                Outgoing(variables = Set("a", "x")),
                ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
              ),
              ExpectedWorkingScope(
                Ast("RETURN a * x AS x"),
                Incoming(variables = Set("a", "x")),
                Referenced(Set("a", "x")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("a * x"),
                  Incoming(constants = Set("a", "x")),
                  Referenced(Set("a", "x")),
                  ExpectedWorkingScope.varExp("a", Set("a", "x")),
                  ExpectedWorkingScope.varExp("x", Set("a", "x"))
                )
              )
            )
          ),
          ExpectedWorkingScope.constExp("2 + 3", Set("a", "b")),
          ExpectedWorkingScope.constExp("2.5", Set("a", "b"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, r"),
          Incoming(variables = Set("a", "b", "x", "r")),
          Referenced(Set("a", "r")),
          Outgoing(variables = Set("a", "r")),
          ExpectedResult.TableResult("a", "r"),
          ExpectedWorkingScope.varExp("a", Set("a", "b", "x", "r")),
          ExpectedWorkingScope.varExp("r", Set("a", "b", "x", "r"))
        )
      )
    )
  }

  test("""LET n = 1
         |LET x = COUNT {
         |  LET a = 1
         |  CALL (a) {
         |    LET n = 3
         |    RETURN a + n AS result
         |  }
         |  RETURN result
         |}
         |RETURN *""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET n = 1
              |LET x = COUNT {
              |  LET a = 1
              |  CALL (a) {
              |    LET n = 3
              |    RETURN a + n AS result
              |  }
              |  RETURN result
              |}
              |RETURN *""".stripMargin),
        Outgoing(variables = Set("n", "x")),
        ExpectedResult.TableResult("n", "x"),
        ExpectedWorkingScope(
          Ast("LET n = 1"),
          Declared(variables = Seq("n")),
          Outgoing(variables = Set("n")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""LET x = COUNT { LET a = 1
                |  CALL (a) {
                |    LET n = 3
                |    RETURN a + n AS result
                |  }
                |  RETURN result
                |}""".stripMargin),
          Incoming(variables = Set("n")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("n", "x")),
          ExpectedWorkingScope(
            Ast("""COUNT {
                  |  LET a = 1
                  |  CALL (a) {
                  |    LET n = 3
                  |    RETURN a + n AS result
                  |  }
                  |  RETURN result
                  |}""".stripMargin),
            Incoming(constants = Set("n")),
            ExpectedResult.ExpressionResult,
            ExpectedWorkingScope(
              Ast("""LET a = 1
                    |CALL (a) {
                    |  LET n = 3
                    |  RETURN a + n AS result
                    |}
                    |RETURN result""".stripMargin),
              Incoming(constants = Set("n")),
              Outgoing(variables = Set("result")),
              ExpectedResult.TableResult("result"),
              ExpectedWorkingScope(
                Ast("LET a = 1"),
                Incoming(constants = Set("n")),
                Declared(variables = Seq("a")),
                Outgoing(constants = Set("n"), variables = Set("a")),
                ExpectedWorkingScope.constExp("1", Set("n"))
              ),
              ExpectedWorkingScope(
                Ast("""CALL (a) {
                      |  LET n = 3
                      |  RETURN a + n AS result
                      |}""".stripMargin),
                Incoming(constants = Set("n"), variables = Set("a")),
                Referenced(Set("a")),
                Declared(variables = Seq("result")),
                Outgoing(constants = Set("n"), variables = Set("a", "result")),
                ExpectedWorkingScope(
                  Ast("""LET n = 3
                        |RETURN a + n AS result""".stripMargin),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  Outgoing(variables = Set("result")),
                  ExpectedResult.TableResult("result"),
                  ExpectedWorkingScope(
                    Ast("LET n = 3"),
                    Incoming(constants = Set("a")),
                    Declared(variables = Seq("n")),
                    Outgoing(constants = Set("a"), variables = Set("n")),
                    ExpectedWorkingScope.constExp("3", Set("a"))
                  ),
                  ExpectedWorkingScope(
                    Ast("RETURN a + n AS result"),
                    Incoming(constants = Set("a"), variables = Set("n")),
                    Referenced(Set("a", "n")),
                    Outgoing(variables = Set("result")),
                    ExpectedResult.TableResult("result"),
                    ExpectedWorkingScope(
                      Ast("a + n"),
                      Incoming(constants = Set("a", "n")),
                      Referenced(Set("a", "n")),
                      ExpectedWorkingScope.varExp("a", Set("a", "n")),
                      ExpectedWorkingScope.varExp("n", Set("a", "n"))
                    )
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("RETURN result"),
                Incoming(constants = Set("n"), variables = Set("a", "result")),
                Referenced(Set("result")),
                Outgoing(variables = Set("result")),
                ExpectedResult.TableResult("result"),
                ExpectedWorkingScope.varExp("result", Set("n", "a", "result"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN *"),
          Incoming(variables = Set("n", "x")),
          Referenced(Set("n", "x")),
          Outgoing(variables = Set("n", "x")),
          ExpectedResult.TableResult("n", "x")
        )
      )
    )
  }

  test("""LET a = 10
         |CALL {
         |  WITH a
         |  LET b = a
         |  WITH b
         |  RETURN b AS x
         |}
         |RETURN a, x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |CALL {
              |  WITH a
              |  LET b = a
              |  WITH b
              |  RETURN b AS x
              |}
              |RETURN a, x""".stripMargin),
        Outgoing(variables = Set("a", "x")),
        ExpectedResult.TableResult("a", "x"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  WITH a
                |  LET b = a
                |  WITH b
                |  RETURN b AS x
                |}""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope(
            Ast("""WITH a
                  |LET b = a
                  |WITH b
                  |RETURN b AS x""".stripMargin),
            Incoming(variables = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            InImportingWith(),
            ExpectedWorkingScope(
              Ast("WITH a"),
              Incoming(variables = Set("a")),
              Referenced(Set("a")),
              Outgoing(variables = Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a")),
              ExpectedWorkingScope(
                Ast("LET b = a"),
                Incoming(variables = Set("a")),
                Referenced(Set("a")),
                Declared(variables = Seq("b")),
                Outgoing(variables = Set("a", "b")),
                ExpectedWorkingScope.varExp("a", Set("a"))
              ),
              ExpectedWorkingScope(
                Ast("WITH b"),
                Incoming(variables = Set("a", "b")),
                Referenced(Set("b")),
                Outgoing(variables = Set("b")),
                ExpectedWorkingScope.varExp("b", Set("a", "b"))
              ),
              ExpectedWorkingScope(
                Ast("RETURN b AS x"),
                Incoming(variables = Set("b")),
                Referenced(Set("b")),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope.varExp("b", Set("b"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, x"),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedResult.TableResult("a", "x"),
          ExpectedWorkingScope.varExp("a", Set("a", "x")),
          ExpectedWorkingScope.varExp("x", Set("a", "x"))
        )
      )
    )
  }

  test("CALL db.info()") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CALL db.info()"), // query level
        ExpectedResult.TableResultWithNotYetKnownColumns,
        ExpectedWorkingScope(
          Ast("CALL db.info()"),
          ExpectedResult.TableResultWithNotYetKnownColumns
        )
      )
    )
  }

  test("CALL db.info() YIELD *") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CALL db.info() YIELD *"), // query level
        ExpectedResult.TableResultWithNotYetKnownColumns,
        ExpectedWorkingScope(
          Ast("CALL db.info() YIELD *"),
          ExpectedResult.TableResultWithNotYetKnownColumns
        )
      )
    )
  }

  test("CALL db.info() YIELD name, id") {
    hasScope(
      ExpectedWorkingScope(
        Ast("CALL db.info() YIELD name, id"), // query level
        Outgoing(variables = Set("name", "id")),
        ExpectedResult.TableResult("name", "id"),
        ExpectedWorkingScope(
          Ast("CALL db.info() YIELD name, id"),
          Declared(variables = Seq("name", "id")),
          Outgoing(variables = Set("name", "id")),
          ExpectedResult.TableResult("name", "id")
        )
      )
    )
  }

  test(
    """CALL db.info()
      |RETURN 1 AS x""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""CALL db.info()
              |RETURN 1 AS x""".stripMargin), // query level
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("CALL db.info()"),
          ExpectedResult.OmittedResult
        ),
        ExpectedWorkingScope(
          Ast("RETURN 1 AS x"),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope.constExp("1")
        )
      )
    )
  }

  test(
    """CALL () {
      |  CALL db.info()
      |}
      |RETURN 1 AS x""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""CALL () {
              |  CALL db.info()
              |}
              |RETURN 1 AS x""".stripMargin), // query level
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("""CALL () {
                |  CALL db.info()
                |}""".stripMargin),
          ExpectedResult.NoResult,
          ExpectedWorkingScope(
            Ast("CALL db.info()"), // query level
            ExpectedResult.OmittedResult,
            ExpectedWorkingScope(
              Ast("CALL db.info()"),
              ExpectedResult.OmittedResult
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN 1 AS x"),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope.constExp("1")
        )
      )
    )
  }

//  ExpectedWorkingScope(
//    Ast("""CALL (a) {
//          |  UNWIND [1, 2, 3] AS x
//          |  RETURN a * x AS x
//          |}""".stripMargin),
//    Incoming(variables = Set("a", "b")),
//    Referenced(Set("a")),
//    Declared(variables = Seq("x")),
//    Outgoing(variables = Set("a", "b", "x")),
//    ExpectedWorkingScope(
//      Ast("""UNWIND [1, 2, 3] AS x
//            |RETURN a * x AS x""".stripMargin),
//      Incoming(constants = Set("a")),
//      Referenced(Set("a")),
//      Outgoing(variables = Set("x")),
//      ExpectedResult.TableResult("x"),
//      ExpectedWorkingScope(
//        Ast("UNWIND [1, 2, 3] AS x"),
//        Incoming(constants = Set("a")),
//        Declared(variables = Seq("x")),
//        Outgoing(constants = Set("a"), variables = Set("x")),
//        ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
//      ),
//      ExpectedWorkingScope(
//        Ast("RETURN a * x AS x"),
//        Incoming(constants = Set("a"), variables = Set("x")),
//        Referenced(Set("a", "x")),
//        Outgoing(variables = Set("x")),
//        ExpectedResult.TableResult("x"),
//        ExpectedWorkingScope(
//          Ast("a * x"),
//          Incoming(constants = Set("a", "x")),
//          Referenced(Set("a", "x")),
//          ExpectedWorkingScope.varExp("a", Set("a", "x")),
//          ExpectedWorkingScope.varExp("x", Set("a", "x"))
//        )
//      )
//    )
//  ),
  test("""CALL db.info() YIELD name, id
         |RETURN name""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""CALL db.info() YIELD name, id
              |RETURN name""".stripMargin),
        Outgoing(variables = Set("name")),
        ExpectedResult.TableResult("name"),
        ExpectedWorkingScope(
          Ast("CALL db.info() YIELD name, id"),
          Declared(variables = Seq("name", "id")),
          Outgoing(variables = Set("name", "id")),
          ExpectedResult.NoResult
        ),
        ExpectedWorkingScope(
          Ast("RETURN name"),
          Incoming(variables = Set("name", "id")),
          Referenced(Set("name")),
          Outgoing(variables = Set("name")),
          ExpectedResult.TableResult("name"),
          ExpectedWorkingScope.varExp("name", Set("name", "id"))
        )
      )
    )
  }

  test("""LET query = "bob"
         |CALL db.index.fulltext.queryNodes("myIndex", query) YIELD node AS n, score
         |RETURN n ORDER BY score ASCENDING LIMIT 3""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET query = "bob"
              |CALL db.index.fulltext.queryNodes("myIndex", query) YIELD node AS n, score
              |RETURN n ORDER BY score ASCENDING LIMIT 3""".stripMargin),
        Outgoing(variables = Set("n")),
        ExpectedResult.TableResult("n"),
        ExpectedWorkingScope(
          Ast("""LET query = "bob" """),
          Declared(variables = Seq("query")),
          Outgoing(variables = Set("query")),
          ExpectedWorkingScope.constExp(""" "bob" """)
        ),
        ExpectedWorkingScope(
          Ast("""CALL db.index.fulltext.queryNodes("myIndex", query) YIELD node AS n, score"""),
          Incoming(variables = Set("query")),
          Referenced(Set("query")),
          Declared(variables = Seq("n", "score")),
          Outgoing(variables = Set("query", "n", "score")),
          ExpectedResult.NoResult,
          ExpectedWorkingScope.constExp(""" "myIndex" """, Set("query")),
          ExpectedWorkingScope.varExp("query", Set("query"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN n ORDER BY score ASCENDING LIMIT 3"),
          Incoming(variables = Set("query", "n", "score")),
          Referenced(Set("n", "score")),
          Outgoing(variables = Set("n")),
          ExpectedResult.TableResult("n"),
          ExpectedWorkingScope.varExp("n", Set("query", "n", "score")),
          ExpectedWorkingScope.varProjExp("score", Set("query", "n", "score")),
          ExpectedWorkingScope.constProjExp("3", Set("query", "n", "score"))
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN SUM(x) AS s""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN SUM(x) AS s""".stripMargin),
        Outgoing(variables = Set("s")),
        ExpectedResult.TableResult("s"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("RETURN SUM(x) AS s"),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("s")),
          ExpectedResult.TableResult("s"),
          ExpectedWorkingScope(
            Ast("SUM(x)"),
            ProjectionIncoming(variables = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x"))
          )
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN SUM(x) AS s
         |  ORDER BY COUNT(1) ASCENDING""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN SUM(x) AS s
              |  ORDER BY COUNT(1) ASCENDING""".stripMargin),
        Outgoing(variables = Set("s")),
        ExpectedResult.TableResult("s"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("""RETURN SUM(x) AS s
                |  ORDER BY COUNT(1) ASCENDING""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("s")),
          ExpectedResult.TableResult("s"),
          ExpectedWorkingScope(
            Ast("SUM(x)"),
            ProjectionIncoming(variables = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x"))
          ),
          ExpectedWorkingScope(
            Ast("COUNT(1)"),
            ProjectionIncoming(variables = Set("s", "x")),
            ExpectedWorkingScope.constExp("1", Set("x", "s"))
          )
        )
      ),
      skipVariableChecker = true
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN SUM(x) AS s
         |  ORDER BY s ASCENDING""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN SUM(x) AS s
              |  ORDER BY s ASCENDING""".stripMargin),
        Outgoing(variables = Set("s")),
        ExpectedResult.TableResult("s"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("""RETURN SUM(x) AS s
                |  ORDER BY s ASCENDING""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("s")),
          ExpectedResult.TableResult("s"),
          ExpectedWorkingScope(
            Ast("SUM(x)"),
            ProjectionIncoming(variables = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope.varExp("x", Set("x"))
          ),
          ExpectedWorkingScope.varProjExp("s", incomingConstants = Set("s"))
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS x
         |RETURN a, SUM(x / a) + a * 5 AS s""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS x
              |RETURN a, SUM(x / a) + a * 5 AS s""".stripMargin),
        Outgoing(variables = Set("a", "s")),
        ExpectedResult.TableResult("a", "s"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, SUM(x / a) + a * 5 AS s"),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "s")),
          ExpectedResult.TableResult("a", "s"),
          ExpectedWorkingScope.varExp("a", Set("a", "x")),
          ExpectedWorkingScope(
            Ast("SUM(x / a) + a * 5"),
            ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"))),
            Referenced(Set("a", "x")),
            ExpectedWorkingScope(
              Ast("SUM(x / a)"),
              ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"))),
              Referenced(Set("a", "x")),
              ExpectedWorkingScope(
                Ast("x / a"),
                Incoming(constants = Set("a", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x"))
              )
            ),
            ExpectedWorkingScope.varProjExp("a", incomingVariables = Set("a", "x"), incomingKeys = Set(gk("a")))
          )
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS x
         |RETURN *, SUM(x) + a AS s""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS x
              |RETURN *, SUM(x) + a AS s""".stripMargin),
        Outgoing(variables = Set("a", "x", "s")),
        ExpectedResult.TableResult("a", "x", "s"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("RETURN *, SUM(x) + a AS s"),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "x", "s")),
          ExpectedResult.TableResult("a", "x", "s"),
          ExpectedWorkingScope(
            Ast("SUM(x) + a"),
            ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"), gk("x"))),
            Referenced(Set("a", "x")),
            ExpectedWorkingScope(
              Ast("SUM(x)"),
              ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"), gk("x"))),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("a", "x"))
            ),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingVariables = Set("a", "x"),
              incomingKeys = Set(gk("a"), gk("x"))
            )
          )
        )
      )
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS x
         |RETURN a, SUM(x / a) + a * 5 AS s
         |  ORDER BY -1 * MAX(a * x) - a ASCENDING""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS x
              |RETURN a, SUM(x / a) + a * 5 AS s
              |  ORDER BY -1 * MAX(a * x) - a ASCENDING""".stripMargin),
        Outgoing(variables = Set("a", "s")),
        ExpectedResult.TableResult("a", "s"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""RETURN a, SUM(x / a) + a * 5 AS s
                |  ORDER BY -1 * MAX(a * x) - a ASCENDING""".stripMargin),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "s")),
          ExpectedResult.TableResult("a", "s"),
          ExpectedWorkingScope.varExp("a", incomingConstants = Set("a", "x")),
          ExpectedWorkingScope(
            Ast("SUM(x / a) + a * 5"),
            ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"))),
            Referenced(Set("a", "x")),
            ExpectedWorkingScope(
              Ast("SUM(x / a)"),
              ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"))),
              Referenced(Set("a", "x")),
              ExpectedWorkingScope(
                Ast("x / a"),
                Incoming(constants = Set("a", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x"))
              )
            ),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingVariables = Set("a", "x"),
              incomingKeys = Set(gk("a"))
            )
          ),
          ExpectedWorkingScope(
            Ast("-1 * MAX(a * x) - a"),
            ProjectionIncoming(variables = Set("a", "x", "s"), groupingKeys = Set(gk("a"))),
            Referenced(Set("a", "x")),
            ExpectedWorkingScope(
              Ast("MAX(a * x)"),
              ProjectionIncoming(variables = Set("a", "x", "s"), groupingKeys = Set(gk("a"))),
              Referenced(Set("a", "x")),
              ExpectedWorkingScope(
                Ast("a * x"),
                Incoming(constants = Set("a", "x", "s")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x", "s")),
                ExpectedWorkingScope.varExp("x", Set("a", "x", "s"))
              )
            ),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingVariables = Set("a", "x", "s"),
              incomingKeys = Set(gk("a"))
            )
          )
        )
      ),
      skipVariableChecker = true
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS x
         |RETURN a, SUM(x / a) + a * 5 AS s
         |  ORDER BY s * MAX(a * x) - a ASCENDING""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS x
              |RETURN a, SUM(x / a) + a * 5 AS s
              |  ORDER BY s * MAX(a * x) - a ASCENDING""".stripMargin),
        Outgoing(variables = Set("a", "s")),
        ExpectedResult.TableResult("a", "s"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""RETURN a, SUM(x / a) + a * 5 AS s
                |  ORDER BY s * MAX(a * x) - a ASCENDING""".stripMargin),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("a", "s")),
          ExpectedResult.TableResult("a", "s"),
          ExpectedWorkingScope.varProjExp("a", Set("a", "x"), incomingKeys = Set(gk("a"))),
          ExpectedWorkingScope(
            Ast("SUM(x / a) + a * 5"),
            ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"))),
            Referenced(Set("a", "x")),
            ExpectedWorkingScope(
              Ast("SUM(x / a)"),
              ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("a"))),
              Referenced(Set("a", "x")),
              ExpectedWorkingScope(
                Ast("x / a"),
                Incoming(constants = Set("a", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x"))
              )
            ),
            ExpectedWorkingScope.varProjExp("a", incomingVariables = Set("a", "x"), incomingKeys = Set(gk("a")))
          ),
          ExpectedWorkingScope(
            Ast("s * MAX(a * x) - a"),
            ProjectionIncoming(variables = Set("a", "x", "s"), groupingKeys = Set(gk("a"))),
            Referenced(Set("a", "s", "x")),
            ExpectedWorkingScope.varProjExp(
              "s",
              incomingVariables = Set("a", "x", "s"),
              incomingKeys = Set(gk("a"))
            ),
            ExpectedWorkingScope(
              Ast("MAX(a * x)"),
              ProjectionIncoming(variables = Set("a", "x", "s"), groupingKeys = Set(gk("a"))),
              Referenced(Set("a", "x")),
              ExpectedWorkingScope(
                Ast("a * x"),
                Incoming(constants = Set("a", "x", "s")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x", "s")),
                ExpectedWorkingScope.varExp("x", Set("a", "x", "s"))
              )
            ),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingVariables = Set("a", "x", "s"),
              incomingKeys = Set(gk("a"))
            )
          )
        )
      ),
      skipVariableChecker = true
    )
  }

  test("""LET a = 10
         |UNWIND [1, 2, 3] AS x
         |RETURN a AS g, SUM(x / a) + a * 5 AS s
         |  ORDER BY s * MAX(g * x) - a ASCENDING""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |UNWIND [1, 2, 3] AS x
              |RETURN a AS g, SUM(x / a) + a * 5 AS s
              |  ORDER BY s * MAX(g * x) - a ASCENDING""".stripMargin),
        Outgoing(variables = Set("g", "s")),
        ExpectedResult.TableResult("g", "s"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS x"),
          Incoming(variables = Set("a")),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("a", "x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
        ),
        ExpectedWorkingScope(
          Ast("""RETURN a AS g, SUM(x / a) + a * 5 AS s
                |  ORDER BY s * MAX(g * x) - a ASCENDING""".stripMargin),
          Incoming(variables = Set("a", "x")),
          Referenced(Set("a", "x")),
          Outgoing(variables = Set("g", "s")),
          ExpectedResult.TableResult("g", "s"),
          ExpectedWorkingScope.varExp("a", Set("a", "x")),
          ExpectedWorkingScope(
            Ast("SUM(x / a) + a * 5"),
            ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("g"))),
            Referenced(Set("a", "x")),
            ExpectedWorkingScope(
              Ast("SUM(x / a)"),
              ProjectionIncoming(variables = Set("a", "x"), groupingKeys = Set(gk("g"))),
              Referenced(Set("a", "x")),
              ExpectedWorkingScope(
                Ast("x / a"),
                Incoming(constants = Set("a", "x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope.varExp("x", Set("a", "x")),
                ExpectedWorkingScope.varExp("a", Set("a", "x"))
              )
            ),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingVariables = Set("a", "x"),
              incomingKeys = Set(gk("g"))
            )
          ),
          ExpectedWorkingScope(
            Ast("s * MAX(g * x) - a"),
            ProjectionIncoming(variables = Set("a", "x", "g", "s"), groupingKeys = Set(gk("g"))),
            Referenced(Set("a", "g", "s", "x")),
            ExpectedWorkingScope.varProjExp(
              "s",
              incomingVariables = Set("a", "x", "g", "s"),
              incomingKeys = Set(gk("g"))
            ),
            ExpectedWorkingScope(
              Ast("MAX(g * x)"),
              ProjectionIncoming(variables = Set("a", "x", "g", "s"), groupingKeys = Set(gk("g"))),
              Referenced(Set("g", "x")),
              ExpectedWorkingScope(
                Ast("g * x"),
                Incoming(constants = Set("a", "x", "g", "s")),
                Referenced(Set("g", "x")),
                ExpectedWorkingScope.varExp("g", Set("a", "x", "g", "s")),
                ExpectedWorkingScope.varExp("x", Set("a", "x", "g", "s"))
              )
            ),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingVariables = Set("a", "x", "g", "s"),
              incomingKeys = Set(gk("g"))
            )
          )
        )
      ),
      skipVariableChecker = true
    )
  }

  test("""UNWIND [1, 2, 3] AS a
         |CALL (a) {
         |  UNWIND [1, 2, 3] AS x
         |  RETURN COUNT(x) * a AS s
         |}
         |RETURN a, AVG(s) AS s""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS a
              |CALL (a) {
              |  UNWIND [1, 2, 3] AS x
              |  RETURN COUNT(x) * a AS s
              |}
              |RETURN a, AVG(s) AS s""".stripMargin),
        Outgoing(variables = Set("a", "s")),
        ExpectedResult.TableResult("a", "s"),
        ExpectedWorkingScope(
          Ast("UNWIND [1, 2, 3] AS a"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (a) {
                |  UNWIND [1, 2, 3] AS x
                |  RETURN COUNT(x) * a AS s
                |}""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("s")),
          Outgoing(variables = Set("a", "s")),
          ExpectedWorkingScope(
            Ast("""UNWIND [1, 2, 3] AS x
                  |RETURN COUNT(x) * a AS s""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            Outgoing(variables = Set("s")),
            ExpectedResult.TableResult("s"),
            ExpectedWorkingScope(
              Ast("UNWIND [1, 2, 3] AS x"),
              Incoming(constants = Set("a")),
              Declared(variables = Seq("x")),
              Outgoing(constants = Set("a"), variables = Set("x")),
              ExpectedWorkingScope.constExp("[1, 2, 3]", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("RETURN COUNT(x) * a AS s"),
              Incoming(constants = Set("a"), variables = Set("x")),
              Referenced(Set("a", "x")),
              Outgoing(variables = Set("s")),
              ExpectedResult.TableResult("s"),
              ExpectedWorkingScope(
                Ast("COUNT(x) * a"),
                ProjectionIncoming(constants = Set("a"), variables = Set("x")),
                Referenced(Set("a", "x")),
                ExpectedWorkingScope(
                  Ast("COUNT(x)"),
                  ProjectionIncoming(constants = Set("a"), variables = Set("x")),
                  Referenced(Set("x")),
                  ExpectedWorkingScope.varExp("x", Set("a", "x"))
                ),
                ExpectedWorkingScope.varProjExp("a", Set("a"), Set("x"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, AVG(s) AS s"),
          Incoming(variables = Set("a", "s")),
          Referenced(Set("a", "s")),
          Outgoing(variables = Set("a", "s")),
          ExpectedResult.TableResult("a", "s"),
          ExpectedWorkingScope.varProjExp(
            "a",
            incomingConstants = Set("a", "s"),
            incomingKeys = Set(gk("a"))
          ),
          ExpectedWorkingScope(
            Ast("AVG(s)"),
            ProjectionIncoming(variables = Set("a", "s"), groupingKeys = Set(gk("a"))),
            Referenced(Set("s")),
            ExpectedWorkingScope.varExp("s", Set("a", "s"))
          )
        )
      )
    )
  }

  test(
    """WITH "d" AS word
      |RETURN any(prefix IN ["a", "b", "c", word] WHERE word = prefix) AS check""".stripMargin
  ) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH "d" AS word
              |RETURN any(prefix IN ["a", "b", "c", word] WHERE word = prefix) AS check""".stripMargin), // query level
        Outgoing(variables = Set("check")),
        ExpectedResult.TableResult("check"),
        ExpectedWorkingScope(
          Ast("""WITH "d" AS word"""),
          Declared(variables = Seq("word")),
          Outgoing(variables = Set("word")),
          ExpectedWorkingScope.constExp("\"d\"")
        ),
        ExpectedWorkingScope(
          Ast("""RETURN any(prefix IN ["a", "b", "c", word] WHERE word = prefix) AS check""".stripMargin),
          Incoming(variables = Set("word")),
          Referenced(Set("word")),
          Outgoing(variables = Set("check")),
          ExpectedResult.TableResult("check"),
          ExpectedWorkingScope(
            Ast("""any(prefix IN ["a", "b", "c", word] WHERE word = prefix)""".stripMargin),
            Incoming(constants = Set("word")),
            Referenced(Set("word")),
            ExpectedWorkingScope(
              Ast("[prefix WHERE word = prefix]"),
              Incoming(constants = Set("prefix", "word")),
              Declared(Seq("prefix")),
              Referenced(Set("word")),
              ExpectedWorkingScope(
                Ast("word = prefix"),
                Incoming(constants = Set("prefix", "word")),
                Referenced(Set("prefix", "word")),
                ExpectedWorkingScope.varExp("word", incomingConstants = Set("prefix", "word")),
                ExpectedWorkingScope.varExp("prefix", incomingConstants = Set("prefix", "word"))
              )
            ),
            ExpectedWorkingScope(
              Ast("""["a", "b", "c", word]"""),
              Incoming(constants = Set("word")),
              Referenced(Set("word")),
              ExpectedWorkingScope.varExp("word", Set("word"))
            )
          )
        )
      )
    )
  }

  test("""LET a = 10
         |RETURN allReduce(acc = 0, x IN [1, 3, a] | acc + x, acc < 10) AS red""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET a = 10
              |RETURN allReduce(acc = 0, x IN [1, 3, a] | acc + x, acc < 10) AS red""".stripMargin),
        Outgoing(variables = Set("red")),
        ExpectedResult.TableResult("red"),
        ExpectedWorkingScope(
          Ast("LET a = 10"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("10")
        ),
        ExpectedWorkingScope(
          Ast("RETURN allReduce(acc = 0, x IN [1, 3, a] | acc + x, acc < 10) AS red"),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("red")),
          ExpectedResult.TableResult("red"),
          ExpectedWorkingScope(
            Ast("allReduce(acc = 0, x IN [1, 3, a] | acc + x, acc < 10)"),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope.constExp("0", Set("a")),
            ExpectedWorkingScope(
              Ast("[1, 3, a]"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", Set("a"))
            ),
            ExpectedWorkingScope(
              Ast("allReduceScope(acc, reductionStep(x | acc + x, acc < 10))"),
              Incoming(constants = Set("a", "acc")),
              Declared(constants = Seq("acc")),
              ExpectedWorkingScope(
                Ast("reductionStep(x | acc + x, acc < 10)"),
                Incoming(constants = Set("a", "acc", "x")),
                Declared(constants = Seq("x")),
                Referenced(Set("acc")),
                ExpectedWorkingScope(
                  Ast("acc + x"),
                  Incoming(constants = Set("a", "acc", "x")),
                  Referenced(Set("acc", "x")),
                  ExpectedWorkingScope.varExp("acc", Set("a", "acc", "x")),
                  ExpectedWorkingScope.varExp("x", Set("a", "acc", "x"))
                ),
                ExpectedWorkingScope(
                  Ast("acc < 10"),
                  Incoming(constants = Set("a", "acc", "x")),
                  Referenced(Set("acc")),
                  ExpectedWorkingScope.varExp("acc", Set("a", "acc", "x"))
                )
              )
            )
          )
        )
      )
    )
  }

  test("""LET x = 1
         |RETURN [(a:A WHERE a.prop > x)-[r]-(b:B) | a.prop] AS result""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""LET x = 1
              |RETURN [(a:A WHERE a.prop > x)-[r]-(b:B) | a.prop] AS result""".stripMargin),
        Outgoing(variables = Set("result")),
        ExpectedResult.TableResult("result"),
        ExpectedWorkingScope(
          Ast("LET x = 1"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("RETURN [(a:A WHERE a.prop > x)-[r]-(b:B) | a.prop] AS result"),
          Incoming(variables = Set("x")),
          Outgoing(variables = Set("result")),
          Referenced(Set("x")),
          ExpectedResult.TableResult("result"),
          ExpectedWorkingScope(
            Ast("[(a:A WHERE a.prop > x)-[r]-(b:B) | a.prop]"),
            Incoming(constants = Set("x")),
            Declared(constants = Seq("a", "r", "b")),
            Referenced(Set("x")),
            ExpectedWorkingScope(
              Ast("(a:A WHERE a.prop > x)-[r]-(b:B)"),
              PatternIncoming(topology = Set("x"), predicate = Set("x", "a", "r", "b")),
              Declared(variables = Seq("a", "r", "b")),
              Outgoing(variables = Set("a", "r", "b")),
              ExpectedResult.TableResult("a", "r", "b"),
              Referenced(Set("x")),
              ExpectedWorkingScope(
                Ast("(a:A WHERE a.prop > x)"),
                PatternIncoming(topology = Set("x"), predicate = Set("x", "a", "r", "b")),
                ExpectedResult.TableResult("a"),
                Declared(variables = Seq("a")),
                Outgoing(variables = Set("a")),
                Referenced(Set("x")),
                ExpectedWorkingScope.constExp("A", Set("x", "a", "r", "b")),
                ExpectedWorkingScope(
                  Ast("a.prop > x"),
                  Incoming(constants = Set("x", "a", "r", "b")),
                  Referenced(Set("a", "x")),
                  ExpectedWorkingScope.varExp("a", Set("x", "a", "r", "b")),
                  ExpectedWorkingScope.varExp("x", Set("x", "a", "r", "b"))
                )
              ),
              ExpectedWorkingScope(
                Ast("-[r]-"),
                PatternIncoming(topology = Set("x", "a"), predicate = Set("x", "a", "r", "b")),
                Declared(variables = Seq("r")),
                Outgoing(variables = Set("r")),
                ExpectedResult.TableResult("r")
              ),
              ExpectedWorkingScope(
                Ast("(b:B)"),
                PatternIncoming(topology = Set("x", "a", "r"), predicate = Set("x", "a", "r", "b")),
                Declared(variables = Seq("b")),
                Outgoing(variables = Set("b")),
                ExpectedResult.TableResult("b"),
                ExpectedWorkingScope.constExp("B", Set("x", "a", "r", "b"))
              )
            ),
            ExpectedWorkingScope(
              Ast("a.prop"),
              Incoming(constants = Set("x", "a", "r", "b")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", Set("x", "a", "r", "b"))
            )
          )
        )
      )
    )
  }

  test("SHOW USERS YIELD user, suspended RETURN count(user) as custom, suspended") {
    hasScope(
      ExpectedWorkingScope(
        Ast("SHOW USERS YIELD user, suspended RETURN count(user) AS custom, suspended"),
        Outgoing(variables = Set("custom", "suspended")),
        Referenced(Set("suspended", "user")),
        ExpectedResult.TableResult("custom", "suspended"),
        ExpectedWorkingScope(
          Ast("SHOW USERS"),
          Declared(variables = Seq("user", "roles", "passwordChangeRequired", "suspended", "home", "tags")),
          Outgoing(variables = Set("user", "roles", "passwordChangeRequired", "suspended", "home", "tags")),
          ExpectedResult.TableResult("user", "roles", "passwordChangeRequired", "suspended", "home", "tags")
        ),
        ExpectedWorkingScope(
          Ast("YIELD user, suspended"),
          Incoming(variables = Set("suspended", "user", "roles", "passwordChangeRequired", "home", "tags")),
          Referenced(Set("suspended", "user")),
          Outgoing(variables = Set("suspended", "user")),
          ExpectedResult.TableResult("suspended", "user"),
          ExpectedWorkingScope.varExp(
            "user",
            Set("suspended", "user", "roles", "passwordChangeRequired", "home", "tags")
          ),
          ExpectedWorkingScope.varExp(
            "suspended",
            Set("suspended", "user", "roles", "passwordChangeRequired", "home", "tags")
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN count(user) AS custom, suspended"),
          Incoming(variables = Set("suspended", "user")),
          Referenced(Set("suspended", "user")),
          Outgoing(variables = Set("custom", "suspended")),
          ExpectedResult.TableResult("custom", "suspended"),
          ExpectedWorkingScope.varProjExp(
            "suspended",
            incomingConstants = Set("suspended", "user"),
            incomingKeys = Set(gk("suspended"))
          ),
          ExpectedWorkingScope(
            Ast("count(user)"),
            ProjectionIncoming(variables = Set("suspended", "user"), groupingKeys = Set(gk("suspended"))),
            Referenced(Set("user")),
            ExpectedWorkingScope.varExp("user", Set("suspended", "user"))
          )
        )
      )
    )
  }

  test("SHOW ALL ROLES WHERE role = \"PUBLIC\"") {
    hasScope(
      ExpectedWorkingScope(
        Ast("SHOW ALL ROLES WHERE role = \"PUBLIC\""),
        Outgoing(variables = Set("role")),
        Referenced(Set("role")),
        ExpectedResult.TableResult("role"),
        ExpectedWorkingScope(
          Ast("SHOW ALL ROLES"),
          Declared(variables = Seq("role")),
          Outgoing(variables = Set("role")),
          ExpectedResult.TableResult("role")
        ),
        ExpectedWorkingScope(
          Ast("role = \"PUBLIC\""),
          Incoming(constants = Set("role")),
          Referenced(Set("role")),
          ExpectedWorkingScope.varExp("role", Set("role"))
        )
      )
    )
  }

  test("SHOW ALIAS $alias FOR DATABASE YIELD *") {
    val aliasCols =
      Seq("name", "composite", "database", "location", "url", "user", "driver", "defaultLanguage", "properties")
    hasScope(
      ExpectedWorkingScope(
        Ast("SHOW ALIAS $alias FOR DATABASE YIELD *"),
        Outgoing(variables = aliasCols.toSet),
        ExpectedResult.TableResult(aliasCols: _*),
        ExpectedWorkingScope(
          Ast("SHOW ALIAS $alias FOR DATABASE"),
          Declared(variables = aliasCols),
          Outgoing(variables = aliasCols.toSet),
          ExpectedResult.TableResult(aliasCols: _*)
        ),
        ExpectedWorkingScope(
          Ast("YIELD *"),
          Incoming(variables = aliasCols.toSet),
          Outgoing(variables = aliasCols.toSet),
          ExpectedResult.TableResult(aliasCols: _*)
        )
      )
    )
  }

  test("""CREATE DATABASE slow WAIT 5 SECONDS""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""CREATE DATABASE slow WAIT 5 SECONDS""".stripMargin),
        ExpectedResult.OmittedResult
      )
    )

    hasScope(
      ExpectedWorkingScope(
        Ast("""CREATE DATABASE slow WAIT 5 SECONDS""".stripMargin),
        Declared(constants = Seq.empty, variables = Seq("address", "state", "message", "success")),
        Outgoing(variables = Set("address", "state", "message", "success")),
        ExpectedResult.TableResult("address", "state", "message", "success")
      ),
      version = CypherVersion.Cypher5,
      skipVariableChecker = false
    )
  }

  test("""CREATE VECTOR INDEX moviePlots IF NOT EXISTS
         |FOR (m:Movie) ON (m.embedding)
         |WITH [m.releaseDate, m.rating]
         |OPTIONS {indexConfig: {`vector.similarity_function`: cosine}}""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""CREATE VECTOR INDEX moviePlots IF NOT EXISTS
              |FOR (m:Movie) ON (m.embedding)
              |WITH [m.releaseDate, m.rating]
              |OPTIONS {indexConfig: {`vector.similarity_function`: cosine}}""".stripMargin),
        Referenced(Set("cosine")),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("""m.embedding""".stripMargin),
          Incoming(constants = Set("m")),
          Referenced(Set("m")),
          ExpectedWorkingScope.varExp("m", Set("m"))
        ),
        ExpectedWorkingScope(
          Ast("""{indexConfig: {`vector.similarity_function`: cosine}}""".stripMargin),
          Referenced(Set("cosine")),
          ExpectedWorkingScope.varExp("cosine", Set.empty)
        )
      ),
      version = CypherVersion.Cypher25,
      skipVariableChecker = true
    )
  }

  test("""CREATE INDEX movies IF NOT EXISTS
         |FOR (m:Movie) ON (m.title, m.year)
         |OPTIONS {constraintConfig: {foo: "bar"}}""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""CREATE INDEX movies IF NOT EXISTS
              |FOR (m:Movie) ON (m.title, m.year)
              |OPTIONS {constraintConfig: {foo: "bar"}}""".stripMargin),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("""m.title""".stripMargin),
          Incoming(constants = Set("m")),
          Referenced(Set("m")),
          ExpectedWorkingScope.varExp("m", Set("m"))
        ),
        ExpectedWorkingScope(
          Ast("""m.year""".stripMargin),
          Incoming(constants = Set("m")),
          Referenced(Set("m")),
          ExpectedWorkingScope.varExp("m", Set("m"))
        ),
        ExpectedWorkingScope(
          Ast("""{constraintConfig: {foo: "bar"}}""".stripMargin)
        )
      )
    )
  }

  test("""CREATE CONSTRAINT movies IF NOT EXISTS
         |FOR (m:Movie) REQUIRE (m.title, m.year) IS KEY
         |OPTIONS {constraintConfig: {foo: "bar"}}""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""CREATE CONSTRAINT movies IF NOT EXISTS
              |FOR (m:Movie) REQUIRE (m.title, m.year) IS KEY
              |OPTIONS {constraintConfig: {foo: "bar"}}""".stripMargin),
        ExpectedResult.OmittedResult,
        ExpectedWorkingScope(
          Ast("""m.title""".stripMargin),
          Incoming(constants = Set("m")),
          Referenced(Set("m")),
          ExpectedWorkingScope.varExp("m", Set("m"))
        ),
        ExpectedWorkingScope(
          Ast("""m.year""".stripMargin),
          Incoming(constants = Set("m")),
          Referenced(Set("m")),
          ExpectedWorkingScope.varExp("m", Set("m"))
        ),
        ExpectedWorkingScope(
          Ast("""{constraintConfig: {foo: "bar"}}""".stripMargin)
        )
      )
    )
  }

  test("""MATCH (a)
         |RETURN EXISTS {
         |  MATCH (a)
         |  RETURN a AS b
         |  NEXT
         |  RETURN a
         |} AS x""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""MATCH (a)
              |RETURN EXISTS {
              |  MATCH (a)
              |  RETURN a AS b
              |  NEXT
              |  RETURN a
              |} AS x""".stripMargin),
        Outgoing(variables = Set("x")),
        ExpectedResult.TableResult("x"),
        ExpectedWorkingScope(
          Ast("""MATCH (a)""".stripMargin),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope(
            Ast("""(a)""".stripMargin),
            PatternIncoming(predicate = Set("a")),
            Declared(variables = Seq("a")),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a")
          )
        ),
        ExpectedWorkingScope(
          Ast("""RETURN EXISTS {
                |  MATCH (a)
                |  RETURN a AS b
                |  NEXT
                |  RETURN a
                |} AS x""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("""EXISTS {
                  |  MATCH (a)
                  |  RETURN a AS b
                  |  NEXT
                  |  RETURN a
                  |}""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("""MATCH (a)
                    |RETURN a AS b
                    |NEXT
                    |RETURN a""".stripMargin),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedResult.TableResult("a"),
              ExpectedWorkingScope(
                Ast("""MATCH (a)
                      |RETURN a AS b""".stripMargin),
                Incoming(constants = Set("a")),
                Referenced(Set("a")),
                Outgoing(variables = Set("b")),
                ExpectedResult.TableResult("b"),
                ExpectedWorkingScope(
                  Ast("""MATCH (a)""".stripMargin),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  Outgoing(constants = Set("a")),
                  ExpectedWorkingScope(
                    Ast("""(a)""".stripMargin),
                    PatternIncoming(topology = Set("a"), predicate = Set("a")),
                    Referenced(Set("a")),
                    Outgoing(variables = Set("a")),
                    ExpectedResult.TableResult("a")
                  )
                ),
                ExpectedWorkingScope(
                  Ast("""RETURN a AS b""".stripMargin),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  Outgoing(variables = Set("b")),
                  ExpectedResult.TableResult("b"),
                  ExpectedWorkingScope.varExp("a", Set("a"))
                )
              ),
              ExpectedWorkingScope(
                Ast("YIELD b AS b"),
                Incoming(constants = Set("a")),
                Declared(variables = Seq("b")),
                Outgoing(constants = Set("a"), variables = Set("b"))
              ),
              ExpectedWorkingScope(
                Ast("""RETURN a""".stripMargin), // query level
                Incoming(constants = Set("a"), variables = Set("b")),
                Referenced(Set("a")),
                ExpectedResult.TableResult("a"),
                ExpectedWorkingScope(
                  Ast("""RETURN a""".stripMargin),
                  Incoming(constants = Set("a"), variables = Set("b")),
                  Referenced(Set("a")),
                  ExpectedResult.TableResult("a"),
                  ExpectedWorkingScope.varExp("a", Set("a", "b"))
                )
              )
            )
          )
        )
      )
    )
  }

  test("""UNWIND [1, 2, 3] AS x
         |RETURN COLLECT {
         |  UNWIND [1, 2] AS y
         |  RETURN y
         |  NEXT
         |  RETURN x + COUNT(y)
         |} AS y""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""UNWIND [1, 2, 3] AS x
              |RETURN COLLECT {
              |  UNWIND [1, 2] AS y
              |  RETURN y
              |  NEXT
              |  RETURN x + COUNT(y)
              |} AS y""".stripMargin),
        Outgoing(variables = Set("y")),
        ExpectedResult.TableResult("y"),
        ExpectedWorkingScope(
          Ast("""UNWIND [1, 2, 3] AS x""".stripMargin),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("[1, 2, 3]")
        ),
        ExpectedWorkingScope(
          Ast("""RETURN COLLECT {
                |  UNWIND [1, 2] AS y
                |  RETURN y
                |  NEXT
                |  RETURN x + COUNT(y)
                |} AS y""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Outgoing(variables = Set("y")),
          ExpectedResult.TableResult("y"),
          ExpectedWorkingScope(
            Ast("""COLLECT {
                  |  UNWIND [1, 2] AS y
                  |  RETURN y
                  |  NEXT
                  |  RETURN x + COUNT(y)
                  |}""".stripMargin),
            Incoming(constants = Set("x")),
            Referenced(Set("x")),
            ExpectedWorkingScope(
              Ast("""UNWIND [1, 2] AS y
                    |RETURN y
                    |NEXT
                    |RETURN x + COUNT(y)""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              Outgoing(variables = Set("x + COUNT(y)")),
              ExpectedResult.TableResult("x + COUNT(y)"),
              ExpectedWorkingScope(
                Ast("""UNWIND [1, 2] AS y
                      |RETURN y""".stripMargin),
                Incoming(constants = Set("x")),
                Outgoing(variables = Set("y")),
                ExpectedResult.TableResult("y"),
                ExpectedWorkingScope(
                  Ast("""UNWIND [1, 2] AS y""".stripMargin),
                  Incoming(constants = Set("x")),
                  Declared(variables = Seq("y")),
                  Outgoing(constants = Set("x"), variables = Set("y")),
                  ExpectedWorkingScope.constExp("[1, 2]", Set("x"))
                ),
                ExpectedWorkingScope(
                  Ast("""RETURN y""".stripMargin),
                  Incoming(constants = Set("x"), variables = Set("y")),
                  Referenced(Set("y")),
                  Outgoing(variables = Set("y")),
                  ExpectedResult.TableResult("y"),
                  ExpectedWorkingScope.varExp("y", Set("x", "y"))
                )
              ),
              ExpectedWorkingScope(
                Ast("YIELD y AS y"),
                Incoming(constants = Set("x")),
                Declared(variables = Seq("y")),
                Outgoing(constants = Set("x"), variables = Set("y"))
              ),
              ExpectedWorkingScope(
                Ast("""RETURN x + COUNT(y)""".stripMargin), // query level
                Incoming(constants = Set("x"), variables = Set("y")),
                Referenced(Set("x", "y")),
                Outgoing(variables = Set("x + COUNT(y)")),
                ExpectedResult.TableResult("x + COUNT(y)"),
                ExpectedWorkingScope(
                  Ast("""RETURN x + COUNT(y)""".stripMargin),
                  Incoming(constants = Set("x"), variables = Set("y")),
                  Referenced(Set("x", "y")),
                  Outgoing(variables = Set("x + COUNT(y)")),
                  ExpectedResult.TableResult("x + COUNT(y)"),
                  ExpectedWorkingScope(
                    Ast("x + COUNT(y)"),
                    ProjectionIncoming(
                      constants = Set("x"),
                      variables = Set("y")
                    ),
                    Referenced(Set("x", "y")),
                    ExpectedWorkingScope.varProjExp("x", Set("x"), Set("y")),
                    ExpectedWorkingScope(
                      Ast("COUNT(y)"),
                      ProjectionIncoming(
                        constants = Set("x"),
                        variables = Set("y")
                      ),
                      Referenced(Set("y")),
                      ExpectedWorkingScope.varExp("y", Set("x", "y"))
                    )
                  )
                )
              )
            )
          )
        )
      )
    )
  }

  test("""CREATE (a)
         |
         |NEXT
         |
         |MATCH (n)
         |RETURN *, n.x AS x""".stripMargin) {

    hasScope(
      ExpectedWorkingScope(
        Ast("""CREATE (a)
              |
              |NEXT
              |
              |MATCH (n)
              |RETURN *, n.x AS x""".stripMargin),
        Outgoing(variables = Set("n", "x")),
        ExpectedResult.TableResult("n", "x"),
        ExpectedWorkingScope(
          Ast("""CREATE (a)""".stripMargin),
          Outgoing(variables = Set("a")),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("""CREATE (a)""".stripMargin),
            Declared(variables = Seq("a")),
            Outgoing(variables = Set("a")),
            ExpectedResult.OmittedResult,
            ExpectedWorkingScope(
              Ast("(a)"),
              Declared(variables = Seq("a")),
              Outgoing(variables = Set("a")),
              ExpectedResult.TableResult("a")
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("YIELD"),
          Outgoing(constants = Set(), variables = Set())
        ),
        ExpectedWorkingScope(
          Ast("""MATCH (n)
                |RETURN *, n.x AS x""".stripMargin),
          Outgoing(variables = Set("n", "x")),
          ExpectedResult.TableResult("n", "x"),
          ExpectedWorkingScope(
            Ast("""MATCH (n)""".stripMargin),
            Declared(variables = Seq("n")),
            Outgoing(variables = Set("n")),
            ExpectedWorkingScope(
              Ast("(n)"),
              PatternIncoming(predicate = Set("n")),
              Declared(variables = Seq("n")),
              Outgoing(variables = Set("n")),
              ExpectedResult.TableResult("n")
            )
          ),
          ExpectedWorkingScope(
            Ast("""RETURN *, n.x AS x""".stripMargin),
            Incoming(variables = Set("n")),
            Outgoing(variables = Set("n", "x")),
            Referenced(Set("n")),
            ExpectedResult.TableResult("n", "x"),
            ExpectedWorkingScope(
              Ast("n.x"),
              Incoming(constants = Set("n")),
              Referenced(Set("n")),
              ExpectedWorkingScope.varExp("n", Set("n"))
            )
          )
        )
      )
    )
  }

  test("""WITH 1 AS x
         |CALL {
         |  WITH x
         |  WITH x, 2 AS y
         |  CALL {
         |    WITH x, y
         |    WITH x, y, 4 AS z
         |    RETURN z
         |  }
         |  RETURN y, z
         |}
         |RETURN x, y, z""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x
              |CALL {
              |  WITH x
              |  WITH x, 2 AS y
              |  CALL {
              |    WITH x, y
              |    WITH x, y, 4 AS z
              |    RETURN z
              |  }
              |  RETURN y, z
              |}
              |RETURN x, y, z""".stripMargin),
        Outgoing(variables = Set("x", "y", "z")),
        ExpectedResult.TableResult("x", "y", "z"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  WITH x
                |  WITH x, 2 AS y
                |  CALL {
                |    WITH x, y
                |    WITH x, y, 4 AS z
                |    RETURN z
                |  }
                |  RETURN y, z
                |}""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("y", "z")),
          Outgoing(variables = Set("x", "y", "z")),
          ExpectedWorkingScope(
            Ast("""WITH x
                  |WITH x, 2 AS y
                  |CALL {
                  |  WITH x, y
                  |  WITH x, y, 4 AS z
                  |  RETURN z
                  |}
                  |RETURN y, z""".stripMargin),
            Incoming(variables = Set("x")),
            Referenced(Set("x")),
            Outgoing(variables = Set("y", "z")),
            ExpectedResult.TableResult("y", "z"),
            InImportingWith(),
            ExpectedWorkingScope(
              Ast("WITH x"),
              Incoming(variables = Set("x")),
              Referenced(Set("x")),
              Outgoing(variables = Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x")),
              ExpectedWorkingScope(
                Ast("WITH x, 2 AS y"),
                Incoming(variables = Set("x")),
                Referenced(Set("x")),
                Declared(variables = Seq("y")),
                Outgoing(variables = Set("x", "y")),
                ExpectedWorkingScope.varExp("x", Set("x")),
                ExpectedWorkingScope.constExp("2", Set("x"))
              ),
              ExpectedWorkingScope(
                Ast("""CALL {
                      |  WITH x, y
                      |  WITH x, y, 4 AS z
                      |  RETURN z
                      |}""".stripMargin),
                Incoming(variables = Set("x", "y")),
                Referenced(Set("x", "y")),
                Declared(variables = Seq("z")),
                Outgoing(variables = Set("x", "y", "z")),
                ExpectedWorkingScope(
                  Ast("""WITH x, y
                        |WITH x, y, 4 AS z
                        |RETURN z""".stripMargin),
                  Incoming(variables = Set("x", "y")),
                  Referenced(Set("x", "y")),
                  Outgoing(variables = Set("z")),
                  ExpectedResult.TableResult("z"),
                  InImportingWith(),
                  ExpectedWorkingScope(
                    Ast("WITH x, y"),
                    Incoming(variables = Set("x", "y")),
                    Referenced(Set("x", "y")),
                    Outgoing(variables = Set("x", "y")),
                    ExpectedWorkingScope.varExp("x", Set("x", "y")),
                    ExpectedWorkingScope.varExp("y", Set("x", "y")),
                    ExpectedWorkingScope(
                      Ast("WITH x, y, 4 AS z"),
                      Incoming(variables = Set("x", "y")),
                      Referenced(Set("x", "y")),
                      Declared(variables = Seq("z")),
                      Outgoing(variables = Set("x", "y", "z")),
                      ExpectedWorkingScope.varExp("x", Set("x", "y")),
                      ExpectedWorkingScope.varExp("y", Set("x", "y")),
                      ExpectedWorkingScope.constExp("4", Set("x", "y"))
                    ),
                    ExpectedWorkingScope(
                      Ast("RETURN z"),
                      Incoming(variables = Set("x", "y", "z")),
                      Referenced(Set("z")),
                      Outgoing(variables = Set("z")),
                      ExpectedResult.TableResult("z"),
                      ExpectedWorkingScope.varExp("z", Set("x", "y", "z"))
                    )
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("RETURN y, z"),
                Incoming(variables = Set("x", "y", "z")),
                Referenced(Set("y", "z")),
                Outgoing(variables = Set("y", "z")),
                ExpectedResult.TableResult("y", "z"),
                ExpectedWorkingScope.varExp("y", Set("x", "y", "z")),
                ExpectedWorkingScope.varExp("z", Set("x", "y", "z"))
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y, z"),
          Incoming(variables = Set("x", "y", "z")),
          Referenced(Set("x", "y", "z")),
          Outgoing(variables = Set("x", "y", "z")),
          ExpectedResult.TableResult("x", "y", "z"),
          ExpectedWorkingScope.varExp("x", Set("x", "y", "z")),
          ExpectedWorkingScope.varExp("y", Set("x", "y", "z")),
          ExpectedWorkingScope.varExp("z", Set("x", "y", "z"))
        )
      )
    )
  }

  test("""WITH 1 AS x
         |CALL {
         |  WITH x
         |  RETURN x AS y
         |  UNION
         |  WITH x
         |  RETURN x AS y
         |}
         |RETURN x, y""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x
              |CALL {
              |  WITH x
              |  RETURN x AS y
              |  UNION
              |  WITH x
              |  RETURN x AS y
              |}
              |RETURN x, y""".stripMargin),
        Outgoing(variables = Set("x", "y")),
        ExpectedResult.TableResult("x", "y"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x"),
          Declared(variables = Seq("x")),
          Outgoing(variables = Set("x")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  WITH x
                |  RETURN x AS y
                |  UNION
                |  WITH x
                |  RETURN x AS y
                |}""".stripMargin),
          Incoming(variables = Set("x")),
          Referenced(Set("x")),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope(
            Ast("""WITH x
                  |RETURN x AS y
                  |UNION
                  |WITH x
                  |RETURN x AS y""".stripMargin),
            Incoming(variables = Set("x")),
            Referenced(Set("x")),
            Declared(variables = Seq("y")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope(
              Ast("""WITH x
                    |RETURN x AS y""".stripMargin),
              Incoming(variables = Set("x")),
              Referenced(Set("x")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              InImportingWith(),
              ExpectedWorkingScope(
                Ast("WITH x"),
                Incoming(variables = Set("x")),
                Referenced(Set("x")),
                Outgoing(variables = Set("x")),
                ExpectedWorkingScope.varExp("x", Set("x")),
                ExpectedWorkingScope(
                  Ast("RETURN x AS y"),
                  Incoming(variables = Set("x")),
                  Referenced(Set("x")),
                  Outgoing(variables = Set("y")),
                  ExpectedResult.TableResult("y"),
                  ExpectedWorkingScope.varExp("x", Set("x"))
                )
              )
            ),
            ExpectedWorkingScope(
              Ast("""WITH x
                    |RETURN x AS y""".stripMargin),
              Incoming(variables = Set("x")),
              Referenced(Set("x")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              InImportingWith(),
              ExpectedWorkingScope(
                Ast("WITH x"),
                Incoming(variables = Set("x")),
                Referenced(Set("x")),
                Outgoing(variables = Set("x")),
                ExpectedWorkingScope.varExp("x", Set("x")),
                ExpectedWorkingScope(
                  Ast("RETURN x AS y"),
                  Incoming(variables = Set("x")),
                  Referenced(Set("x")),
                  Outgoing(variables = Set("y")),
                  ExpectedResult.TableResult("y"),
                  ExpectedWorkingScope.varExp("x", Set("x"))
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y"),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedResult.TableResult("x", "y"),
          ExpectedWorkingScope.varExp("x", Set("x", "y")),
          ExpectedWorkingScope.varExp("y", Set("x", "y"))
        )
      )
    )
  }

  test("""WITH 1 AS a
         |CALL (*) {
         |  RETURN a AS b
         |  UNION ALL
         |  RETURN a + 1 AS b
         |  UNION ALL
         |  RETURN a + 2 AS b
         |}
         |RETURN a, b""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS a
              |CALL (*) {
              |  RETURN a AS b
              |  UNION ALL
              |  RETURN a + 1 AS b
              |  UNION ALL
              |  RETURN a + 2 AS b
              |}
              |RETURN a, b""".stripMargin),
        Outgoing(variables = Set("a", "b")),
        ExpectedResult.TableResult("a", "b"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS a"),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedWorkingScope.constExp("1")
        ),
        ExpectedWorkingScope(
          Ast("""CALL (*) {
                |  RETURN a AS b
                |  UNION ALL
                |  RETURN a + 1 AS b
                |  UNION ALL
                |  RETURN a + 2 AS b
                |}""".stripMargin),
          Incoming(variables = Set("a")),
          Referenced(Set("a")),
          Declared(variables = Seq("b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedWorkingScope(
            Ast("""RETURN a AS b
                  |  UNION ALL
                  |RETURN a + 1 AS b
                  |  UNION ALL
                  |RETURN a + 2 AS b""".stripMargin),
            Incoming(constants = Set("a")),
            Referenced(Set("a")),
            Declared(variables = Seq("b")),
            Outgoing(variables = Set("b")),
            ExpectedResult.TableResult("b"),
            ExpectedWorkingScope(
              Ast("""RETURN a AS b
                    |  UNION ALL
                    |RETURN a + 1 AS b""".stripMargin),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              Declared(variables = Seq("b")),
              Outgoing(variables = Set("b")),
              ExpectedResult.TableResult("b"),
              ExpectedWorkingScope(
                Ast("RETURN a AS b"),
                Incoming(constants = Set("a")),
                Referenced(Set("a")),
                Outgoing(variables = Set("b")),
                ExpectedResult.TableResult("b"),
                ExpectedWorkingScope(
                  Ast("RETURN a AS b"),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  Outgoing(variables = Set("b")),
                  ExpectedResult.TableResult("b"),
                  ExpectedWorkingScope.varExp("a", Set("a"))
                )
              ),
              ExpectedWorkingScope(
                Ast("RETURN a + 1 AS b"),
                Incoming(constants = Set("a")),
                Referenced(Set("a")),
                Outgoing(variables = Set("b")),
                ExpectedResult.TableResult("b"),
                ExpectedWorkingScope(
                  Ast("RETURN a + 1 AS b"),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  Outgoing(variables = Set("b")),
                  ExpectedResult.TableResult("b"),
                  ExpectedWorkingScope(
                    Ast("a + 1"),
                    Incoming(constants = Set("a")),
                    Referenced(Set("a")),
                    ExpectedResult.ExpressionResult,
                    ExpectedWorkingScope.varExp("a", Set("a"))
                  )
                )
              )
            ),
            ExpectedWorkingScope(
              Ast("RETURN a + 2 AS b"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              Outgoing(variables = Set("b")),
              ExpectedResult.TableResult("b"),
              ExpectedWorkingScope(
                Ast("RETURN a + 2 AS b"),
                Incoming(constants = Set("a")),
                Referenced(Set("a")),
                Outgoing(variables = Set("b")),
                ExpectedResult.TableResult("b"),
                ExpectedWorkingScope(
                  Ast("a + 2"),
                  Incoming(constants = Set("a")),
                  Referenced(Set("a")),
                  ExpectedResult.ExpressionResult,
                  ExpectedWorkingScope.varExp("a", Set("a"))
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a, b"),
          Incoming(variables = Set("a", "b")),
          Referenced(Set("a", "b")),
          Outgoing(variables = Set("a", "b")),
          ExpectedResult.TableResult("a", "b"),
          ExpectedWorkingScope.varExp("a", Set("a", "b")),
          ExpectedWorkingScope.varExp("b", Set("a", "b"))
        )
      )
    )
  }

  test("""WITH 1 AS x, 2 AS y
         |CALL {
         |  WITH *
         |  RETURN x + y AS a
         |  UNION
         |  WITH *
         |  RETURN x + y AS a
         |}
         |RETURN a""".stripMargin) {
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x, 2 AS y
              |CALL {
              |  WITH *
              |  RETURN x + y AS a
              |  UNION
              |  WITH *
              |  RETURN x + y AS a
              |}
              |RETURN a""".stripMargin),
        Outgoing(variables = Set("a")),
        ExpectedResult.TableResult("a"),
        ExpectedWorkingScope(
          Ast("WITH 1 AS x, 2 AS y"),
          Declared(variables = Seq("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedWorkingScope.constExp("1"),
          ExpectedWorkingScope.constExp("2")
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  WITH *
                |  RETURN x + y AS a
                |  UNION
                |  WITH *
                |  RETURN x + y AS a
                |}""".stripMargin),
          Incoming(variables = Set("x", "y")),
          Referenced(Set("x", "y")),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("x", "y", "a")),
          ExpectedWorkingScope(
            Ast("""WITH *
                  |RETURN x + y AS a
                  |UNION
                  |WITH *
                  |RETURN x + y AS a""".stripMargin),
            Incoming(variables = Set("x", "y")),
            Referenced(Set("x", "y")),
            Declared(variables = Seq("a")),
            Outgoing(variables = Set("a")),
            ExpectedResult.TableResult("a"),
            ExpectedWorkingScope(
              Ast("""WITH *
                    |RETURN x + y AS a""".stripMargin),
              Incoming(variables = Set("x", "y")),
              Referenced(Set("x", "y")),
              Outgoing(variables = Set("a")),
              ExpectedResult.TableResult("a"),
              InImportingWith(),
              ExpectedWorkingScope(
                Ast("WITH *"),
                Incoming(variables = Set("x", "y")),
                Outgoing(variables = Set("x", "y")),
                ExpectedWorkingScope(
                  Ast("RETURN x + y AS a"),
                  Incoming(variables = Set("x", "y")),
                  Referenced(Set("x", "y")),
                  Outgoing(variables = Set("a")),
                  ExpectedResult.TableResult("a"),
                  ExpectedWorkingScope(
                    Ast("x + y"),
                    Incoming(constants = Set("x", "y")),
                    Referenced(Set("x", "y")),
                    ExpectedResult.ExpressionResult,
                    ExpectedWorkingScope.varExp("x", Set("x", "y")),
                    ExpectedWorkingScope.varExp("y", Set("x", "y"))
                  )
                )
              )
            ),
            ExpectedWorkingScope(
              Ast("""WITH *
                    |RETURN x + y AS a""".stripMargin),
              Incoming(variables = Set("x", "y")),
              Referenced(Set("x", "y")),
              Outgoing(variables = Set("a")),
              ExpectedResult.TableResult("a"),
              InImportingWith(),
              ExpectedWorkingScope(
                Ast("WITH *"),
                Incoming(variables = Set("x", "y")),
                Outgoing(variables = Set("x", "y")),
                ExpectedWorkingScope(
                  Ast("RETURN x + y AS a"),
                  Incoming(variables = Set("x", "y")),
                  Referenced(Set("x", "y")),
                  Outgoing(variables = Set("a")),
                  ExpectedResult.TableResult("a"),
                  ExpectedWorkingScope(
                    Ast("x + y"),
                    Incoming(constants = Set("x", "y")),
                    Referenced(Set("x", "y")),
                    ExpectedResult.ExpressionResult,
                    ExpectedWorkingScope.varExp("x", Set("x", "y")),
                    ExpectedWorkingScope.varExp("y", Set("x", "y"))
                  )
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN a"),
          Incoming(variables = Set("x", "y", "a")),
          Referenced(Set("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a"),
          ExpectedWorkingScope.varExp("a", Set("x", "y", "a"))
        )
      )
    )
  }

  test(
    """MATCH (a)
      |WITH a.p AS p, a.p + sum(a.q) AS value
      |  GROUP BY p, a.x
      |  ORDER BY p ASCENDING, a.x ASCENDING
      |  WHERE a.x > 0
      |RETURN value, p""".stripMargin
  ) {
    hasScope(ExpectedWorkingScope(
      Ast("""MATCH (a)
            |WITH a.p AS p, a.p + sum(a.q) AS value
            |  GROUP BY p, a.x
            |  ORDER BY p ASCENDING, a.x ASCENDING
            |  WHERE a.x > 0
            |RETURN value, p""".stripMargin),
      Outgoing(variables = Set("value", "p")),
      ExpectedResult.TableResult("value", "p"),
      ExpectedWorkingScope(
        Ast("MATCH (a)"),
        Declared(variables = Seq("a")),
        Outgoing(variables = Set("a")),
        ExpectedWorkingScope(
          Ast("(a)"),
          PatternIncoming(predicate = Set("a")),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a")
        )
      ),
      ExpectedWorkingScope(
        Ast("""WITH a.p AS p, a.p + sum(a.q) AS value
              |  GROUP BY p, a.x
              |  ORDER BY p ASCENDING, a.x ASCENDING
              |  WHERE a.x > 0""".stripMargin),
        Incoming(variables = Set("a")),
        Referenced(Set("a")),
        Declared(variables = Seq("p", "value")),
        Outgoing(variables = Set("p", "value")),
        ExpectedWorkingScope.varProjExp(
          "a.p",
          incomingKeys = Set(gk("p"), gk("a.x")),
          referenced = Set("p")
        ),
        ExpectedWorkingScope(
          Ast("a.p + sum(a.q)"),
          ProjectionIncoming(variables = Set("a"), groupingKeys = Set(gk("p"), gk("a.x"))),
          Referenced(Set("a", "p")),
          ExpectedWorkingScope.varProjExp(
            "a.p",
            incomingVariables = Set("a"),
            incomingKeys = Set(gk("p"), gk("a.x")),
            referenced = Set("p")
          ),
          ExpectedWorkingScope(
            Ast("sum(a.q)"),
            ProjectionIncoming(variables = Set("a"), groupingKeys = Set(gk("p"), gk("a.x"))),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("a.q"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", incomingConstants = Set("a"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("GROUP BY p, a.x"),
          ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"), gk("a.x"))),
          Outgoing(constants = Set("a", "p", "value")),
          Referenced(Set("a", "p")),
          ExpectedWorkingScope(
            Ast("p"),
            ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"), gk("a.x"))),
            Referenced(Set("a", "p"))
          ),
          ExpectedWorkingScope(
            Ast("a.x"),
            ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"), gk("a.x"))),
            Referenced(Set("a")),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingConstants = Set("a", "p", "value"),
              incomingKeys = Set(gk("p"), gk("a.x"))
            )
          )
        ),
        ExpectedWorkingScope.varProjExp(
          "p",
          incomingConstants = Set("p", "value"),
          incomingKeys = Set(gk("p"), gk("a.x"))
        ),
        ExpectedWorkingScope(
          Ast("a.x"),
          ProjectionIncoming(constants = Set("p", "value"), groupingKeys = Set(gk("p"), gk("a.x"))),
          Referenced(Set.empty)
        ),
        ExpectedWorkingScope(
          Ast("a.x > 0"),
          ProjectionIncoming(constants = Set("p", "value"), groupingKeys = Set(gk("p"), gk("a.x"))),
          Referenced(Set.empty),
          ExpectedWorkingScope(
            Ast("a.x"),
            ProjectionIncoming(constants = Set("p", "value"), groupingKeys = Set(gk("p"), gk("a.x"))),
            Referenced(Set.empty)
          )
        )
      ),
      ExpectedWorkingScope(
        Ast("RETURN value, p"),
        Incoming(variables = Set("p", "value")),
        Referenced(Set("value", "p")),
        Outgoing(variables = Set("p", "value")),
        ExpectedResult.TableResult("value", "p"),
        ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("p", "value")),
        ExpectedWorkingScope.varProjExp("p", incomingConstants = Set("p", "value"))
      )
    ))
  }

  test(
    """MATCH (a)
      |WITH a.p AS p, a.p + sum(a.q) AS value
      |  GROUP BY ALL
      |  ORDER BY p ASCENDING, value ASCENDING
      |  WHERE value > 0
      |RETURN value, p""".stripMargin
  ) {
    hasScope(ExpectedWorkingScope(
      Ast("""MATCH (a)
            |WITH a.p AS p, a.p + sum(a.q) AS value
            |  GROUP BY ALL
            |  ORDER BY p ASCENDING, value ASCENDING
            |  WHERE value > 0
            |RETURN value, p""".stripMargin),
      Outgoing(variables = Set("value", "p")),
      ExpectedResult.TableResult("value", "p"),
      ExpectedWorkingScope(
        Ast("MATCH (a)"),
        Declared(variables = Seq("a")),
        Outgoing(variables = Set("a")),
        ExpectedWorkingScope(
          Ast("(a)"),
          PatternIncoming(predicate = Set("a")),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a")
        )
      ),
      ExpectedWorkingScope(
        Ast("""WITH a.p AS p, a.p + sum(a.q) AS value
              |  GROUP BY ALL
              |  ORDER BY p ASCENDING, value ASCENDING
              |  WHERE value > 0""".stripMargin),
        Incoming(variables = Set("a")),
        Referenced(Set("a")),
        Declared(variables = Seq("p", "value")),
        Outgoing(variables = Set("p", "value")),
        ExpectedWorkingScope.varProjExp(
          "a.p",
          incomingConstants = Set("a"),
          incomingKeys = Set(gk("p")),
          referenced = Set("p")
        ),
        ExpectedWorkingScope(
          Ast("a.p + sum(a.q)"),
          ProjectionIncoming(variables = Set("a"), groupingKeys = Set(gk("p"))),
          Referenced(Set("a", "p")),
          ExpectedWorkingScope.varProjExp(
            "a.p",
            incomingVariables = Set("a"),
            incomingKeys = Set(gk("p")),
            referenced = Set("p")
          ),
          ExpectedWorkingScope(
            Ast("sum(a.q)"),
            ProjectionIncoming(variables = Set("a"), groupingKeys = Set(gk("p"))),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("a.q"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", incomingConstants = Set("a"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("GROUP BY ALL"),
          ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"))),
          Outgoing(constants = Set("a", "p", "value")),
          Referenced(Set("a")),
          ExpectedWorkingScope(
            Ast("a.p"),
            ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"))),
            Referenced(Set("a")),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingConstants = Set("a", "p", "value"),
              incomingKeys = Set(gk("p"))
            )
          )
        ),
        ExpectedWorkingScope.varProjExp("p", incomingConstants = Set("p", "value"), incomingKeys = Set(gk("p"))),
        ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("p", "value"), incomingKeys = Set(gk("p"))),
        ExpectedWorkingScope(
          Ast("value > 0"),
          ProjectionIncoming(constants = Set("p", "value"), groupingKeys = Set(gk("p"))),
          Referenced(Set("value")),
          ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("p", "value"), incomingKeys = Set(gk("p")))
        )
      ),
      ExpectedWorkingScope(
        Ast("RETURN value, p"),
        Incoming(variables = Set("p", "value")),
        Referenced(Set("value", "p")),
        Outgoing(variables = Set("p", "value")),
        ExpectedResult.TableResult("value", "p"),
        ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("p", "value")),
        ExpectedWorkingScope.varProjExp("p", incomingConstants = Set("p", "value"))
      )
    ))
  }

  test(
    """MATCH (a)
      |WITH 1 + sum(a.q) AS value
      |  GROUP BY ()
      |  ORDER BY value ASCENDING
      |  WHERE value > 0
      |RETURN value""".stripMargin
  ) {
    hasScope(ExpectedWorkingScope(
      Ast("""MATCH (a)
            |WITH 1 + sum(a.q) AS value
            |  GROUP BY ()
            |  ORDER BY value ASCENDING
            |  WHERE value > 0
            |RETURN value""".stripMargin),
      Outgoing(variables = Set("value")),
      ExpectedResult.TableResult("value"),
      ExpectedWorkingScope(
        Ast("MATCH (a)"),
        Declared(variables = Seq("a")),
        Outgoing(variables = Set("a")),
        ExpectedWorkingScope(
          Ast("(a)"),
          PatternIncoming(predicate = Set("a")),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a")
        )
      ),
      ExpectedWorkingScope(
        Ast("""WITH 1 + sum(a.q) AS value
              |  GROUP BY ()
              |  ORDER BY value ASCENDING
              |  WHERE value > 0""".stripMargin),
        Incoming(variables = Set("a")),
        // TODO these should not be projected up here
        Referenced(Set("a")),
        Declared(variables = Seq("value")),
        Outgoing(variables = Set("value")),
        ExpectedWorkingScope(
          Ast("1 + sum(a.q)"),
          ProjectionIncoming(variables = Set("a")),
          Referenced(Set("a")),
          ExpectedWorkingScope(
            Ast("sum(a.q)"),
            ProjectionIncoming(variables = Set("a")),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("a.q"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", incomingConstants = Set("a"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("GROUP BY ()"),
          ProjectionIncoming(constants = Set("a", "value")),
          Outgoing(constants = Set("a", "value"))
        ),
        ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("value")),
        ExpectedWorkingScope(
          Ast("value > 0"),
          ProjectionIncoming(constants = Set("value")),
          Referenced(Set("value")),
          ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("value"))
        )
      ),
      ExpectedWorkingScope(
        Ast("RETURN value"),
        Incoming(variables = Set("value")),
        Referenced(Set("value")),
        Outgoing(variables = Set("value")),
        ExpectedResult.TableResult("value"),
        ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("value"))
      )
    ))
  }

  test(
    """MATCH (a)
      |WITH a.p AS p, a.p + sum(a.q) AS value
      |  GROUP BY p, a.x + 1
      |  ORDER BY p ASCENDING, a.x + 1 ASCENDING
      |  WHERE a.p > 0
      |RETURN value, p""".stripMargin
  ) {
    hasScope(ExpectedWorkingScope(
      Ast("""MATCH (a)
            |WITH a.p AS p, a.p + sum(a.q) AS value
            |  GROUP BY p, a.x + 1
            |  ORDER BY p ASCENDING, a.x + 1 ASCENDING
            |  WHERE a.p > 0
            |RETURN value, p""".stripMargin),
      Outgoing(variables = Set("value", "p")),
      ExpectedResult.TableResult("value", "p"),
      ExpectedWorkingScope(
        Ast("MATCH (a)"),
        Declared(variables = Seq("a")),
        Outgoing(variables = Set("a")),
        ExpectedWorkingScope(
          Ast("(a)"),
          PatternIncoming(predicate = Set("a")),
          Declared(variables = Seq("a")),
          Outgoing(variables = Set("a")),
          ExpectedResult.TableResult("a")
        )
      ),
      ExpectedWorkingScope(
        Ast("""WITH a.p AS p, a.p + sum(a.q) AS value
              |  GROUP BY p, a.x + 1
              |  ORDER BY p ASCENDING, a.x + 1 ASCENDING
              |  WHERE a.p > 0""".stripMargin),
        Incoming(variables = Set("a")),
        Referenced(Set("a")),
        Declared(variables = Seq("p", "value")),
        Outgoing(variables = Set("p", "value")),
        ExpectedWorkingScope.varProjExp(
          "a.p",
          incomingKeys = Set(gk("p"), gk("a.x + 1")),
          referenced = Set("p")
        ),
        ExpectedWorkingScope(
          Ast("a.p + sum(a.q)"),
          ProjectionIncoming(variables = Set("a"), groupingKeys = Set(gk("p"), gk("a.x + 1"))),
          Referenced(Set("a", "p")),
          ExpectedWorkingScope.varProjExp(
            "a.p",
            incomingVariables = Set("a"),
            incomingKeys = Set(gk("p"), gk("a.x + 1")),
            referenced = Set("p")
          ),
          ExpectedWorkingScope(
            Ast("sum(a.q)"),
            ProjectionIncoming(variables = Set("a"), groupingKeys = Set(gk("p"), gk("a.x + 1"))),
            Referenced(Set("a")),
            ExpectedWorkingScope(
              Ast("a.q"),
              Incoming(constants = Set("a")),
              Referenced(Set("a")),
              ExpectedWorkingScope.varExp("a", incomingConstants = Set("a"))
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("GROUP BY p, a.x + 1"),
          ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"), gk("a.x + 1"))),
          Outgoing(constants = Set("a", "p", "value")),
          Referenced(Set("a", "p")),
          ExpectedWorkingScope(
            Ast("p"),
            ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"), gk("a.x + 1"))),
            Referenced(Set("a", "p"))
          ),
          ExpectedWorkingScope(
            Ast("a.x + 1"),
            ProjectionIncoming(constants = Set("a", "p", "value"), groupingKeys = Set(gk("p"), gk("a.x + 1"))),
            Referenced(Set("a")),
            ExpectedWorkingScope.varProjExp(
              "a",
              incomingConstants = Set("a", "p", "value"),
              incomingKeys = Set(gk("p"), gk("a.x + 1"))
            )
          )
        ),
        ExpectedWorkingScope.varProjExp(
          "p",
          incomingConstants = Set("p", "value"),
          incomingKeys = Set(gk("p"), gk("a.x + 1"))
        ),
        ExpectedWorkingScope(
          Ast("a.x + 1"),
          ProjectionIncoming(constants = Set("p", "value"), groupingKeys = Set(gk("p"), gk("a.x + 1"))),
          Referenced(Set.empty)
        ),
        ExpectedWorkingScope(
          Ast("a.p > 0"),
          ProjectionIncoming(constants = Set("p", "value"), groupingKeys = Set(gk("p"), gk("a.x + 1"))),
          Referenced(Set("p")),
          ExpectedWorkingScope.varProjExp(
            "a.p",
            incomingConstants = Set("p", "value"),
            incomingKeys = Set(gk("p"), gk("a.x + 1")),
            referenced = Set("p")
          )
        )
      ),
      ExpectedWorkingScope(
        Ast("RETURN value, p"),
        Incoming(variables = Set("p", "value")),
        Referenced(Set("value", "p")),
        Outgoing(variables = Set("p", "value")),
        ExpectedResult.TableResult("value", "p"),
        ExpectedWorkingScope.varProjExp("value", incomingConstants = Set("p", "value")),
        ExpectedWorkingScope.varProjExp("p", incomingConstants = Set("p", "value"))
      )
    ))
  }

  test("""WITH 1 AS x, "g1" AS gn1, "g2" AS gn2
         |CALL {
         |  USE graph.byName(gn1)
         |  WITH x
         |  RETURN x AS y
         |  UNION
         |  WITH x, gn2
         |  USE graph.byName(gn2)
         |  RETURN x AS y
         |}
         |RETURN x, y""".stripMargin) {
    val outer = Set("x", "gn1", "gn2")
    hasScope(
      ExpectedWorkingScope(
        Ast("""WITH 1 AS x, "g1" AS gn1, "g2" AS gn2
              |CALL {
              |  USE graph.byName(gn1)
              |  WITH x
              |  RETURN x AS y
              |  UNION
              |  WITH x, gn2
              |  USE graph.byName(gn2)
              |  RETURN x AS y
              |}
              |RETURN x, y""".stripMargin),
        Outgoing(variables = Set("x", "y")),
        ExpectedResult.TableResult("x", "y"),
        ExpectedWorkingScope(
          Ast("""WITH 1 AS x, "g1" AS gn1, "g2" AS gn2"""),
          Declared(variables = Seq("x", "gn1", "gn2")),
          Outgoing(variables = outer),
          ExpectedWorkingScope.constExp("1"),
          ExpectedWorkingScope.constExp("\"g1\""),
          ExpectedWorkingScope.constExp("\"g2\"")
        ),
        ExpectedWorkingScope(
          Ast("""CALL {
                |  USE graph.byName(gn1)
                |  WITH x
                |  RETURN x AS y
                |  UNION
                |  WITH x, gn2
                |  USE graph.byName(gn2)
                |  RETURN x AS y
                |}""".stripMargin),
          Incoming(variables = outer),
          Referenced(Set("gn1", "gn2", "x")),
          Declared(variables = Seq("y")),
          Outgoing(variables = Set("x", "gn1", "gn2", "y")),
          ExpectedWorkingScope(
            Ast("""USE graph.byName(gn1)
                  |WITH x
                  |RETURN x AS y
                  |UNION
                  |WITH x, gn2
                  |USE graph.byName(gn2)
                  |RETURN x AS y""".stripMargin),
            Incoming(variables = outer),
            Referenced(Set("gn1", "gn2", "x")),
            Declared(variables = Seq("y")),
            Outgoing(variables = Set("y")),
            ExpectedResult.TableResult("y"),
            ExpectedWorkingScope(
              Ast("""USE graph.byName(gn1)
                    |WITH x
                    |RETURN x AS y""".stripMargin),
              Incoming(variables = outer),
              Referenced(Set("gn1", "x")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              InImportingWith(),
              ExpectedWorkingScope(
                Ast("USE graph.byName(gn1)"),
                Incoming(variables = outer),
                Referenced(Set("gn1")),
                Outgoing(variables = outer),
                ExpectedWorkingScope(
                  Ast("graph.byName(gn1)"), // graph reference
                  Incoming(constants = outer),
                  Referenced(Set("gn1")),
                  ExpectedResult.ExpressionResult,
                  ExpectedWorkingScope(
                    Ast("graph.byName(gn1)"), // function invocation
                    Incoming(constants = outer),
                    Referenced(Set("gn1")),
                    ExpectedResult.ExpressionResult,
                    ExpectedWorkingScope.varExp("gn1", outer)
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("WITH x"),
                Incoming(variables = outer),
                Referenced(Set("x")),
                Outgoing(variables = Set("x")),
                ExpectedWorkingScope.varExp("x", outer),
                ExpectedWorkingScope(
                  Ast("RETURN x AS y"),
                  Incoming(variables = Set("x")),
                  Referenced(Set("x")),
                  Outgoing(variables = Set("y")),
                  ExpectedResult.TableResult("y"),
                  ExpectedWorkingScope.varExp("x", Set("x"))
                )
              )
            ),
            ExpectedWorkingScope(
              Ast("""WITH x, gn2
                    |USE graph.byName(gn2)
                    |RETURN x AS y""".stripMargin),
              Incoming(variables = outer),
              Referenced(Set("gn2", "x")),
              Outgoing(variables = Set("y")),
              ExpectedResult.TableResult("y"),
              InImportingWith(),
              ExpectedWorkingScope(
                Ast("WITH x, gn2"),
                Incoming(variables = outer),
                Referenced(Set("x", "gn2")),
                Outgoing(variables = Set("x", "gn2")),
                ExpectedWorkingScope.varExp("x", outer),
                ExpectedWorkingScope.varExp("gn2", outer),
                ExpectedWorkingScope(
                  Ast("USE graph.byName(gn2)"),
                  Incoming(variables = Set("x", "gn2")),
                  Referenced(Set("gn2")),
                  Outgoing(variables = Set("x", "gn2")),
                  ExpectedWorkingScope(
                    Ast("graph.byName(gn2)"), // graph reference
                    Incoming(constants = Set("x", "gn2")),
                    Referenced(Set("gn2")),
                    ExpectedResult.ExpressionResult,
                    ExpectedWorkingScope(
                      Ast("graph.byName(gn2)"), // function invocation
                      Incoming(constants = Set("x", "gn2")),
                      Referenced(Set("gn2")),
                      ExpectedResult.ExpressionResult,
                      ExpectedWorkingScope.varExp("gn2", Set("x", "gn2"))
                    )
                  )
                ),
                ExpectedWorkingScope(
                  Ast("RETURN x AS y"),
                  Incoming(variables = Set("x", "gn2")),
                  Referenced(Set("x")),
                  Outgoing(variables = Set("y")),
                  ExpectedResult.TableResult("y"),
                  ExpectedWorkingScope.varExp("x", Set("x", "gn2"))
                )
              )
            )
          )
        ),
        ExpectedWorkingScope(
          Ast("RETURN x, y"),
          Incoming(variables = Set("x", "gn1", "gn2", "y")),
          Referenced(Set("x", "y")),
          Outgoing(variables = Set("x", "y")),
          ExpectedResult.TableResult("x", "y"),
          ExpectedWorkingScope.varExp("x", Set("x", "gn1", "gn2", "y")),
          ExpectedWorkingScope.varExp("y", Set("x", "gn1", "gn2", "y"))
        )
      )
    )
  }

}
