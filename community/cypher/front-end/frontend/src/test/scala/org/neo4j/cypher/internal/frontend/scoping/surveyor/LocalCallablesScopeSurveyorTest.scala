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
package org.neo4j.cypher.internal.frontend.scoping.surveyor

import org.neo4j.cypher.internal.frontend.scoping.VariableCheckingTestSuite

class LocalCallablesScopeSurveyorTest extends VariableCheckingTestSuite {

  test("""DEFINE PROCEDURE bar() {
         |  RETURN 1 AS one
         |}
         |
         |CALL bar()
         |RETURN one""".stripMargin) {
    val callableBar = Callable("bar", ExpectedResult.TableResult("one"))
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE PROCEDURE bar() {
                |  RETURN 1 AS one
                |}
                |
                |CALL bar()
                |RETURN one""".stripMargin),
          Outgoing(variables = Set("one")),
          ExpectedResult.TableResult("one"),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE bar() {
                  |  RETURN 1 AS one
                  |}""".stripMargin),
            Declared(localCallables = Seq(callableBar)),
            Outgoing(localCallables = Set(callableBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""RETURN 1 AS one""".stripMargin), // query level
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""RETURN 1 AS one""".stripMargin),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.constExp("1")
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("""CALL bar()
                  |RETURN one""".stripMargin),
            Incoming(localCallables = Set(callableBar)),
            Outgoing(variables = Set("one")),
            ExpectedResult.TableResult("one"),
            ExpectedWorkingScope(
              Ast("""CALL bar()""".stripMargin),
              Incoming(localCallables = Set(callableBar)),
              Outgoing(variables = Set("one"), localCallables = Set(callableBar)),
              Declared(variables = Seq("one")),
              ExpectedResult.NoResult
            ),
            ExpectedWorkingScope(
              Ast("""RETURN one""".stripMargin),
              Incoming(variables = Set("one"), localCallables = Set(callableBar)),
              Referenced(Set("one")),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope.varExp("one", Set("one"), incomingCallables = Set(callableBar))
            )
          )
        )
    )
  }

  test("""DEFINE PROCEDURE bar() {
         |  RETURN 1 AS one
         |}
         |
         |CALL bar() YIELD one AS x
         |RETURN x""".stripMargin) {
    val callableBar = Callable("bar", ExpectedResult.TableResult("one"))
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE PROCEDURE bar() {
                |  RETURN 1 AS one
                |}
                |
                |CALL bar() YIELD one AS x
                |RETURN x""".stripMargin),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE bar() {
                  |  RETURN 1 AS one
                  |}""".stripMargin),
            Declared(localCallables = Seq(callableBar)),
            Outgoing(localCallables = Set(callableBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""RETURN 1 AS one""".stripMargin), // query level
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""RETURN 1 AS one""".stripMargin),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.constExp("1")
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("""CALL bar() YIELD one AS x
                  |RETURN x""".stripMargin),
            Incoming(localCallables = Set(callableBar)),
            Outgoing(variables = Set("x")),
            ExpectedResult.TableResult("x"),
            ExpectedWorkingScope(
              Ast("""CALL bar() YIELD one AS x""".stripMargin),
              Incoming(localCallables = Set(callableBar)),
              Outgoing(variables = Set("x"), localCallables = Set(callableBar)),
              Declared(variables = Seq("x")),
              ExpectedResult.NoResult
            ),
            ExpectedWorkingScope(
              Ast("""RETURN x""".stripMargin),
              Incoming(variables = Set("x"), localCallables = Set(callableBar)),
              Referenced(Set("x")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope.varExp("x", Set("x"), incomingCallables = Set(callableBar))
            )
          )
        )
    )
  }

  test("""DEFINE PROCEDURE bar(x) {
         |  RETURN x AS one
         |}
         |
         |CALL bar(1)
         |RETURN one""".stripMargin) {
    val callableBar = Callable("bar", ExpectedResult.TableResult("one"))
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE PROCEDURE bar(x) {
                |  RETURN x AS one
                |}
                |
                |CALL bar(1)
                |RETURN one""".stripMargin),
          Outgoing(variables = Set("one")),
          ExpectedResult.TableResult("one"),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE bar(x) {
                  |  RETURN x AS one
                  |}""".stripMargin),
            Declared(localCallables = Seq(callableBar)),
            Outgoing(localCallables = Set(callableBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""RETURN x AS one""".stripMargin), // query level
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""RETURN x AS one""".stripMargin),
                Incoming(constants = Set("x")),
                Referenced(Set("x")),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.varExp("x", Set("x"))
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("""CALL bar(1)
                  |RETURN one""".stripMargin),
            Incoming(localCallables = Set(callableBar)),
            Outgoing(variables = Set("one")),
            ExpectedResult.TableResult("one"),
            ExpectedWorkingScope(
              Ast("""CALL bar(1)""".stripMargin),
              Incoming(localCallables = Set(callableBar)),
              Outgoing(variables = Set("one"), localCallables = Set(callableBar)),
              Declared(variables = Seq("one")),
              ExpectedResult.NoResult,
              ExpectedWorkingScope.constExp("1", incomingCallables = Set(callableBar))
            ),
            ExpectedWorkingScope(
              Ast("""RETURN one""".stripMargin),
              Incoming(variables = Set("one"), localCallables = Set(callableBar)),
              Referenced(Set("one")),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope.varExp("one", Set("one"), incomingCallables = Set(callableBar))
            )
          )
        )
    )
  }

  test("""DEFINE PROCEDURE bar(x) {
         |  RETURN x AS one
         |}
         |
         |LET a = 0
         |CALL bar(a + 1)
         |RETURN one""".stripMargin) {
    val callableBar = Callable("bar", ExpectedResult.TableResult("one"))
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE PROCEDURE bar(x) {
                |  RETURN x AS one
                |}
                |
                |LET a = 0
                |CALL bar(a + 1)
                |RETURN one""".stripMargin),
          Outgoing(variables = Set("one")),
          ExpectedResult.TableResult("one"),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE bar(x) {
                  |  RETURN x AS one
                  |}""".stripMargin),
            Declared(localCallables = Seq(callableBar)),
            Outgoing(localCallables = Set(callableBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""RETURN x AS one""".stripMargin), // query level
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""RETURN x AS one""".stripMargin),
                Incoming(constants = Set("x")),
                Referenced(Set("x")),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.varExp("x", Set("x"))
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("""LET a = 0
                  |CALL bar(a + 1)
                  |RETURN one""".stripMargin),
            Incoming(localCallables = Set(callableBar)),
            Outgoing(variables = Set("one")),
            ExpectedResult.TableResult("one"),
            ExpectedWorkingScope(
              Ast("""LET a = 0""".stripMargin),
              Incoming(localCallables = Set(callableBar)),
              Declared(variables = Seq("a")),
              Outgoing(variables = Set("a"), localCallables = Set(callableBar)),
              ExpectedResult.NoResult,
              ExpectedWorkingScope.constExp("0", incomingCallables = Set(callableBar))
            ),
            ExpectedWorkingScope(
              Ast("""CALL bar(a + 1)""".stripMargin),
              Incoming(variables = Set("a"), localCallables = Set(callableBar)),
              Referenced(Set("a")),
              Outgoing(variables = Set("a", "one"), localCallables = Set(callableBar)),
              Declared(variables = Seq("one")),
              ExpectedResult.NoResult,
              ExpectedWorkingScope(
                Ast("""a + 1""".stripMargin),
                Incoming(constants = Set("a"), localCallables = Set(callableBar)),
                Referenced(Set("a")),
                ExpectedWorkingScope.varExp("a", Set("a"), incomingCallables = Set(callableBar))
              )
            ),
            ExpectedWorkingScope(
              Ast("""RETURN one""".stripMargin),
              Incoming(variables = Set("a", "one"), localCallables = Set(callableBar)),
              Referenced(Set("one")),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope.varExp("one", Set("a", "one"), incomingCallables = Set(callableBar))
            )
          )
        )
    )
  }

  test("""DEFINE PROCEDURE foo() {
         |  RETURN 1 AS foo
         |}
         |DEFINE PROCEDURE bar() {
         |  CALL foo()
         |  RETURN foo AS one
         |}
         |
         |CALL bar()
         |RETURN one""".stripMargin) {
    val callableFoo = Callable("foo", ExpectedResult.TableResult("one"))
    val callableBar = Callable("bar", ExpectedResult.TableResult("one"))
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE PROCEDURE foo() {
                |  RETURN 1 AS foo
                |}
                |DEFINE PROCEDURE bar() {
                |  CALL foo()
                |  RETURN foo AS one
                |}
                |
                |CALL bar()
                |RETURN one""".stripMargin),
          Outgoing(variables = Set("one")),
          ExpectedResult.TableResult("one"),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE foo() {
                  |  RETURN 1 AS foo
                  |}""".stripMargin),
            Declared(localCallables = Seq(callableFoo)),
            Outgoing(localCallables = Set(callableFoo)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""RETURN 1 AS foo""".stripMargin), // query level
              Outgoing(variables = Set("foo")),
              ExpectedResult.TableResult("foo"),
              ExpectedWorkingScope(
                Ast("""RETURN 1 AS foo""".stripMargin),
                Outgoing(variables = Set("foo")),
                ExpectedResult.TableResult("foo"),
                ExpectedWorkingScope.constExp("1")
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE bar() {
                  |  CALL foo()
                  |  RETURN foo AS one
                  |}""".stripMargin),
            Incoming(localCallables = Set(callableFoo)),
            Declared(localCallables = Seq(callableBar)),
            Outgoing(localCallables = Set(callableFoo, callableBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""CALL foo()
                    |RETURN foo AS one""".stripMargin),
              Incoming(localCallables = Set(callableFoo)),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""CALL foo()""".stripMargin),
                Incoming(localCallables = Set(callableFoo)),
                Outgoing(variables = Set("foo"), localCallables = Set(callableFoo)),
                Declared(variables = Seq("foo")),
                ExpectedResult.NoResult
              ),
              ExpectedWorkingScope(
                Ast("""RETURN foo AS one""".stripMargin),
                Incoming(variables = Set("foo"), localCallables = Set(callableFoo)),
                Referenced(Set("foo")),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.varExp("foo", Set("foo"), incomingCallables = Set(callableFoo))
              )
            )
          ), {
            val localCallables = Set(callableFoo, callableBar)
            ExpectedWorkingScope(
              Ast(
                """CALL bar()
                  |RETURN one""".stripMargin
              ),
              Incoming(localCallables = localCallables),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""CALL bar()""".stripMargin),
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("one"), localCallables = localCallables),
                Declared(variables = Seq("one")),
                ExpectedResult.NoResult
              ),
              ExpectedWorkingScope(
                Ast("""RETURN one""".stripMargin),
                Incoming(variables = Set("one"), localCallables = localCallables),
                Referenced(Set("one")),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.varExp("one", Set("one"), incomingCallables = localCallables)
              )
            )
          }
        )
    )
  }

  test("""DEFINE PROCEDURE foo() {
         |  RETURN 1 AS foo
         |}
         |DEFINE PROCEDURE bar() {
         |  DEFINE PROCEDURE bar2() {
         |    CALL foo()
         |    RETURN foo
         |  }
         |  CALL bar2()
         |  RETURN foo AS one
         |}
         |
         |CALL bar()
         |RETURN one""".stripMargin) {
    val callableFoo = Callable("foo", ExpectedResult.TableResult("one"))
    val callableBar = Callable("bar", ExpectedResult.TableResult("one"))
    val callableBar2 = Callable("bar2", ExpectedResult.TableResult("foo"))
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE PROCEDURE foo() {
                |  RETURN 1 AS foo
                |}
                |DEFINE PROCEDURE bar() {
                |  DEFINE PROCEDURE bar2() {
                |    CALL foo()
                |    RETURN foo
                |  }
                |  CALL bar2()
                |  RETURN foo AS one
                |}
                |
                |CALL bar()
                |RETURN one""".stripMargin),
          Outgoing(variables = Set("one")),
          ExpectedResult.TableResult("one"),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE foo() {
                  |  RETURN 1 AS foo
                  |}""".stripMargin),
            Declared(localCallables = Seq(callableFoo)),
            Outgoing(localCallables = Set(callableFoo)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""RETURN 1 AS foo""".stripMargin), // query level
              Outgoing(variables = Set("foo")),
              ExpectedResult.TableResult("foo"),
              ExpectedWorkingScope(
                Ast("""RETURN 1 AS foo""".stripMargin),
                Outgoing(variables = Set("foo")),
                ExpectedResult.TableResult("foo"),
                ExpectedWorkingScope.constExp("1")
              )
            )
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE bar() {
                  |  DEFINE PROCEDURE bar2() {
                  |    CALL foo()
                  |    RETURN foo
                  |  }
                  |  CALL bar2()
                  |  RETURN foo AS one
                  |}""".stripMargin),
            Incoming(localCallables = Set(callableFoo)),
            Declared(localCallables = Seq(callableBar)),
            Outgoing(localCallables = Set(callableFoo, callableBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""DEFINE PROCEDURE bar2() {
                    |  CALL foo()
                    |  RETURN foo
                    |}
                    |CALL bar2()
                    |RETURN foo AS one""".stripMargin),
              Incoming(localCallables = Set(callableFoo)),
              Outgoing(variables = Set("one"), localCallables = Set(callableFoo)),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""DEFINE PROCEDURE bar2() {
                      |  CALL foo()
                      |  RETURN foo
                      |}""".stripMargin),
                Incoming(localCallables = Set(callableFoo)),
                Declared(localCallables = Seq(callableBar2)),
                Outgoing(localCallables = Set(callableFoo, callableBar2)),
                ExpectedResult.NoResult,
                ExpectedWorkingScope(
                  Ast("""CALL foo()
                        |RETURN foo""".stripMargin),
                  Incoming(localCallables = Set(callableFoo)),
                  Outgoing(variables = Set("foo")),
                  ExpectedResult.TableResult("foo"),
                  ExpectedWorkingScope(
                    Ast("""CALL foo()""".stripMargin),
                    Incoming(localCallables = Set(callableFoo)),
                    Outgoing(variables = Set("foo"), localCallables = Set(callableFoo)),
                    Declared(variables = Seq("foo")),
                    ExpectedResult.NoResult
                  ),
                  ExpectedWorkingScope(
                    Ast("""RETURN foo""".stripMargin),
                    Incoming(variables = Set("foo"), localCallables = Set(callableFoo)),
                    Referenced(Set("foo")),
                    Outgoing(variables = Set("foo")),
                    ExpectedResult.TableResult("foo"),
                    ExpectedWorkingScope.varExp("foo", Set("foo"), incomingCallables = Set(callableFoo))
                  )
                )
              ), {
                val localCallables = Set(callableFoo, callableBar2)
                ExpectedWorkingScope(
                  Ast(
                    """CALL bar2()
                      |RETURN foo AS one""".stripMargin
                  ),
                  Incoming(localCallables = localCallables),
                  Outgoing(variables = Set("one")),
                  ExpectedResult.TableResult("one"),
                  ExpectedWorkingScope(
                    Ast("""CALL bar2()""".stripMargin),
                    Incoming(localCallables = localCallables),
                    Outgoing(variables = Set("foo"), localCallables = localCallables),
                    Declared(variables = Seq("foo")),
                    ExpectedResult.NoResult
                  ),
                  ExpectedWorkingScope(
                    Ast("""RETURN foo AS one""".stripMargin),
                    Incoming(variables = Set("foo"), localCallables = localCallables),
                    Referenced(Set("foo")),
                    Outgoing(variables = Set("one")),
                    ExpectedResult.TableResult("one"),
                    ExpectedWorkingScope.varExp("foo", Set("foo"), incomingCallables = localCallables)
                  )
                )
              }
            )
          ), {
            val localCallables = Set(callableFoo, callableBar)
            ExpectedWorkingScope(
              Ast(
                """CALL bar()
                  |RETURN one""".stripMargin
              ),
              Incoming(localCallables = localCallables),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""CALL bar()""".stripMargin),
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("one"), localCallables = localCallables),
                Declared(variables = Seq("one")),
                ExpectedResult.NoResult
              ),
              ExpectedWorkingScope(
                Ast("""RETURN one""".stripMargin),
                Incoming(variables = Set("one"), localCallables = localCallables),
                Referenced(Set("one")),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.varExp("one", Set("one"), incomingCallables = localCallables)
              )
            )
          }
        )
    )
  }

  test("""DEFINE FUNCTION bar() = 1
         |
         |RETURN bar() AS one""".stripMargin) {
    val callableBar = Callable("bar", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION bar() = 1
                |
                |RETURN bar() AS one""".stripMargin),
          Outgoing(variables = Set("one")),
          ExpectedResult.TableResult("one"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION bar() = 1""".stripMargin),
            Declared(localCallables = Seq(callableBar)),
            Outgoing(localCallables = Set(callableBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope.constExp("1")
          ),
          ExpectedWorkingScope(
            Ast("""RETURN bar() AS one""".stripMargin), // query level
            Incoming(localCallables = Set(callableBar)),
            Outgoing(variables = Set("one")),
            ExpectedResult.TableResult("one"),
            ExpectedWorkingScope(
              Ast("""RETURN bar() AS one""".stripMargin),
              Incoming(localCallables = Set(callableBar)),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope.constExp("bar()", incomingCallables = Set(callableBar))
            )
          )
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |
         |RETURN twice(2) AS four""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |
                |RETURN twice(2) AS four""".stripMargin),
          Outgoing(variables = Set("four")),
          ExpectedResult.TableResult("four"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""RETURN twice(2) AS four""".stripMargin), // query level
            Incoming(localCallables = Set(callableTwice)),
            Outgoing(variables = Set("four")),
            ExpectedResult.TableResult("four"),
            ExpectedWorkingScope(
              Ast("""RETURN twice(2) AS four""".stripMargin),
              Incoming(localCallables = Set(callableTwice)),
              Outgoing(variables = Set("four")),
              ExpectedResult.TableResult("four"),
              ExpectedWorkingScope(
                Ast("""twice(2)""".stripMargin),
                Incoming(localCallables = Set(callableTwice)),
                ExpectedWorkingScope.constExp("2", incomingCallables = Set(callableTwice))
              )
            )
          )
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |DEFINE FUNCTION add(a, b) = a + b
         |
         |RETURN twice(add(1, 1)) AS four""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    val callableAdd = Callable("add", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |DEFINE FUNCTION add(a, b) = a + b
                |
                |RETURN twice(add(1, 1)) AS four""".stripMargin),
          Outgoing(variables = Set("four")),
          ExpectedResult.TableResult("four"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION add(a, b) = a + b""".stripMargin),
            Incoming(localCallables = Set(callableTwice)),
            Declared(localCallables = Seq(callableAdd)),
            Outgoing(localCallables = Set(callableTwice, callableAdd)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""a + b""".stripMargin),
              Incoming(constants = Set("a", "b"), localCallables = Set(callableTwice)),
              Referenced(Set("a", "b")),
              ExpectedWorkingScope.varExp("a", Set("a", "b"), incomingCallables = Set(callableTwice)),
              ExpectedWorkingScope.varExp("b", Set("a", "b"), incomingCallables = Set(callableTwice))
            )
          ), {
            val localCallables = Set(callableTwice, callableAdd)
            ExpectedWorkingScope(
              Ast("""RETURN twice(add(1, 1)) AS four""".stripMargin), // query level
              Incoming(localCallables = localCallables),
              Outgoing(variables = Set("four")),
              ExpectedResult.TableResult("four"),
              ExpectedWorkingScope(
                Ast("""RETURN twice(add(1, 1)) AS four""".stripMargin),
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("four")),
                ExpectedResult.TableResult("four"),
                ExpectedWorkingScope(
                  Ast("""twice(add(1, 1))""".stripMargin),
                  Incoming(localCallables = localCallables),
                  ExpectedWorkingScope(
                    Ast("""add(1, 1)""".stripMargin),
                    Incoming(localCallables = localCallables),
                    ExpectedWorkingScope.constExp("1", incomingCallables = localCallables),
                    ExpectedWorkingScope.constExp("1", incomingCallables = localCallables)
                  )
                )
              )
            )
          }
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |DEFINE FUNCTION add(a, b) = a + b
         |
         |RETURN add(2, 2) AS four
         |
         |NEXT
         |
         |RETURN twice(four) AS eight""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    val callableAdd = Callable("add", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |DEFINE FUNCTION add(a, b) = a + b
                |
                |RETURN add(2, 2) AS four
                |
                |NEXT
                |
                |RETURN twice(four) AS eight""".stripMargin),
          Outgoing(variables = Set("eight")),
          ExpectedResult.TableResult("eight"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION add(a, b) = a + b""".stripMargin),
            Incoming(localCallables = Set(callableTwice)),
            Declared(localCallables = Seq(callableAdd)),
            Outgoing(localCallables = Set(callableTwice, callableAdd)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""a + b""".stripMargin),
              Incoming(constants = Set("a", "b"), localCallables = Set(callableTwice)),
              Referenced(Set("a", "b")),
              ExpectedWorkingScope.varExp("a", Set("a", "b"), incomingCallables = Set(callableTwice)),
              ExpectedWorkingScope.varExp("b", Set("a", "b"), incomingCallables = Set(callableTwice))
            )
          ), {
            val localCallables = Set(callableTwice, callableAdd)
            ExpectedWorkingScope(
              Ast("""RETURN add(2, 2) AS four
                    |
                    |NEXT
                    |
                    |RETURN twice(four) AS eight""".stripMargin),
              Incoming(localCallables = localCallables),
              Outgoing(variables = Set("eight")),
              ExpectedResult.TableResult("eight"),
              ExpectedWorkingScope(
                Ast("""RETURN add(2, 2) AS four""".stripMargin), // query level
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("four")),
                ExpectedResult.TableResult("four"),
                ExpectedWorkingScope(
                  Ast("""RETURN add(2, 2) AS four""".stripMargin),
                  Incoming(localCallables = localCallables),
                  Outgoing(variables = Set("four")),
                  ExpectedResult.TableResult("four"),
                  ExpectedWorkingScope(
                    Ast("""add(2, 2)""".stripMargin),
                    Incoming(localCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables)
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("""YIELD four AS four""".stripMargin),
                Incoming(localCallables = localCallables),
                Declared(variables = Seq("four")),
                Outgoing(variables = Set("four"), localCallables = localCallables)
              ),
              ExpectedWorkingScope(
                Ast("""RETURN twice(four) AS eight""".stripMargin), // query level
                Incoming(variables = Set("four"), localCallables = localCallables),
                Referenced(Set("four")),
                Outgoing(variables = Set("eight")),
                ExpectedResult.TableResult("eight"),
                ExpectedWorkingScope(
                  Ast("""RETURN twice(four) AS eight""".stripMargin),
                  Incoming(variables = Set("four"), localCallables = localCallables),
                  Referenced(Set("four")),
                  Outgoing(variables = Set("eight")),
                  ExpectedResult.TableResult("eight"),
                  ExpectedWorkingScope(
                    Ast("""twice(four)""".stripMargin),
                    Incoming(constants = Set("four"), localCallables = localCallables),
                    Referenced(Set("four")),
                    ExpectedWorkingScope.varExp("four", Set("four"), incomingCallables = localCallables)
                  )
                )
              )
            )
          }
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |DEFINE FUNCTION add(a, b) = a + b
         |
         |RETURN add(2, 2) AS x
         |UNION
         |RETURN twice(3) AS x""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    val callableAdd = Callable("add", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |DEFINE FUNCTION add(a, b) = a + b
                |
                |RETURN add(2, 2) AS x
                |UNION
                |RETURN twice(3) AS x""".stripMargin),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION add(a, b) = a + b""".stripMargin),
            Incoming(localCallables = Set(callableTwice)),
            Declared(localCallables = Seq(callableAdd)),
            Outgoing(localCallables = Set(callableTwice, callableAdd)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""a + b""".stripMargin),
              Incoming(constants = Set("a", "b"), localCallables = Set(callableTwice)),
              Referenced(Set("a", "b")),
              ExpectedWorkingScope.varExp("a", Set("a", "b"), incomingCallables = Set(callableTwice)),
              ExpectedWorkingScope.varExp("b", Set("a", "b"), incomingCallables = Set(callableTwice))
            )
          ), {
            val localCallables = Set(callableTwice, callableAdd)
            ExpectedWorkingScope(
              Ast("""RETURN add(2, 2) AS x
                    |UNION
                    |RETURN twice(3) AS x""".stripMargin),
              Incoming(localCallables = localCallables),
              Declared(variables = List("x")),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("""RETURN add(2, 2) AS x""".stripMargin), // query level
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("""RETURN add(2, 2) AS x""".stripMargin),
                  Incoming(localCallables = localCallables),
                  Outgoing(variables = Set("x")),
                  ExpectedResult.TableResult("x"),
                  ExpectedWorkingScope(
                    Ast("""add(2, 2)""".stripMargin),
                    Incoming(localCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables)
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("""RETURN twice(3) AS x""".stripMargin), // query level
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("""RETURN twice(3) AS x""".stripMargin),
                  Incoming(localCallables = localCallables),
                  Outgoing(variables = Set("x")),
                  ExpectedResult.TableResult("x"),
                  ExpectedWorkingScope(
                    Ast("""twice(3)""".stripMargin),
                    Incoming(localCallables = localCallables),
                    ExpectedWorkingScope.constExp("3", incomingCallables = localCallables)
                  )
                )
              )
            )
          }
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |DEFINE FUNCTION add(a, b) = a + b
         |
         |{
         |  LET four = add(2, 2)
         |  RETURN twice(four) AS eight
         |}""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    val callableAdd = Callable("add", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |DEFINE FUNCTION add(a, b) = a + b
                |
                |{
                |  LET four = add(2, 2)
                |  RETURN twice(four) AS eight
                |}""".stripMargin),
          Outgoing(variables = Set("eight")),
          ExpectedResult.TableResult("eight"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION add(a, b) = a + b""".stripMargin),
            Incoming(localCallables = Set(callableTwice)),
            Declared(localCallables = Seq(callableAdd)),
            Outgoing(localCallables = Set(callableTwice, callableAdd)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""a + b""".stripMargin),
              Incoming(constants = Set("a", "b"), localCallables = Set(callableTwice)),
              Referenced(Set("a", "b")),
              ExpectedWorkingScope.varExp("a", Set("a", "b"), incomingCallables = Set(callableTwice)),
              ExpectedWorkingScope.varExp("b", Set("a", "b"), incomingCallables = Set(callableTwice))
            )
          ), {
            val localCallables = Set(callableTwice, callableAdd)
            ExpectedWorkingScope(
              Ast("""{
                    |  LET four = add(2, 2)
                    |  RETURN twice(four) AS eight
                    |}""".stripMargin), // query level
              Incoming(localCallables = localCallables),
              Outgoing(variables = Set("eight")),
              ExpectedResult.TableResult("eight"),
              ExpectedWorkingScope(
                Ast("""LET four = add(2, 2)
                      |RETURN twice(four) AS eight""".stripMargin), // query level
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("eight")),
                ExpectedResult.TableResult("eight"),
                ExpectedWorkingScope(
                  Ast("""LET four = add(2, 2)""".stripMargin),
                  Incoming(localCallables = localCallables),
                  Declared(variables = Seq("four")),
                  Outgoing(variables = Set("four"), localCallables = localCallables),
                  ExpectedResult.NoResult,
                  ExpectedWorkingScope(
                    Ast("""add(2, 2)""".stripMargin),
                    Incoming(localCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables)
                  )
                ),
                ExpectedWorkingScope(
                  Ast("""RETURN twice(four) AS eight""".stripMargin),
                  Incoming(variables = Set("four"), localCallables = localCallables),
                  Referenced(Set("four")),
                  Outgoing(variables = Set("eight")),
                  ExpectedResult.TableResult("eight"),
                  ExpectedWorkingScope(
                    Ast("""twice(four)""".stripMargin),
                    Incoming(constants = Set("four"), localCallables = localCallables),
                    Referenced(Set("four")),
                    ExpectedWorkingScope.varExp("four", Set("four"), incomingCallables = localCallables)
                  )
                )
              )
            )
          }
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |DEFINE FUNCTION add(a, b) = a + b
         |
         |WHEN add(2, 2) = 4 THEN RETURN twice(4) AS x
         |WHEN twice(4) = 8 THEN RETURN add(4, 4) AS x
         |ELSE RETURN twice(add(2, 2)) AS x""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    val callableAdd = Callable("add", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |DEFINE FUNCTION add(a, b) = a + b
                |
                |WHEN add(2, 2) = 4 THEN RETURN twice(4) AS x
                |WHEN twice(4) = 8 THEN RETURN add(4, 4) AS x
                |ELSE RETURN twice(add(2, 2)) AS x""".stripMargin),
          Outgoing(variables = Set("x")),
          ExpectedResult.TableResult("x"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION add(a, b) = a + b""".stripMargin),
            Incoming(localCallables = Set(callableTwice)),
            Declared(localCallables = Seq(callableAdd)),
            Outgoing(localCallables = Set(callableTwice, callableAdd)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""a + b""".stripMargin),
              Incoming(constants = Set("a", "b"), localCallables = Set(callableTwice)),
              Referenced(Set("a", "b")),
              ExpectedWorkingScope.varExp("a", Set("a", "b"), incomingCallables = Set(callableTwice)),
              ExpectedWorkingScope.varExp("b", Set("a", "b"), incomingCallables = Set(callableTwice))
            )
          ), {
            val localCallables = Set(callableTwice, callableAdd)
            ExpectedWorkingScope(
              Ast("""WHEN add(2, 2) = 4 THEN RETURN twice(4) AS x
                    |WHEN twice(4) = 8 THEN RETURN add(4, 4) AS x
                    |ELSE RETURN twice(add(2, 2)) AS x""".stripMargin),
              Incoming(localCallables = localCallables),
              Outgoing(variables = Set("x")),
              ExpectedResult.TableResult("x"),
              ExpectedWorkingScope(
                Ast("""WHEN add(2, 2) = 4 THEN RETURN twice(4) AS x""".stripMargin),
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("""add(2, 2) = 4""".stripMargin),
                  Incoming(localCallables = localCallables),
                  ExpectedWorkingScope(
                    Ast("""add(2, 2)""".stripMargin),
                    Incoming(localCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables),
                    ExpectedWorkingScope.constExp("2", incomingCallables = localCallables)
                  )
                ),
                ExpectedWorkingScope(
                  Ast("""RETURN twice(4) AS x""".stripMargin), // query level
                  Incoming(localCallables = localCallables),
                  Outgoing(variables = Set("x")),
                  ExpectedResult.TableResult("x"),
                  ExpectedWorkingScope(
                    Ast("""RETURN twice(4) AS x""".stripMargin),
                    Incoming(localCallables = localCallables),
                    Outgoing(variables = Set("x")),
                    ExpectedResult.TableResult("x"),
                    ExpectedWorkingScope(
                      Ast("""twice(4)""".stripMargin),
                      Incoming(localCallables = localCallables),
                      ExpectedWorkingScope.constExp("4", incomingCallables = localCallables)
                    )
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("""WHEN twice(4) = 8 THEN RETURN add(4, 4) AS x""".stripMargin),
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("""twice(4) = 8""".stripMargin),
                  Incoming(localCallables = localCallables),
                  ExpectedWorkingScope(
                    Ast("""twice(4)""".stripMargin),
                    Incoming(localCallables = localCallables),
                    ExpectedWorkingScope.constExp("4", incomingCallables = localCallables)
                  )
                ),
                ExpectedWorkingScope(
                  Ast("""RETURN add(4, 4) AS x""".stripMargin), // query level
                  Incoming(localCallables = localCallables),
                  Outgoing(variables = Set("x")),
                  ExpectedResult.TableResult("x"),
                  ExpectedWorkingScope(
                    Ast("""RETURN add(4, 4) AS x""".stripMargin),
                    Incoming(localCallables = localCallables),
                    Outgoing(variables = Set("x")),
                    ExpectedResult.TableResult("x"),
                    ExpectedWorkingScope(
                      Ast("""add(4, 4)""".stripMargin),
                      Incoming(localCallables = localCallables),
                      ExpectedWorkingScope.constExp("4", incomingCallables = localCallables),
                      ExpectedWorkingScope.constExp("4", incomingCallables = localCallables)
                    )
                  )
                )
              ),
              ExpectedWorkingScope(
                Ast("""ELSE RETURN twice(add(2, 2)) AS x""".stripMargin),
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("x")),
                ExpectedResult.TableResult("x"),
                ExpectedWorkingScope(
                  Ast("""RETURN twice(add(2, 2)) AS x""".stripMargin), // query level
                  Incoming(localCallables = localCallables),
                  Outgoing(variables = Set("x")),
                  ExpectedResult.TableResult("x"),
                  ExpectedWorkingScope(
                    Ast("""RETURN twice(add(2, 2)) AS x""".stripMargin),
                    Incoming(localCallables = localCallables),
                    Outgoing(variables = Set("x")),
                    ExpectedResult.TableResult("x"),
                    ExpectedWorkingScope(
                      Ast("""twice(add(2, 2))""".stripMargin),
                      Incoming(localCallables = localCallables),
                      ExpectedWorkingScope(
                        Ast("""add(2, 2)""".stripMargin),
                        Incoming(localCallables = localCallables),
                        ExpectedWorkingScope.constExp("2", incomingCallables = localCallables),
                        ExpectedWorkingScope.constExp("2", incomingCallables = localCallables)
                      )
                    )
                  )
                )
              )
            )
          }
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |
         |LET two = twice(1)
         |CALL (two) {
         |  DEFINE FUNCTION add(a, b) = a + b
         |
         |  RETURN add(two, two) AS four
         |}
         |RETURN twice(four) AS eight""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    val callableAdd = Callable("add", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |
                |LET two = twice(1)
                |CALL (two) {
                |  DEFINE FUNCTION add(a, b) = a + b
                |
                |  RETURN add(two, two) AS four
                |}
                |RETURN twice(four) AS eight""".stripMargin),
          Outgoing(variables = Set("eight")),
          ExpectedResult.TableResult("eight"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""LET two = twice(1)
                  |CALL (two) {
                  |  DEFINE FUNCTION add(a, b) = a + b
                  |
                  |  RETURN add(two, two) AS four
                  |}
                  |RETURN twice(four) AS eight""".stripMargin),
            Incoming(localCallables = Set(callableTwice)),
            Outgoing(variables = Set("eight")),
            ExpectedResult.TableResult("eight"),
            ExpectedWorkingScope(
              Ast("""LET two = twice(1)""".stripMargin),
              Incoming(localCallables = Set(callableTwice)),
              Declared(variables = Seq("two")),
              Outgoing(variables = Set("two"), localCallables = Set(callableTwice)),
              ExpectedWorkingScope(
                Ast("""twice(1)""".stripMargin),
                Incoming(localCallables = Set(callableTwice)),
                ExpectedWorkingScope.constExp("1", incomingCallables = Set(callableTwice))
              )
            ),
            ExpectedWorkingScope(
              Ast("""CALL (two) {
                    |  DEFINE FUNCTION add(a, b) = a + b
                    |
                    |  RETURN add(two, two) AS four
                    |}""".stripMargin),
              Incoming(variables = Set("two"), localCallables = Set(callableTwice)),
              Referenced(Set("two")),
              Declared(variables = Seq("four")),
              Outgoing(variables = Set("two", "four"), localCallables = Set(callableTwice)),
              ExpectedWorkingScope(
                Ast("""DEFINE FUNCTION add(a, b) = a + b
                      |
                      |RETURN add(two, two) AS four""".stripMargin),
                Incoming(constants = Set("two"), localCallables = Set(callableTwice)),
                Referenced(Set("two")),
                Outgoing(variables = Set("four"), localCallables = Set(callableTwice)),
                ExpectedResult.TableResult("four"),
                ExpectedWorkingScope(
                  Ast("""DEFINE FUNCTION add(a, b) = a + b""".stripMargin),
                  Incoming(localCallables = Set(callableTwice)),
                  Declared(localCallables = Seq(callableAdd)),
                  Outgoing(localCallables = Set(callableTwice, callableAdd)),
                  ExpectedResult.NoResult,
                  ExpectedWorkingScope(
                    Ast("""a + b""".stripMargin),
                    Incoming(constants = Set("a", "b"), localCallables = Set(callableTwice)),
                    Referenced(Set("a", "b")),
                    ExpectedWorkingScope.varExp("a", Set("a", "b"), incomingCallables = Set(callableTwice)),
                    ExpectedWorkingScope.varExp("b", Set("a", "b"), incomingCallables = Set(callableTwice))
                  )
                ), {
                  val localCallables = Set(callableTwice, callableAdd)
                  ExpectedWorkingScope(
                    Ast("""RETURN add(two, two) AS four""".stripMargin), // query level
                    Incoming(constants = Set("two"), localCallables = localCallables),
                    Referenced(Set("two")),
                    Outgoing(variables = Set("four")),
                    ExpectedResult.TableResult("four"),
                    ExpectedWorkingScope(
                      Ast("""RETURN add(two, two) AS four""".stripMargin),
                      Incoming(constants = Set("two"), localCallables = localCallables),
                      Referenced(Set("two")),
                      Outgoing(variables = Set("four")),
                      ExpectedResult.TableResult("four"),
                      ExpectedWorkingScope(
                        Ast("""add(two, two)""".stripMargin),
                        Incoming(constants = Set("two"), localCallables = localCallables),
                        Referenced(Set("two")),
                        ExpectedWorkingScope.varExp("two", Set("two"), incomingCallables = localCallables),
                        ExpectedWorkingScope.varExp("two", Set("two"), incomingCallables = localCallables)
                      )
                    )
                  )
                }
              )
            ),
            ExpectedWorkingScope(
              Ast("""RETURN twice(four) AS eight""".stripMargin),
              Incoming(variables = Set("two", "four"), localCallables = Set(callableTwice)),
              Referenced(Set("four")),
              Outgoing(variables = Set("eight")),
              ExpectedResult.TableResult("eight"),
              ExpectedWorkingScope(
                Ast("""twice(four)""".stripMargin),
                Incoming(constants = Set("two", "four"), localCallables = Set(callableTwice)),
                Referenced(Set("four")),
                ExpectedWorkingScope.varExp("four", Set("two", "four"), incomingCallables = Set(callableTwice))
              )
            )
          )
        )
    )
  }

  test("""DEFINE FUNCTION twice(x) = 2 * x
         |
         |FOREACH ( i IN [1, 2, 3] |
         |  CREATE (n:A {p: i})
         |  SET n.q = twice(i)
         |)
         |FINISH""".stripMargin) {
    val callableTwice = Callable("twice", ExpectedResult.ExpressionResult)
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION twice(x) = 2 * x
                |
                |FOREACH ( i IN [1, 2, 3] |
                |  CREATE (n:A {p: i})
                |  SET n.q = twice(i)
                |)
                |FINISH""".stripMargin),
          ExpectedResult.OmittedResult,
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION twice(x) = 2 * x""".stripMargin),
            Declared(localCallables = Seq(callableTwice)),
            Outgoing(localCallables = Set(callableTwice)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""2 * x""".stripMargin),
              Incoming(constants = Set("x")),
              Referenced(Set("x")),
              ExpectedWorkingScope.varExp("x", Set("x"))
            )
          ),
          ExpectedWorkingScope(
            Ast("""FOREACH ( i IN [1, 2, 3] |
                  |  CREATE (n:A {p: i})
                  |  SET n.q = twice(i)
                  |)
                  |FINISH""".stripMargin),
            Incoming(localCallables = Set(callableTwice)),
            ExpectedResult.OmittedResult,
            ExpectedWorkingScope(
              Ast("""FOREACH ( i IN [1, 2, 3] |
                    |  CREATE (n:A {p: i})
                    |  SET n.q = twice(i)
                    |)""".stripMargin),
              Incoming(localCallables = Set(callableTwice)),
              Declared(constants = Seq("i")),
              ExpectedResult.OmittedResult,
              Outgoing(localCallables = Set(callableTwice)),
              ExpectedWorkingScope.constExp("[1, 2, 3]", incomingCallables = Set(callableTwice)),
              ExpectedWorkingScope(
                Ast("""CREATE (n:A {p: i})
                      |SET n.q = twice(i)""".stripMargin),
                Incoming(variables = Set("i"), localCallables = Set(callableTwice)),
                Referenced(Set("i")),
                Outgoing(variables = Set("i", "n"), localCallables = Set(callableTwice)),
                ExpectedResult.OmittedResult,
                ExpectedWorkingScope(
                  Ast("CREATE (n:A {p: i})"),
                  Incoming(variables = Set("i"), localCallables = Set(callableTwice)),
                  Referenced(Set("i")),
                  Declared(variables = Seq("n")),
                  Outgoing(variables = Set("i", "n"), localCallables = Set(callableTwice)),
                  ExpectedResult.OmittedResult,
                  ExpectedWorkingScope(
                    Ast("(n:A {p: i})"),
                    PatternIncoming(topology = Set("i"), predicate = Set("i"), localCallables = Set(callableTwice)),
                    Referenced(Set("i")),
                    Declared(variables = Seq("n")),
                    Outgoing(variables = Set("n")),
                    ExpectedResult.TableResult("n"),
                    ExpectedWorkingScope.constExp("A", Set("i"), incomingCallables = Set(callableTwice)),
                    ExpectedWorkingScope(
                      Ast("{p: i}"),
                      Incoming(constants = Set("i"), localCallables = Set(callableTwice)),
                      Referenced(Set("i")),
                      ExpectedWorkingScope.varExp("i", Set("i"), incomingCallables = Set(callableTwice))
                    )
                  )
                ),
                ExpectedWorkingScope(
                  Ast("SET n.q = twice(i)"),
                  Incoming(variables = Set("i", "n"), localCallables = Set(callableTwice)),
                  Referenced(Set("i", "n")),
                  Outgoing(variables = Set("i", "n"), localCallables = Set(callableTwice)),
                  ExpectedResult.OmittedResult,
                  ExpectedWorkingScope(
                    Ast("n.q"),
                    Incoming(constants = Set("i", "n"), localCallables = Set(callableTwice)),
                    Referenced(Set("n")),
                    ExpectedWorkingScope.varExp("n", Set("i", "n"), incomingCallables = Set(callableTwice))
                  ),
                  ExpectedWorkingScope(
                    Ast("twice(i)"),
                    Incoming(constants = Set("i", "n"), localCallables = Set(callableTwice)),
                    Referenced(Set("i")),
                    ExpectedWorkingScope.varExp("i", Set("i", "n"), incomingCallables = Set(callableTwice))
                  )
                )
              )
            ),
            ExpectedWorkingScope(
              Ast("""FINISH""".stripMargin),
              Incoming(localCallables = Set(callableTwice)),
              ExpectedResult.OmittedResult
            )
          )
        )
    )
  }

  test("""DEFINE FUNCTION bar() = 1
         |DEFINE PROCEDURE bar() { // note that this is invalid
         |  RETURN 1 AS one
         |}
         |
         |CALL bar()
         |RETURN one""".stripMargin) {
    val callableFuncBar = Callable("bar", ExpectedResult.ExpressionResult)
    val callableProcBar = Callable("bar", ExpectedResult.TableResult("one"))
    hasScope(
      skipVariableChecker = true,
      expected =
        ExpectedWorkingScope(
          Ast("""DEFINE FUNCTION bar() = 1
                |DEFINE PROCEDURE bar() {
                |  RETURN 1 AS one
                |}
                |
                |CALL bar()
                |RETURN one""".stripMargin),
          Outgoing(variables = Set("one")),
          ExpectedResult.TableResult("one"),
          ExpectedWorkingScope(
            Ast("""DEFINE FUNCTION bar() = 1""".stripMargin),
            Declared(localCallables = Seq(callableFuncBar)),
            Outgoing(localCallables = Set(callableFuncBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope.constExp("1")
          ),
          ExpectedWorkingScope(
            Ast("""DEFINE PROCEDURE bar() {
                  |  RETURN 1 AS one
                  |}""".stripMargin),
            Incoming(localCallables = Set(callableFuncBar)),
            Declared(localCallables = Seq(callableProcBar)),
            Outgoing(localCallables = Set(callableFuncBar, callableProcBar)),
            ExpectedResult.NoResult,
            ExpectedWorkingScope(
              Ast("""RETURN 1 AS one""".stripMargin), // query level
              Incoming(localCallables = Set(callableFuncBar)),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                Ast("""RETURN 1 AS one""".stripMargin),
                Incoming(localCallables = Set(callableFuncBar)),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.constExp("1", incomingCallables = Set(callableFuncBar))
              )
            )
          ), {
            val localCallables = Set(callableFuncBar, callableProcBar)
            ExpectedWorkingScope(
              Ast(
                """CALL bar()
                  |RETURN one""".stripMargin
              ),
              Incoming(localCallables = localCallables),
              Outgoing(variables = Set("one")),
              ExpectedResult.TableResult("one"),
              ExpectedWorkingScope(
                // scoping of call still picks up the procedure
                Ast("""CALL bar()""".stripMargin),
                Incoming(localCallables = localCallables),
                Outgoing(variables = Set("one"), localCallables = localCallables),
                Declared(variables = Seq("one")),
                ExpectedResult.NoResult
              ),
              ExpectedWorkingScope(
                Ast("""RETURN one""".stripMargin),
                Incoming(variables = Set("one"), localCallables = localCallables),
                Referenced(Set("one")),
                Outgoing(variables = Set("one")),
                ExpectedResult.TableResult("one"),
                ExpectedWorkingScope.varExp("one", Set("one"), incomingCallables = localCallables)
              )
            )
          }
        )
    )
  }
}
