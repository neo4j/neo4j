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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalFunctions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.DiffPrinter
import org.neo4j.cypher.internal.util.test_helpers.TestName

class LocalCallableResolutionTest extends CypherFunSuite with TestName with AstConstructionTestSupport {

  test(
    """DEFINE FUNCTION foo() = "abc"
      |
      |RETURN foo() AS foo
      |""".stripMargin
  ) {
    hasAst(
      queryWithLocalDefinitions(
        localFunctionDefinition("foo").body(literalString("abc"))
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunction(
                "foo"
              ),
              "foo"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE FUNCTION foo(x) = x
      |
      |RETURN foo("abc") AS abc
      |""".stripMargin
  ) {
    val fieldX = localFieldSignature("x")
    hasAst(
      queryWithLocalDefinitions(
        localFunctionDefinition(
          "foo",
          fieldX
        ).body(varFor("x"))
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunctionSignatureBased(
                "foo",
                Seq(
                  fieldX -> Some(literalString("abc"))
                )
              ),
              "abc"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE FUNCTION foo(x :: STRING) :: STRING = x
      |
      |RETURN foo("abc") AS abc, foo("def") AS def
      |""".stripMargin
  ) {
    val fieldX = localFieldSignature("x", symbols.CTString)
    hasAst(
      queryWithLocalDefinitions(
        localFunctionDefinition(
          "foo",
          fieldX
        ).typ(symbols.CTString).body(varFor("x"))
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunctionSignatureBased(
                "foo",
                Seq(
                  fieldX -> Some(literalString("abc"))
                ),
                Some(symbols.CTString)
              ),
              "abc"
            ),
            aliasedReturnItem(
              localFunctionSignatureBased(
                "foo",
                Seq(
                  fieldX -> Some(literalString("def"))
                ),
                Some(symbols.CTString)
              ),
              "def"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE FUNCTION foo(x :: STRING = "abc") = x
      |
      |RETURN foo() AS abc, foo("def") AS def
      |""".stripMargin
  ) {
    hasAst(
      queryWithLocalDefinitions(
        localFunctionDefinition(
          "foo",
          localFieldSignature("x", symbols.CTString, literalString("abc"))
        ).body(varFor("x"))
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunction(
                "foo",
                Seq(
                  ("x", symbols.CTString, Some(literalString("abc")), None)
                )
              ),
              "abc"
            ),
            aliasedReturnItem(
              localFunction(
                "foo",
                Seq(
                  ("x", symbols.CTString, Some(literalString("abc")), Some(literalString("def")))
                )
              ),
              "def"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE FUNCTION foo() = "abc"
      |DEFINE FUNCTION bar() = "def"
      |
      |RETURN foo() AS abc, bar() AS def
      |""".stripMargin
  ) {
    hasAst(
      queryWithLocalDefinitions(
        localFunctionDefinition("foo").body(literalString("abc")),
        localFunctionDefinition("bar").body(literalString("def"))
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunction(
                "foo"
              ),
              "abc"
            ),
            aliasedReturnItem(
              localFunction(
                "bar"
              ),
              "def"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE FUNCTION foo() = "abc"
      |DEFINE FUNCTION bar() = "def"
      |
      |RETURN foo() AS abc, size(bar()) AS three
      |""".stripMargin
  ) {
    hasAst(
      queryWithLocalDefinitions(
        localFunctionDefinition("foo").body(literalString("abc")),
        localFunctionDefinition("bar").body(literalString("def"))
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunction(
                "foo"
              ),
              "abc"
            ),
            aliasedReturnItem(
              function(
                "size",
                localFunction(
                  "bar"
                )
              ),
              "three"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE FUNCTION foo() = "abc"
      |DEFINE FUNCTION bar(x) = x
      |
      |RETURN bar(10 * size(foo())) AS x, size(foo()) * bar(10) AS y
      |""".stripMargin
  ) {
    val fieldX = localFieldSignature("x")
    hasAst(
      queryWithLocalDefinitions(
        localFunctionDefinition("foo").body(literalString("abc")),
        localFunctionDefinition("bar", fieldX).body(varFor("x"))
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunctionSignatureBased(
                "bar",
                Seq(
                  fieldX -> Some(multiply(
                    literalInt(10),
                    function("size", localFunction("foo"))
                  ))
                )
              ),
              "x"
            ),
            aliasedReturnItem(
              multiply(
                function(
                  "size",
                  localFunction(
                    "foo"
                  )
                ),
                localFunctionSignatureBased(
                  "bar",
                  Seq(
                    fieldX -> Some(literalInt(10))
                  )
                )
              ),
              "y"
            )
          )
        )
      )
    )
  }

  private val messageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  def hasAst(expected: Statement): Unit = {
    CypherVersion.values().filterNot(_ == CypherVersion.Cypher5).foreach { version =>
      val result = run(testName, version)
      val actual = result.statement
      if (actual == expected) {
        succeed
      } else {
        fail(
          s"""Version: CYPHER $version
             |Query:
             |------
             |${result.query}
             |
             |Diff condensed (expected -> actual):
             |------------------------------------
             |${DiffPrinter.render(
              pprint.apply(expected).render,
              pprint.apply(actual).render,
              harmonize = harmonizeAstLine,
              isCondensed = true
            )}
             |
             |Diff full (expected -> actual):
             |-------------------------------
             |${DiffPrinter.render(
              pprint.apply(expected).render,
              pprint.apply(actual).render,
              harmonize = harmonizeAstLine,
              isCondensed = false
            )}
             |
             |Expected:
             |---------
             |${pprint.apply(expected)}
             |
             |Actual:
             |-------
             |${pprint.apply(actual)}
             |""".stripMargin
        )
      }
    }
  }

  private case class RunResult(query: String, cypherVersion: CypherVersion, statement: Statement)

  private def run(query: String, cypherVersion: CypherVersion): RunResult = {
    val context = new ErrorCollectingContext(cypherVersion) {
      override def errorMessageProvider: ErrorMessageProvider = messageProvider
    }
    val transformer = Parse andThen ScopeSurveyor andThen ResolveLocalFunctions
    val initialState = InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)

    val state = transformer.transform(initialState, context)
    val statement: Statement = state.maybeStatement.getOrElse(throw new IllegalStateException("No statement in state"))
    RunResult(query, cypherVersion, statement)
  }

  def harmonizeAstLine(line: String): String = {
    val y = "\u001B[33m"
    val r = "\u001B[39m"
    Seq(s"${y}Vector${r}(", s"${y}ArraySeq${r}(", s"${y}List${r}(").foldLeft(line) {
      case (updatedLine, needle) =>
        updatedLine.replace(needle, s"${y}Seq${r}(")
    }
  }
}
