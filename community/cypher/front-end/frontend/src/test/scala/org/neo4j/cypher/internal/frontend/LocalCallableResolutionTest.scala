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
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.GroupBy
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.LocalFieldSignature
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.NonOptional
import org.neo4j.cypher.internal.ast.OptionalState
import org.neo4j.cypher.internal.ast.ParsedAsFilter
import org.neo4j.cypher.internal.ast.ParsedAsLet
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.ProcedureResultItem
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.functions.LocalFunction
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.InitialState
import org.neo4j.cypher.internal.frontend.phases.LocalDefinitionsDirectory
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.QueryLanguage
import org.neo4j.cypher.internal.frontend.phases.ResolvedLocalCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedNonLocalCall
import org.neo4j.cypher.internal.frontend.phases.ScopedProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.TryResolveCallables
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ExtractLocalDefinitions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.Parse
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.PreparatoryRewriting
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalFunctions
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalProceduresStep1
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.ResolveLocalProceduresStep2
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.ScopeSurveyor
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.ErrorMessageProvider
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.NotImplementedErrorMessageProvider
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.ProcedureOutput
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.DiffPrinter
import org.neo4j.cypher.internal.util.test_helpers.TestName
import org.neo4j.exceptions.Neo4jException
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs
import org.scalatest.Assertions

import scala.util.Random

class LocalCallableResolutionTest extends CypherFunSuite with TestName with AstConstructionTestSupport {

  private val prettifier: Prettifier = Prettifier(ExpressionStringifier())

  // Note that fewer tests may be actually executed, since equal fuzz tests are executed only once
  private val numberOfFuzzTests = 100
  private val astRenderHeightForDiff = 1000
  private val astRenderHeight = 1000
  private val astRenderWidth = 500

  test(
    """DEFINE PROCEDURE foo() {
      |  FINISH
      |}
      |
      |CALL foo()
      |RETURN abc AS foo
      |""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      finish()
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            declaredResults = false
          ),
          return_(
            aliasedReturnItem(
              varFor("abc"),
              "foo"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE PROCEDURE foo() {
      |  RETURN "abc" AS abc
      |}
      |
      |CALL foo() YIELD abc
      |RETURN abc AS foo
      |""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      return_(
        aliasedReturnItem(
          literalString("abc"),
          "abc"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            outputSignature = Some(Seq(localFieldSignature("abc"))),
            callResults = Seq(None -> "abc")
          ),
          return_(
            aliasedReturnItem(
              varFor("abc"),
              "foo"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE PROCEDURE foo(x) {
      |  RETURN x
      |}
      |
      |CALL foo("abc") YIELD x
      |RETURN x AS foo
      |""".stripMargin
  ) {
    val fieldX = localFieldSignature("x")
    val foo = localProcedureDefinition(
      "foo",
      fieldX
    ).body(
      return_(
        aliasedReturnItem(
          varFor("x"),
          "x"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            inputSignature = Seq(fieldX),
            outputSignature = Some(Seq(localFieldSignature("x"))),
            callArguments = Seq(literalString("abc")),
            callResults = Seq(None -> "x")
          ),
          return_(
            aliasedReturnItem(
              varFor("x"),
              "foo"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE PROCEDURE foo(x :: STRING) :: (x :: STRING) {
      |  RETURN x
      |}
      |
      |CALL foo("abc") YIELD x AS abc
      |CALL foo("def") YIELD x AS def
      |RETURN abc, def
      |""".stripMargin
  ) {
    val fieldX = localFieldSignature("x", symbols.CTString)
    val out = localFieldSignature("x", symbols.CTString)
    val foo = localProcedureDefinition(
      "foo",
      fieldX
    ).out(out).body(
      return_(
        aliasedReturnItem(
          varFor("x"),
          "x"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            inputSignature = Seq(fieldX),
            outputSignature = Some(Seq(out)),
            callArguments = Seq(coerceTo(literalString("abc"), symbols.CTString)),
            callResults = Seq(Some("x") -> "abc")
          ),
          resolvedLocalCall(
            procedureName("foo"),
            inputSignature = Seq(fieldX),
            outputSignature = Some(Seq(out)),
            callArguments = Seq(coerceTo(literalString("def"), symbols.CTString)),
            callResults = Seq(Some("x") -> "def")
          ),
          return_(
            aliasedReturnItem(
              varFor("abc"),
              "abc"
            ),
            aliasedReturnItem(
              varFor("def"),
              "def"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE PROCEDURE foo(x :: STRING = "abc") {
      |  RETURN x
      |}
      |
      |CALL foo() YIELD x AS abc
      |CALL foo("def") YIELD x AS def
      |RETURN abc, def
      |""".stripMargin
  ) {
    val fieldX = localFieldSignature("x", symbols.CTString, literalString("abc"))
    val out = localFieldSignature("x")
    val foo = localProcedureDefinition(
      "foo",
      fieldX
    ).body(
      return_(
        aliasedReturnItem(
          varFor("x"),
          "x"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            inputSignature = Seq(fieldX),
            outputSignature = Some(Seq(out)),
            callResults = Seq(Some("x") -> "abc")
          ),
          resolvedLocalCall(
            procedureName("foo"),
            inputSignature = Seq(fieldX),
            outputSignature = Some(Seq(out)),
            callArguments = Seq(coerceTo(literalString("def"), symbols.CTString)),
            callResults = Seq(Some("x") -> "def")
          ),
          return_(
            aliasedReturnItem(
              varFor("abc"),
              "abc"
            ),
            aliasedReturnItem(
              varFor("def"),
              "def"
            )
          )
        )
      )
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN 1 AS x
       |}
       |
       |CALL bar() YIELD x
       |RETURN x""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      return_(
        aliasedReturnItem(literalInt(1), "x")
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo
    ).isRewrittenTo(
      singleQueryWithLocalDefinitions(
        foo
      )(
        unresolvedCall(
          procedureName("bar"),
          args = Some(Seq.empty),
          yields = Some(Seq(varFor("x")))
        ),
        return_(
          aliasedReturnItem(varFor("x"), "x")
        )
      )
    )
  }

  test(
    """DEFINE PROCEDURE foo() {
      |  RETURN "abc" AS abc
      |}
      |DEFINE PROCEDURE bar() {
      |  RETURN "def" AS def
      |}
      |
      |CALL foo() YIELD abc
      |CALL bar() YIELD def
      |RETURN abc, def
      |""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      return_(
        aliasedReturnItem(
          literalString("abc"),
          "abc"
        )
      )
    )
    val bar = localProcedureDefinition("bar").body(
      return_(
        aliasedReturnItem(
          literalString("def"),
          "def"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo,
      procedureName("bar") -> bar
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo,
        bar
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            outputSignature = Some(Seq(localFieldSignature("abc"))),
            callResults = Seq(None -> "abc")
          ),
          resolvedLocalCall(
            procedureName("bar"),
            outputSignature = Some(Seq(localFieldSignature("def"))),
            callResults = Seq(None -> "def")
          ),
          return_(
            aliasedReturnItem(
              varFor("abc"),
              "abc"
            ),
            aliasedReturnItem(
              varFor("def"),
              "def"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE PROCEDURE foo() {
      |  DELETE null
      |  RETURN "abc" AS abc
      |}
      |DEFINE PROCEDURE bar() {
      |  RETURN "def" AS def
      |}
      |
      |CALL foo() YIELD abc
      |CALL bar() YIELD def
      |RETURN abc, def
      |""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      delete(nullLiteral),
      return_(
        aliasedReturnItem(
          literalString("abc"),
          "abc"
        )
      )
    )
    val bar = localProcedureDefinition("bar").body(
      return_(
        aliasedReturnItem(
          literalString("def"),
          "def"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo,
      procedureName("bar") -> bar
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo,
        bar
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            outputSignature = Some(Seq(localFieldSignature("abc"))),
            bodyContainsUpdates = true,
            callResults = Seq(None -> "abc")
          ),
          resolvedLocalCall(
            procedureName("bar"),
            outputSignature = Some(Seq(localFieldSignature("def"))),
            callResults = Seq(None -> "def")
          ),
          return_(
            aliasedReturnItem(
              varFor("abc"),
              "abc"
            ),
            aliasedReturnItem(
              varFor("def"),
              "def"
            )
          )
        )
      )
    ).hasResolvedLocalCallWithBodyContainsUpdates(
      "foo" -> true,
      "bar" -> false
    )
  }

  test(
    """{
      |  DEFINE PROCEDURE foo() {
      |    DELETE null
      |    RETURN "abc" AS abc
      |  }
      |  DEFINE PROCEDURE bar() {
      |    RETURN "def" AS def
      |  }
      |
      |  CALL foo() YIELD abc
      |  CALL bar() YIELD def
      |  RETURN abc, def
      |}
      |""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      delete(nullLiteral),
      return_(
        aliasedReturnItem(
          literalString("abc"),
          "abc"
        )
      )
    )
    val bar = localProcedureDefinition("bar").body(
      return_(
        aliasedReturnItem(
          literalString("def"),
          "def"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo,
      procedureName("bar") -> bar
    ).isRewrittenTo(
      topLevelBraces(
        queryWithLocalDefinitions(
          foo,
          bar
        )(
          singleQuery(
            resolvedLocalCall(
              procedureName("foo"),
              outputSignature = Some(Seq(localFieldSignature("abc"))),
              bodyContainsUpdates = true,
              callResults = Seq(None -> "abc")
            ),
            resolvedLocalCall(
              procedureName("bar"),
              outputSignature = Some(Seq(localFieldSignature("def"))),
              callResults = Seq(None -> "def")
            ),
            return_(
              aliasedReturnItem(
                varFor("abc"),
                "abc"
              ),
              aliasedReturnItem(
                varFor("def"),
                "def"
              )
            )
          )
        )
      )
    ).hasResolvedLocalCallWithBodyContainsUpdates(
      "foo" -> true,
      "bar" -> false
    )
  }

  test(
    """DEFINE PROCEDURE foo() {
      |  RETURN "abc" AS abc
      |}
      |DEFINE PROCEDURE bar() {
      |  RETURN "def" AS def
      |}
      |
      |CALL foo() YIELD abc
      |CALL nonLocal.unresolved.proc(EXISTS {
      |  CALL bar() YIELD def
      |})
      |RETURN abc
      |""".stripMargin
  ) {
    val foo = localProcedureDefinition("foo").body(
      return_(
        aliasedReturnItem(
          literalString("abc"),
          "abc"
        )
      )
    )
    val bar = localProcedureDefinition("bar").body(
      return_(
        aliasedReturnItem(
          literalString("def"),
          "def"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo,
      procedureName("bar") -> bar
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo,
        bar
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("foo"),
            outputSignature = Some(Seq(localFieldSignature("abc"))),
            callResults = Seq(None -> "abc")
          ),
          unresolvedCall(
            procedureName("nonLocal", "unresolved", "proc"),
            Some(Seq(existsSubquery(
              resolvedLocalCall(
                procedureName("bar"),
                outputSignature = Some(Seq(localFieldSignature("def"))),
                callResults = Seq(None -> "def")
              )
            )))
          ),
          return_(
            aliasedReturnItem(
              varFor("abc"),
              "abc"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE PROCEDURE foo() {
      |  RETURN "abc" AS abc
      |}
      |DEFINE PROCEDURE bar(x) {
      |  RETURN x AS def
      |}
      |
      |CALL bar(EXISTS {
      |  CALL nonLocal.unresolved.proc()
      |}) YIELD def
      |CALL nonLocal.unresolved.proc(EXISTS {
      |  CALL bar(EXISTS {
      |    CALL foo() YIELD abc
      |  }) YIELD def
      |})
      |RETURN def
      |""".stripMargin
  ) {
    val fieldX = localFieldSignature("x")
    val foo = localProcedureDefinition("foo").body(
      return_(
        aliasedReturnItem(
          literalString("abc"),
          "abc"
        )
      )
    )
    val bar = localProcedureDefinition("bar", fieldX).body(
      return_(
        aliasedReturnItem(
          varFor("x"),
          "def"
        )
      )
    )
    cypher25OnwardsTestName.hasExtractedLocalProcedures(
      procedureName("foo") -> foo,
      procedureName("bar") -> bar
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo,
        bar
      )(
        singleQuery(
          resolvedLocalCall(
            procedureName("bar"),
            inputSignature = Seq(fieldX),
            outputSignature = Some(Seq(localFieldSignature("def"))),
            callArguments = Seq(existsSubquery(
              unresolvedCall(
                procedureName("nonLocal", "unresolved", "proc"),
                Some(Seq.empty)
              )
            )),
            callResults = Seq(None -> "def")
          ),
          unresolvedCall(
            procedureName("nonLocal", "unresolved", "proc"),
            Some(Seq(existsSubquery(
              resolvedLocalCall(
                procedureName("bar"),
                inputSignature = Seq(fieldX),
                outputSignature = Some(Seq(localFieldSignature("def"))),
                callArguments = Seq(existsSubquery(
                  resolvedLocalCall(
                    procedureName("foo"),
                    outputSignature = Some(Seq(localFieldSignature("abc"))),
                    callResults = Seq(None -> "abc")
                  )
                )),
                callResults = Seq(None -> "def")
              )
            )))
          ),
          return_(
            aliasedReturnItem(
              varFor("def"),
              "def"
            )
          )
        )
      )
    )
  }

  test(
    """DEFINE FUNCTION foo() = "abc"
      |
      |RETURN foo() AS foo
      |""".stripMargin
  ) {
    val foo = localFunctionDefinition("foo").body(literalString("abc"))
    cypher25OnwardsTestName.hasExtractedLocalFunctions(
      functionName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
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
    val foo = localFunctionDefinition(
      "foo",
      fieldX
    ).body(varFor("x"))
    cypher25OnwardsTestName.hasExtractedLocalFunctions(
      functionName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
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
    val foo = localFunctionDefinition(
      "foo",
      fieldX
    ).typ(symbols.CTString).body(varFor("x"))
    cypher25OnwardsTestName.hasExtractedLocalFunctions(
      functionName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
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
    val fieldX = localFieldSignature("x", symbols.CTString, literalString("abc"))
    val foo = localFunctionDefinition(
      "foo",
      fieldX
    ).body(varFor("x"))
    cypher25OnwardsTestName.hasExtractedLocalFunctions(
      functionName("foo") -> foo
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo
      )(
        singleQuery(
          return_(
            aliasedReturnItem(
              localFunctionSignatureBased(
                "foo",
                Seq(
                  fieldX -> None
                )
              ),
              "abc"
            ),
            aliasedReturnItem(
              localFunctionSignatureBased(
                "foo",
                Seq(
                  fieldX -> Some(literalString("def"))
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
    val foo = localFunctionDefinition("foo").body(literalString("abc"))
    val bar = localFunctionDefinition("bar").body(literalString("def"))
    cypher25OnwardsTestName.hasExtractedLocalFunctions(
      functionName("foo") -> foo,
      functionName("bar") -> bar
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo,
        bar
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
    val foo = localFunctionDefinition("foo").body(literalString("abc"))
    val bar = localFunctionDefinition("bar").body(literalString("def"))
    cypher25OnwardsTestName.hasExtractedLocalFunctions(
      functionName("foo") -> foo,
      functionName("bar") -> bar
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo,
        bar
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
    val foo = localFunctionDefinition("foo").body(literalString("abc"))
    val bar = localFunctionDefinition("bar", fieldX).body(varFor("x"))
    cypher25OnwardsTestName.hasExtractedLocalFunctions(
      functionName("foo") -> foo,
      functionName("bar") -> bar
    ).isRewrittenTo(
      queryWithLocalDefinitions(
        foo,
        bar
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

  /**
   * Tests on the bodyContainsUpdates marker
   *
   * Note that local functions and scalar subqueries are not allowed to contain updates. That is checked in SemanticAnalysis.
   * That check relies on the fact that the resolution of local procedure calls detects if the call procedure contains updates.
   * Here, we test if the resolution of local procedure calls correctly detects if the call procedure contains updates,
   * even in invalid construction with local functions and/or scalar subqueries.
   */
  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN "abc" AS abc
       |}
       |DEFINE PROCEDURE bar() {
       |  CREATE (:N)
       |  RETURN "def" AS def
       |}
       |
       |RETURN abc
       |""".stripMargin
  ) {
    // note that no calls in the tested query
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates()
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN "abc" AS abc
       |}
       |DEFINE PROCEDURE bar() {
       |  CREATE (:N)
       |  RETURN "def" AS def
       |}
       |
       |CALL foo() YIELD abc
       |RETURN abc
       |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "foo" -> false
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN "abc" AS abc
       |}
       |DEFINE PROCEDURE bar() {
       |  CREATE (:N)
       |  RETURN "def" AS def
       |}
       |
       |CALL bar() YIELD def
       |RETURN def
       |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "bar" -> true
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN "abc" AS abc
       |}
       |DEFINE PROCEDURE bar() {
       |  CREATE (:N)
       |  RETURN "def" AS def
       |}
       |
       |CALL foo() YIELD abc
       |CALL bar() YIELD abc
       |RETURN abc
       |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "foo" -> false,
      "bar" -> true
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  RETURN "abc" AS abc
       |}
       |DEFINE PROCEDURE bar() {
       |  CALL nonLocal.update.proc()
       |  RETURN "def" AS def
       |}
       |
       |CALL foo() YIELD abc
       |CALL bar() YIELD abc
       |RETURN abc
       |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "foo" -> false,
      "bar" -> true
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  DEFINE PROCEDURE bar() {
       |    CREATE (:N)
       |    RETURN "def" AS def
       |  }
       |
       |  RETURN "abc" AS abc
       |}
       |
       |CALL foo() YIELD abc
       |RETURN abc
       |""".stripMargin
  ) {
    // note that foo just containing the definition of bar does not make the body of foo containing updates
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "foo" -> false
    )
  }

  test(
    s"""DEFINE PROCEDURE foo() {
       |  DEFINE PROCEDURE bar() {
       |    CREATE (:N)
       |    RETURN "def" AS def
       |  }
       |
       |  CALL bar() YIELD def
       |  RETURN "abc" AS abc
       |}
       |
       |RETURN abc
       |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "bar" -> true
    )
  }

  test(
    s"""DEFINE PROCEDURE bar() {
       |  CREATE (:N)
       |  RETURN "def" AS def
       |}
       |DEFINE PROCEDURE foo() {
       |  CALL bar() YIELD def
       |  RETURN "abc" AS abc
       |}
       |
       |CALL foo() YIELD abc
       |RETURN abc
       |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "foo" -> true,
      "bar" -> true
    )
  }

  test(
    s"""DEFINE FUNCTION foo.a(n) {
       |  DEFINE PROCEDURE set(o) {
       |    SET o:A
       |    RETURN o
       |  }
       |
       |  CALL set(n) YIELD o AS nUpdated
       |  RETURN labels(nUpdated) LIMIT 1
       |}
       |
       |MATCH (a:A)
       |RETURN foo.a(a)
       |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true
    )
  }

  test(
    """DEFINE PROCEDURE local.proc0() {
      |  CREATE (:N)
      |  RETURN 15 AS x1
      |}
      |
      |CALL local.proc0()
      |  YIELD x1 AS x73
      |RETURN 74 AS x70
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "local.proc0" -> true
    )
  }

  test(
    """DEFINE PROCEDURE local.proc0() {
      |  CREATE (:N)
      |  RETURN 15 AS x1
      |}
      |
      |{
      |  CALL local.proc0()
      |    YIELD x1 AS x73
      |  RETURN 74 AS x70
      |}
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "local.proc0" -> true
    )
  }

  test(
    """{
      |  DEFINE PROCEDURE local.proc0() {
      |    CREATE (:N)
      |    RETURN 15 AS x1
      |  }
      |
      |  CALL local.proc0()
      |    YIELD x1 AS x73
      |  RETURN 74 AS x70
      |}
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "local.proc0" -> true
    )
  }

  test(
    """{
      |  DEFINE PROCEDURE local.proc0() {
      |    CALL nonLocal.update.proc()
      |    RETURN 15 AS x1
      |  }
      |
      |  CALL local.proc0()
      |    YIELD x1 AS x73
      |  RETURN 74 AS x70
      |}
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "local.proc0" -> true
    )
  }

  test(
    """RETURN 10 AS x1
      |
      |NEXT
      |{
      |  DEFINE PROCEDURE local.proc0() {
      |    CREATE (:N)
      |    RETURN 15 AS x2
      |  }
      |
      |  CALL local.proc0()
      |    YIELD x1 AS x73
      |  RETURN 74 AS x70
      |}
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "local.proc0" -> true
    )
  }

  test(
    """RETURN 10 AS x1
      |UNION
      |{
      |  DEFINE PROCEDURE local.proc0() {
      |    CREATE (:N)
      |    RETURN 15 AS x2
      |  }
      |
      |  CALL local.proc0()
      |    YIELD x1 AS x73
      |  RETURN 74 AS x1
      |}
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "local.proc0" -> true
    )
  }

  test(
    """WHEN $x = 1 THEN {
      |  DEFINE PROCEDURE local.proc0() {
      |    CREATE (:N)
      |    RETURN 15 AS x2
      |  }
      |
      |  CALL local.proc0()
      |    YIELD x1 AS x73
      |  RETURN 74 AS x1
      |}
      |ELSE RETURN 10 AS x1
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "local.proc0" -> true
    )
  }

  test(
    """DEFINE FUNCTION foo.a(n) {
      |  DEFINE PROCEDURE bar(o) {
      |    DEFINE PROCEDURE set(o) {
      |      SET o:A
      |      FINISH
      |    }
      |
      |    CALL set(o)
      |    RETURN o
      |  }
      |
      |  CALL bar(n) YIELD o AS nUpdated
      |  RETURN labels(nUpdated) LIMIT 1
      |}
      |MATCH (a:A)
      |RETURN foo.a(a)
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "bar" -> true
    )
  }

  test(
    """DEFINE PROCEDURE foo(o) {
      |  DEFINE FUNCTION fun(n) {
      |    DEFINE PROCEDURE set(o) {
      |      SET o:B
      |      FINISH
      |    }
      |
      |
      |    CALL set(n)
      |    RETURN n LIMIT 1
      |  }
      |
      |  MATCH (a:A)
      |  RETURN fun(a) AS ab
      |}
      |
      |CALL foo() YIELD ab
      |RETURN ab
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "foo" -> true
    )
  }

  test(
    """DEFINE PROCEDURE foo(n) {
      |  DEFINE PROCEDURE bar(o) {
      |    DEFINE PROCEDURE set(o) {
      |      SET o:A
      |      FINISH
      |    }
      |
      |    CALL set(o)
      |    RETURN o
      |  }
      |
      |  CALL bar(n) YIELD o AS nUpdated
      |  RETURN labels(nUpdated) AS aUpdated
      |}
      |MATCH (a:A)
      |CALL foo(a) YIELD aUpdated
      |RETURN aUpdated
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "bar" -> true,
      "foo" -> true
    )
  }

  test(
    """DEFINE PROCEDURE foo(n) {
      |  DEFINE PROCEDURE bar(o) {
      |    DEFINE PROCEDURE set(o) {
      |      SET o:A
      |      FINISH
      |    }
      |
      |    RETURN EXISTS {
      |      CALL set(o)
      |    } AS success
      |  }
      |
      |  CALL bar(n) YIELD success
      |  RETURN labels(n) AS updated
      |}
      |MATCH (a:A)
      |CALL foo(a) YIELD updated
      |RETURN updated
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "bar" -> true,
      "foo" -> true
    )
  }

  test(
    """DEFINE PROCEDURE foo(n) {
      |  DEFINE PROCEDURE set(o) {
      |    //SET o:A // <<-- commented out
      |    FINISH
      |  }
      |  DEFINE PROCEDURE bar(o) {
      |    DEFINE FUNCTION fun(n) {
      |      LET y = EXISTS {
      |        CALL set(o)
      |      }
      |      RETURN 1 LIMIT 1
      |    }
      |
      |    UNWIND [fun(o)] AS x
      |    RETURN COUNT(x) AS cnt
      |
      |    NEXT
      |
      |    FINISH
      |  }
      |
      |  CALL bar(n)
      |  RETURN labels(n) AS updated
      |}
      |MATCH (a:A)
      |CALL foo(a) YIELD updated AS aUpdated
      |RETURN aUpdated
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> false,
      "bar" -> false,
      "foo" -> false
    )
  }

  test(
    """DEFINE PROCEDURE foo(n) {
      |  DEFINE PROCEDURE set(o) {
      |    DELETE o // <<-- not commented out
      |    FINISH
      |  }
      |  DEFINE PROCEDURE bar(o) {
      |    DEFINE FUNCTION fun(n) {
      |      LET y = EXISTS {
      |        CALL set(o)
      |      }
      |      RETURN 1 LIMIT 1
      |    }
      |
      |    UNWIND [fun(o)] AS x
      |    RETURN COUNT(x) AS cnt
      |
      |    NEXT
      |
      |    FINISH
      |  }
      |
      |  CALL bar(n)
      |  RETURN labels(n) AS updated
      |}
      |MATCH (a:A)
      |CALL foo(a) YIELD updated AS aUpdated
      |RETURN aUpdated
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "bar" -> true,
      "foo" -> true
    )
  }

  test(
    """DEFINE PROCEDURE set(o) {
      |  SET o:A // <<-- not commented out
      |  FINISH
      |}
      |DEFINE PROCEDURE foo(n) {
      |  DEFINE PROCEDURE bar(o) {
      |    DEFINE FUNCTION fun(n) {
      |      LET y = COLLECT {
      |        CALL set(o)
      |      }
      |      RETURN 1 LIMIT 1
      |    }
      |
      |    WHEN fun(o) = 0 THEN RETURN 0 AS x
      |    ELSE RETURN 1 AS x
      |
      |    NEXT
      |
      |    FINISH
      |  }
      |
      |  CALL bar(n)
      |  RETURN labels(n) AS updated
      |}
      |MATCH (a:A)
      |CALL foo(a) YIELD updated AS aUpdated
      |RETURN aUpdated
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "bar" -> true,
      "foo" -> true
    )
  }

  test(
    """DEFINE PROCEDURE foo(n) {
      |  DEFINE FUNCTION fun(n) {
      |    LET y = COUNT {
      |      DEFINE PROCEDURE set(o) {
      |        REMOVE o:A // <<-- not commented out
      |        FINISH
      |      }
      |
      |      CALL set(o)
      |    }
      |    RETURN 1 LIMIT 1
      |  }
      |  DEFINE PROCEDURE bar(o) {
      |
      |    UNWIND [fun(o)] AS x
      |    RETURN COUNT(x) AS cnt
      |
      |    NEXT
      |
      |    FINISH
      |  }
      |
      |  CALL bar(n)
      |  RETURN labels(n) AS updated
      |}
      |MATCH (a:A)
      |CALL foo(a) YIELD updated AS aUpdated
      |RETURN aUpdated
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "bar" -> true,
      "foo" -> true
    )
  }

  test(
    """DEFINE PROCEDURE foo(n) {
      |  DEFINE FUNCTION fun(n) {
      |    LET y = COUNT {
      |      DEFINE PROCEDURE set(o) {
      |        CALL nonLocal.update.proc()
      |        FINISH
      |      }
      |
      |      CALL set(o)
      |    }
      |    RETURN 1 LIMIT 1
      |  }
      |  DEFINE PROCEDURE bar(o) {
      |
      |    UNWIND [fun(o)] AS x
      |    RETURN COUNT(x) AS cnt
      |
      |    NEXT
      |
      |    FINISH
      |  }
      |
      |  CALL bar(n)
      |  RETURN labels(n) AS updated
      |}
      |MATCH (a:A)
      |CALL foo(a) YIELD updated AS aUpdated
      |RETURN aUpdated
      |""".stripMargin
  ) {
    cypher25OnwardsTestName.hasResolvedLocalCallWithBodyContainsUpdates(
      "set" -> true,
      "bar" -> true,
      "foo" -> true
    )
  }

  /*
   * Test procedure resolution under a variety of input signatures
   */
  for {
    (sig, args, argsCoercedOpt) <- Seq(
      (Seq.empty[LocalFieldSignature], Seq(literalInt(1)), None),
      (Seq(localFieldSignature("a")), Seq.empty, None),
      (
        Seq(localFieldSignature("a", symbols.CTString)),
        Seq(literalString("abc")),
        Some(Seq(coerceTo(literalString("abc"), symbols.CTString)))
      ),
      (Seq(localFieldSignature("a"), localFieldSignature("b")), Seq(literalInt(1)), None),
      (
        Seq(localFieldSignature("a"), localFieldSignature("b"), localFieldSignature("c", literalInt(0))),
        Seq(literalInt(1)),
        None
      ),
      (Seq(localFieldSignature("a"), localFieldSignature("b", literalInt(0))), Seq.empty, None),
      (
        Seq(localFieldSignature("a"), localFieldSignature("b", literalInt(0))),
        Seq(literalInt(1), literalInt(2), literalInt(3)),
        None
      )
    )
    sigCypher = sig.map(f => prettify(f)).mkString(", ")
    argsCypher = args.map(a => prettify(a)).mkString(", ")
  } {
    test(
      s"""DEFINE PROCEDURE foo($sigCypher) {
         |  RETURN 1 AS x
         |}
         |
         |CALL foo($argsCypher) YIELD x
         |RETURN x""".stripMargin
    ) {
      val foo = localProcedureDefinition("foo", sig: _*).body(
        return_(
          aliasedReturnItem(literalInt(1), "x")
        )
      )
      cypher25OnwardsTestName.hasExtractedLocalProcedures(
        procedureName("foo") -> foo
      ).isRewrittenTo(
        singleQueryWithLocalDefinitions(
          foo
        )(
          /* NOTE:
           * call resolution is purely by name
           * the arguments are checked in semantics analysis, which is not part of this test
           */
          resolvedLocalCall(
            procedureName("foo"),
            inputSignature = sig,
            outputSignature = Some(Seq(localFieldSignature("x"))),
            callArguments = argsCoercedOpt.getOrElse(args),
            callResults = Seq(None -> "x")
          ),
          return_(aliasedReturnItem(varFor("x"), "x"))
        )
      )
    }
  }

  for {
    parameters <- Seq(
      Seq("x"),
      Seq("x", "y"),
      Seq("x", "y", "z"),
      ('a' to 'z').map(_.toString)
    )
    (fieldsSeq, fieldDefs) <- Seq(
      (
        parameters,
        parameters.map(v => localFieldSignature(v))
      ),
      (
        parameters.map(v => s"$v :: INT"),
        parameters.map(v => localFieldSignature(v, symbols.CTInteger))
      )
    )
    fields = fieldsSeq.mkString(", ")
    argsSeq = parameters.zipWithIndex.map(p => literalInt(p._2))
    args = argsSeq.map(_.stringVal).mkString(", ")
    callArguments = fieldDefs.zip(argsSeq).map {
      case (fieldDef, arg) => fieldDef.typ.map(t => coerceTo(arg, t)).getOrElse(arg)
    }
    innerReturnStr = parameters.mkString(" + ")
    innerReturnAst = {
      val vars = parameters.map(v => varFor(v))
      vars.tail.foldLeft(vars.head.asInstanceOf[Expression]) {
        case (exp, v) => add(exp, v)
      }
    }
  } {
    test(
      s"""DEFINE PROCEDURE foo($fields) {
         |  RETURN $innerReturnStr AS x
         |}
         |
         |CALL foo($args) YIELD x
         |RETURN x""".stripMargin
    ) {
      val foo = localProcedureDefinition(
        "foo",
        fieldDefs: _*
      ).body(
        return_(
          aliasedReturnItem(innerReturnAst, "x")
        )
      )
      cypher25OnwardsTestName.hasExtractedLocalProcedures(
        procedureName("foo") -> foo
      ).isRewrittenTo(
        singleQueryWithLocalDefinitions(
          foo
        )(
          resolvedLocalCall(
            procedureName("foo"),
            inputSignature = fieldDefs,
            outputSignature = Some(Seq(localFieldSignature("x"))),
            callArguments = callArguments,
            callResults = Seq(None -> "x")
          ),
          return_(aliasedReturnItem(varFor("x"), "x"))
        )
      )
    }
  }

  /*
   * Test procedure resolution under a fuzzed variety of query compositions
   */
  { // generated test cases
    val rand = new Random(0)

    def pickOne[T](list: Seq[T]): T = {
      list(rand.nextInt(list.size))
    }

    def pickOneWeighted[T](list: Seq[(Int, T)]): T = {
      val l = list.foldLeft(Seq.empty[T]) {
        case (l, (i, t)) => l ++ List.fill(i)(t)
      }
      l(rand.nextInt(l.size))
    }

    class Counter {
      private var i = -1
      def next(): Int = {
        i = i + 1
        i
      }
    }

    case class GenCtx(
      rand: Random,
      depthLimit: Int,
      availableProc: Seq[GenLocalProc] = Seq.empty,
      availableFunc: Seq[GenLocalFunc] = Seq.empty,
      requestedReturnCol: Option[String] = None,
      counter: Counter = new Counter(),
      depth: Int = 0,
      inImportingWithSubquery: Boolean = false
    ) {
      def depthLimited(w: Int): Int = if (depth < depthLimit) w else 0

      def getChildCtx: GenCtx = copy(depth = depth + 1)
      def getChildCtxWithoutRequestedReturnCol: GenCtx = copy(depth = depth + 1, requestedReturnCol = None)
    }

    trait GenPart {
      def ctx: GenCtx
      def isUpdating: Boolean
      def ast(resolved: Boolean): ASTNode
      def procedures: Seq[GenLocalProc]
      def functions: Seq[GenLocalFunc]
      def resolvedCallsWithUpdateInfo: Set[(String, Boolean)]
      def children: Seq[GenPart]
      def cypher: String = prettify(ast(resolved = false))
      def printlnRecursive(print: (GenPart, Int) => Unit, indent: Int = 0): Unit = {
        print(this, indent)
        children.foreach { c =>
          c.printlnRecursive(print, indent + 1)
        }
      }
      def debugInfo(): String =
        this.getClass.getSimpleName + " " + cypher.linesIterator.nextOption().getOrElse("") + "..."
    }

    trait GenExpression extends GenPart {
      def ast(resolved: Boolean): Expression
    }

    case class GenSomeExpression0(ctx: GenCtx, gen: () => Expression, isUpdating: Boolean = false)
        extends GenExpression {
      override def ast(resolved: Boolean): Expression = gen()
      override def procedures: Seq[GenLocalProc] = Seq.empty
      override def functions: Seq[GenLocalFunc] = Seq.empty
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = Set.empty
      override def children: Seq[GenPart] = Seq.empty
    }

    case class GenSomeExpression1(
      ctx: GenCtx,
      op1: GenExpression,
      gen: Expression => Expression,
      isItselfUpdating: Boolean = false
    ) extends GenExpression {
      override val isUpdating: Boolean = isItselfUpdating || op1.isUpdating
      override def ast(resolved: Boolean): Expression = gen(op1.ast(resolved))
      override def procedures: Seq[GenLocalProc] = op1.procedures
      override def functions: Seq[GenLocalFunc] = op1.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = op1.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(op1)
    }

    case class GenSomeExpression2(
      ctx: GenCtx,
      op1: GenExpression,
      op2: GenExpression,
      gen: (Expression, Expression) => Expression,
      isItselfUpdating: Boolean = false
    ) extends GenExpression {
      override val isUpdating: Boolean = isItselfUpdating || op1.isUpdating || op2.isUpdating
      override def ast(resolved: Boolean): Expression = gen(op1.ast(resolved), op2.ast(resolved))
      override def procedures: Seq[GenLocalProc] = op1.procedures ++ op2.procedures
      override def functions: Seq[GenLocalFunc] = op1.functions ++ op2.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] =
        op1.resolvedCallsWithUpdateInfo union op2.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(op1, op2)
    }

    case class GenFunctionInvocation(ctx: GenCtx) extends GenExpression {
      private val availableFuncNames =
        ctx.availableFunc.map(f => (f.name, true)) ++ Seq(
          (functionName("e"), false)
        )
      private val functionToCall = pickOne(availableFuncNames)
      override val isUpdating: Boolean = functionToCall._2
      override def ast(resolved: Boolean): Expression = functionToCall match {
        case (fn, true) => FunctionInvocation(
            functionName = fn,
            distinct = false,
            args = IndexedSeq.empty,
            maybeLocalFunction = Some(
              LocalFunction(
                functionName = fn,
                parameterTypes = IndexedSeq.empty,
                outputSignature = None
              )
            )
          )(pos)
        case (fn, false) => FunctionInvocation(
            functionName = fn,
            distinct = false,
            args = IndexedSeq.empty,
            maybeLocalFunction = None
          )(pos)
      }
      override def procedures: Seq[GenLocalProc] = Seq.empty
      override def functions: Seq[GenLocalFunc] = Seq.empty
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = Set.empty
      override def children: Seq[GenPart] = Seq.empty
    }

    case class GenExistsSubqueryExpression(ctx: GenCtx) extends GenExpression {
      private val body = genQuery(ctx)
      override val isUpdating: Boolean = body.isUpdating
      override def ast(resolved: Boolean): Expression =
        ExistsExpression(body.ast(resolved))(pos, None, None)
      override def procedures: Seq[GenLocalProc] = body.procedures
      override def functions: Seq[GenLocalFunc] = body.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = body.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(body)
    }

    case class GenCountSubqueryExpression(ctx: GenCtx) extends GenExpression {
      private val body = genQuery(ctx)
      override val isUpdating: Boolean = body.isUpdating
      override def ast(resolved: Boolean): Expression =
        CountExpression(body.ast(resolved))(pos, None, None)
      override def procedures: Seq[GenLocalProc] = body.procedures
      override def functions: Seq[GenLocalFunc] = body.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = body.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(body)
    }

    case class GenCollectSubqueryExpression(ctx: GenCtx) extends GenExpression {
      private val body = genQuery(ctx)
      override val isUpdating: Boolean = body.isUpdating
      override def ast(resolved: Boolean): Expression =
        CollectExpression(body.ast(resolved))(pos, None, None)
      override def procedures: Seq[GenLocalProc] = body.procedures
      override def functions: Seq[GenLocalFunc] = body.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = body.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(body)
    }

    sealed trait GenLocalCallabel extends GenPart {
      override def ast(resolved: Boolean): LocalCallableDefinition
    }

    case class GenLocalProc(ctx: GenCtx) extends GenLocalCallabel {
      val name: ProcedureName = procedureName("local", s"proc${ctx.counter.next()}")
      private val body: GenQuery = genQuery(ctx)
      val procedureResultCol = body.actualReturnCol
      override val isUpdating = body.isUpdating
      override def ast(resolved: Boolean): LocalProcedureDefinition =
        localProcedureDefinition(name.fullName).body(body.ast(resolved))
      override def procedures: Seq[GenLocalProc] = body.procedures
      override def functions: Seq[GenLocalFunc] = body.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = body.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(body)
    }

    case class GenLocalFunc(ctx: GenCtx) extends GenLocalCallabel {
      val name: FunctionName = functionName("local", s"func${ctx.counter.next()}")
      private val body: GenExpression = genExpression(ctx)
      override val isUpdating = body.isUpdating
      override def ast(resolved: Boolean): LocalFunctionDefinition =
        localFunctionDefinition(name.fullName).body(body.ast(resolved))
      override def procedures: Seq[GenLocalProc] = body.procedures
      override def functions: Seq[GenLocalFunc] = body.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = body.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(body)
    }

    trait GenQuery extends GenPart {
      def actualReturnCol: String
      override def ast(resolved: Boolean): Query
    }

    case class GenQueryWithProcs(ctx: GenCtx, numProc: Int, numFunc: Int) extends GenQuery {
      {
        if (numProc + numFunc < 1)
          throw new RuntimeException("GenQueryWithProcs.numProc + GenQueryWithProcs.numFunc shall be at least 1")
      }
      private val callables: Seq[GenLocalCallabel] = {
        val procOrFunc = ctx.rand.shuffle((0 until numProc).map(_ => true) ++ (0 until numFunc).map(_ => false))
        procOrFunc.map {
          case true  => GenLocalProc(ctx.copy(requestedReturnCol = None))
          case false => GenLocalFunc(ctx.copy(requestedReturnCol = None))
        }
      }
      val (myProcedures, myFunctions) = callables.partitionMap {
        case p: GenLocalProc => Left(p)
        case f: GenLocalFunc => Right(f)
      }
      private val query = genQueryAfterDefinition(ctx.copy(
        availableProc = ctx.availableProc ++ myProcedures,
        availableFunc = ctx.availableFunc ++ myFunctions
      ))
      override val actualReturnCol: String = query.actualReturnCol
      override val isUpdating: Boolean = query.isUpdating
      override def ast(resolved: Boolean): Query = queryWithLocalDefinitions(
        callables.map(_.ast(resolved)): _*
      )(
        query.ast(resolved)
      )
      override def procedures: Seq[GenLocalProc] = callables.flatMap {
        case p: GenLocalProc => p +: p.procedures
        case f: GenLocalFunc => f.procedures
      }
      override def functions: Seq[GenLocalFunc] = callables.flatMap {
        case p: GenLocalProc => p.functions
        case f: GenLocalFunc => f +: f.functions
      }
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] =
        callables.flatMap(_.resolvedCallsWithUpdateInfo).toSet union query.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = callables :+ query
    }

    case class GenNext(ctx: GenCtx, numStmt: Int) extends GenQuery {
      {
        if (numStmt < 2) throw new RuntimeException("GenNext.numStmt shall be at least 2")
      }
      private val stmts =
        (0 until numStmt).map(_ => genQueryUnderNext(ctx))
      override val actualReturnCol: String = stmts.last.actualReturnCol
      override val isUpdating: Boolean = stmts.exists(_.isUpdating)
      override def ast(resolved: Boolean): Query = nextStatement(stmts.map(_.ast(resolved)): _*)
      override def procedures: Seq[GenLocalProc] = stmts.foldLeft(Seq.empty[GenLocalProc]) {
        case (agg, stmt) => agg ++ stmt.procedures
      }
      override def functions: Seq[GenLocalFunc] = stmts.foldLeft(Seq.empty[GenLocalFunc]) {
        case (agg, stmt) => agg ++ stmt.functions
      }
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] =
        stmts.foldLeft(Set.empty[(String, Boolean)]) {
          case (agg, stmt) => agg union stmt.resolvedCallsWithUpdateInfo
        }
      override def children: Seq[GenPart] = stmts
    }

    case class GenConditional(numBranches: Int, ctx: GenCtx) extends GenQuery {
      {
        if (numBranches < 1) throw new RuntimeException("GenConditional.numBranches shall be at least 1")
      }
      private val withDefault = numBranches > 1
      private val branchConditions =
        (0 until numBranches).map(i => {
          Some(GenSomeExpression1(
            ctx,
            genScalarSubqueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => parameter("cond", symbols.CTAny))),
            op1 => AstConstructionTestSupport.equals(op1, literalInt(i))
          ))
        })
      private val firstBranchBody = genPartQuery(ctx)
      override val actualReturnCol: String = firstBranchBody.actualReturnCol
      private val remainingBranchBodies =
        branchConditions.tail.map(_ => genPartQuery(ctx.copy(requestedReturnCol = Some(actualReturnCol))))
      private val branchBodies = firstBranchBody +: remainingBranchBodies
      private val whenBranches =
        if (withDefault) branchConditions.tail.zip(branchBodies.tail) else branchConditions.zip(branchBodies)
      private val elseBranch = if (withDefault) Some(branchBodies.head) else None
      override val isUpdating: Boolean = whenBranches.exists {
        case (Some(cond), body) => cond.isUpdating || body.isUpdating
      } || elseBranch.exists(_.isUpdating)
      override def ast(resolved: Boolean): Query = {
        val branches = whenBranches.collect {
          case (Some(cond), body) => conditionalQueryBranch(cond.ast(resolved), body.ast(resolved))
        }
        val default = conditionalQueryDefault(elseBranch.map(_.ast(resolved)))
        conditionalQueryWhen(default, branches: _*)
      }
      override def procedures: Seq[GenLocalProc] = branchBodies.foldLeft(Seq.empty[GenLocalProc]) {
        case (agg, stmt) => agg ++ stmt.procedures
      }
      override def functions: Seq[GenLocalFunc] = branchBodies.foldLeft(Seq.empty[GenLocalFunc]) {
        case (agg, stmt) => agg ++ stmt.functions
      }
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] =
        whenBranches.foldLeft(Set.empty[(String, Boolean)]) {
          case (agg, (Some(expr), body)) =>
            agg union expr.resolvedCallsWithUpdateInfo union body.resolvedCallsWithUpdateInfo
        } union elseBranch.fold(Set.empty[(String, Boolean)])(_.resolvedCallsWithUpdateInfo)
      override def children: Seq[GenPart] = whenBranches.flatMap(b => b._1.toSeq :+ b._2) ++ elseBranch.toSeq
    }

    case class GenUnion(ctx: GenCtx, distinct: Boolean) extends GenQuery {
      private val lhs = genPartQuery(ctx)
      override val actualReturnCol: String = lhs.actualReturnCol
      private val rhs = genPartQuery(ctx.copy(requestedReturnCol = Some(actualReturnCol)))
      override val isUpdating: Boolean = lhs.isUpdating || rhs.isUpdating
      override def ast(resolved: Boolean): Query = {
        if (distinct) union(lhs.ast(resolved), rhs.ast(resolved))
        else unionAll(lhs.ast(resolved), rhs.ast(resolved))
      }
      override def procedures: Seq[GenLocalProc] = lhs.procedures ++ rhs.procedures
      override def functions: Seq[GenLocalFunc] = lhs.functions ++ rhs.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] =
        lhs.resolvedCallsWithUpdateInfo union rhs.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(lhs, rhs)
    }

    trait GenPartQuery extends GenQuery {
      override def ast(resolved: Boolean): PartQuery
    }

    case class GenTopLevelBraces(ctx: GenCtx) extends GenPartQuery {
      private val body = genQuery(ctx)
      override val actualReturnCol: String = body.actualReturnCol
      override val isUpdating: Boolean = body.isUpdating
      override def ast(resolved: Boolean): PartQuery = topLevelBraces(body.ast(resolved))
      override def procedures: Seq[GenLocalProc] = body.procedures
      override def functions: Seq[GenLocalFunc] = body.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = body.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(body)
    }

    case class GenSimpleQuery(ctx: GenCtx, numClauses: Int) extends GenPartQuery {
      {
        if (numClauses < 0) throw new RuntimeException("GenSimpleQuery.numClauses shall be at least 0")
      }
      private val i = ctx.counter.next()
      override val actualReturnCol: String = ctx.requestedReturnCol.getOrElse(s"x$i")
      private val clauses = (0 until numClauses).map(_ => genClause(ctx))
      private val returnExpression = genScalarSubqueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => literalInt(i)))
      override val isUpdating: Boolean = clauses.exists(_.isUpdating) || returnExpression.isUpdating
      private def clauses(resolved: Boolean): Seq[Clause] =
        clauses.map(_.ast(resolved)) :+ return_(aliasedReturnItem(returnExpression.ast(resolved), actualReturnCol))
      override def ast(resolved: Boolean): PartQuery = singleQuery(clauses(resolved): _*)
      override def procedures: Seq[GenLocalProc] = clauses.flatMap(_.procedures)
      override def functions: Seq[GenLocalFunc] = clauses.flatMap(_.functions)
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] =
        clauses.flatMap(_.resolvedCallsWithUpdateInfo).toSet union returnExpression.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = clauses :+ returnExpression
    }

    trait GenClause extends GenPart {
      override def ast(resolved: Boolean): Clause
    }

    case class GenCall(ctx: GenCtx) extends GenClause {
      case class Proc(
        name: ProcedureName,
        isLocal: Boolean,
        procedureResultCol: Option[String],
        isUpdating: Boolean,
        canBeResolved: Boolean
      )
      private val availableProcNames =
        ctx.availableProc.map(p =>
          Math.max(5, ctx.availableProc.size) -> Proc(p.name, true, Some(p.procedureResultCol), p.isUpdating, true)
        ) ++ Seq(
          1 -> Proc(
            procedureName(ResolverMockMarker.NON_LOCAL, ResolverMockMarker.UNRESOLVED, "proc"),
            false,
            None,
            false,
            false
          ),
          1 -> Proc(procedureName(ResolverMockMarker.NON_LOCAL, "read", "proc"), false, None, false, true),
          1 -> Proc(
            procedureName(ResolverMockMarker.NON_LOCAL, ResolverMockMarker.UPDATE, "proc"),
            false,
            None,
            true,
            true
          )
        )
      private val alias = s"x${ctx.counter.next()}"
      private val procedureToCall = pickOneWeighted(availableProcNames)
      override val isUpdating: Boolean = procedureToCall.isUpdating
      override def ast(resolved: Boolean): Clause = procedureToCall match {
        case Proc(pn, true, resultColOpt, u, true) if resolved =>
          resolvedLocalCall(
            pn,
            outputSignature = Some(resultColOpt.map(r => localFieldSignature(r)).toSeq),
            callResults = Seq(resultColOpt -> alias),
            bodyContainsUpdates = u
          )
        case Proc(pn, false, _, u, true) if resolved => resolvedNonLocalCall(pn, bodyContainsUpdates = u)
        case Proc(pn, _, Some(resultCol), _, _) =>
          unresolvedCallWithYield(
            pn,
            yields = Seq(varFor(resultCol) -> varFor(alias))
          )
        case Proc(pn, _, None, _, _) => unresolvedCall(pn)
      }
      override def procedures: Seq[GenLocalProc] = Seq.empty
      override def functions: Seq[GenLocalFunc] = Seq.empty
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] =
        if (procedureToCall.isLocal) Set(procedureToCall.name.fullName -> procedureToCall.isUpdating) else Set.empty
      override def children: Seq[GenPart] = Seq.empty
    }

    case class GenLet(ctx: GenCtx) extends GenClause {
      private val i = ctx.counter.next()
      private val expression = GenSomeExpression1(
        ctx,
        genScalarSubqueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => parameter("foo", symbols.CTAny))),
        op => isNotNull(op)
      )
      override val isUpdating: Boolean = expression.isUpdating
      override def ast(resolved: Boolean): Clause = withAdditionalItemsTyped(
        ParsedAsLet,
        aliasedReturnItem(expression.ast(resolved), ctx.requestedReturnCol.getOrElse(s"x$i"))
      )
      override def procedures: Seq[GenLocalProc] = expression.procedures
      override def functions: Seq[GenLocalFunc] = expression.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = expression.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(expression)
    }

    case class GenFilter(ctx: GenCtx) extends GenClause {
      private val expression = GenSomeExpression1(
        ctx,
        genScalarSubqueryOrElse(ctx, ctx => GenSomeExpression0(ctx, () => parameter("foo", symbols.CTAny))),
        op => isNotNull(op)
      )
      override val isUpdating: Boolean = expression.isUpdating
      override def ast(resolved: Boolean): Clause = withAllTyped(
        Some(where(expression.ast(resolved))),
        ParsedAsFilter
      )
      override def procedures: Seq[GenLocalProc] = expression.procedures
      override def functions: Seq[GenLocalFunc] = expression.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = expression.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(expression)
    }

    case class GenInlineCall(ctx: GenCtx, withScopeClause: Boolean) extends GenClause {
      private val subquery = genQuery(ctx.copy(inImportingWithSubquery = !withScopeClause))
      override val isUpdating: Boolean = subquery.isUpdating
      override def ast(resolved: Boolean): Clause = {
        val subqueryAst = subquery.ast(resolved)
        if (withScopeClause) scopeClauseSubqueryCall(isImportingAll = false, Seq.empty, subqueryAst)
        else importingWithSubqueryCall(subqueryAst)
      }
      override def procedures: Seq[GenLocalProc] = subquery.procedures
      override def functions: Seq[GenLocalFunc] = subquery.functions
      override def resolvedCallsWithUpdateInfo: Set[(String, Boolean)] = subquery.resolvedCallsWithUpdateInfo
      override def children: Seq[GenPart] = Seq(subquery)
    }

    def genExpression(ctx: GenCtx): GenExpression = {
      pickOneWeighted(Seq(
        1 -> (() => genScalarSubquery(ctx)),
        1 -> (() => GenSomeExpression1(ctx, genExpressionTerminal(ctx), op => isNotNull(op))),
        1 -> (() =>
          GenSomeExpression2(
            ctx,
            genExpressionTerminal(ctx),
            genExpressionTerminal(ctx),
            (op1, op2) => equals(op1, op2)
          )
        ),
        1 -> (() =>
          GenSomeExpression2(
            ctx,
            genExpressionTerminal(ctx),
            genExpressionTerminal(ctx),
            (op1, op2) => add(op1, op2)
          )
        )
      ))()
    }

    def genExpressionTerminal(ctx: GenCtx): GenExpression = {
      pickOneWeighted(Seq(
        1 -> (() => GenSomeExpression0(ctx, () => parameter("foo", symbols.CTAny))),
        1 -> (() => GenSomeExpression0(ctx, () => nullLiteral)),
        1 -> (() => GenSomeExpression0(ctx, () => literalInt(123))),
        1 -> (() => GenSomeExpression0(ctx, () => literalString("abc"))),
        3 -> (() => GenFunctionInvocation(ctx))
      ))()
    }

    def genScalarSubqueryOrElse(ctx: GenCtx, orElse: GenCtx => GenExpression): GenExpression = {
      pickOneWeighted(Seq(
        ctx.depthLimited(1) -> (() => genScalarSubquery(ctx)),
        4 -> (() => orElse(ctx))
      ))()
    }

    def genScalarSubquery(ctx: GenCtx): GenExpression = {
      val childCtx = ctx.getChildCtxWithoutRequestedReturnCol
      pickOneWeighted(Seq(
        1 -> (() => GenExistsSubqueryExpression(childCtx)),
        1 -> (() => GenCountSubqueryExpression(childCtx)),
        1 -> (() => GenCollectSubqueryExpression(childCtx))
      ))()
    }

    def genClause(ctx: GenCtx): GenClause = {
      val childCtx = ctx.getChildCtxWithoutRequestedReturnCol
      pickOneWeighted(Seq(
        1 -> (() => GenLet(childCtx)),
        1 -> (() => GenFilter(childCtx)),
        1 -> (() => GenInlineCall(childCtx, withScopeClause = true)),
        1 -> (() => GenInlineCall(childCtx, withScopeClause = false)),
        3 -> (() => GenCall(childCtx))
      ))()
    }

    def genPartQuery(ctx: GenCtx): GenPartQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            1 -> (() => GenSimpleQuery(childCtx, 0)),
            1 -> (() => GenSimpleQuery(childCtx, 1)),
            1 -> (() => GenSimpleQuery(childCtx, 2)),
            1 -> (() => GenSimpleQuery(childCtx, 5))
          )
        else
          Seq(
            1 -> (() => GenSimpleQuery(childCtx, 0)),
            1 -> (() => GenSimpleQuery(childCtx, 1)),
            1 -> (() => GenSimpleQuery(childCtx, 2)),
            1 -> (() => GenSimpleQuery(childCtx, 5)),
            ctx.depthLimited(1) -> (() => GenTopLevelBraces(childCtx))
          )
      )()
    }

    def genQueryUnderNext(ctx: GenCtx): GenQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            ctx.depthLimited(2) -> (() => GenUnion(childCtx, distinct = true)),
            ctx.depthLimited(1) -> (() => GenUnion(childCtx, distinct = false)),
            1 -> (() => genPartQuery(childCtx))
          )
        else
          Seq(
            ctx.depthLimited(2) -> (() => GenUnion(childCtx, distinct = true)),
            ctx.depthLimited(1) -> (() => GenUnion(childCtx, distinct = false)),
            ctx.depthLimited(1) -> (() => GenConditional(1, childCtx)),
            ctx.depthLimited(1) -> (() => GenConditional(2, childCtx)),
            ctx.depthLimited(1) -> (() => GenConditional(3, childCtx)),
            3 -> (() => genPartQuery(childCtx))
          )
      )()
    }

    def genQueryAfterDefinition(ctx: GenCtx): GenQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            ctx.depthLimited(1) -> (() => genQueryUnderNext(childCtx)),
            1 -> (() => genPartQuery(childCtx))
          )
        else
          Seq(
            ctx.depthLimited(1) -> (() => GenNext(childCtx, 2)),
            ctx.depthLimited(1) -> (() => GenNext(childCtx, 3)),
            ctx.depthLimited(1) -> (() => GenNext(childCtx, 5)),
            ctx.depthLimited(3) -> (() => genQueryUnderNext(childCtx)),
            3 -> (() => genPartQuery(childCtx))
          )
      )()
    }

    def genQuery(ctx: GenCtx): GenQuery = {
      val childCtx = ctx.getChildCtx
      pickOneWeighted(
        if (ctx.inImportingWithSubquery)
          Seq(
            ctx.depthLimited(1) -> (() => genQueryAfterDefinition(childCtx)),
            ctx.depthLimited(1) -> (() => genQueryUnderNext(childCtx)),
            1 -> (() => genPartQuery(childCtx))
          )
        else
          Seq(
            ctx.depthLimited(2) -> (() => GenQueryWithProcs(childCtx, 1, 0)),
            ctx.depthLimited(2) -> (() => GenQueryWithProcs(childCtx, 0, 1)),
            ctx.depthLimited(1) -> (() => GenQueryWithProcs(childCtx, 1, 1)),
            ctx.depthLimited(1) -> (() => GenQueryWithProcs(childCtx, 3, 3)),
            ctx.depthLimited(3) -> (() => genQueryAfterDefinition(childCtx)),
            ctx.depthLimited(3) -> (() => genQueryUnderNext(childCtx)),
            3 -> (() => genPartQuery(childCtx))
          )
      )()
    }

    val generatedTests = for {
      i <- 0 until numberOfFuzzTests
      testCase = genQuery(GenCtx(new Random(i), depthLimit = 10))
    } yield i -> testCase

    for {
      (seed, testCase) <- generatedTests.distinctBy(_._2.cypher)
      extractedLocalProcedures = testCase.procedures.map(p =>
        p.name -> p.ast(resolved = true)
      )
      extractedLocalFunctions = testCase.functions.map(f =>
        f.name -> f.ast(resolved = true)
      )
    } {
      test(
        s"""/* seed $seed */
           |${testCase.cypher}""".stripMargin
      ) {
        // for debug purposes, output the generator tree for a specific seed
        if (false /* seed == 71 */ ) {
          fail({
            val s = new StringBuilder()
            testCase.printlnRecursive((gen, indent) =>
              gen match {
                case _ =>
                  s.append(s"${" " * indent}- ${gen.debugInfo()} -> ${gen.isUpdating}")
                  s.append(System.lineSeparator())
              }
            )
            s.toString()
          })
        }
        // regular test assertion
        cypher25OnwardsTestName.hasExtractedLocalProcedures(
          extractedLocalProcedures: _*
        ).hasExtractedLocalFunctions(
          extractedLocalFunctions: _*
        ).hasResolvedLocalCallWithBodyContainsUpdates(
          testCase.resolvedCallsWithUpdateInfo.toSeq: _*
        ).isRewrittenTo(testCase.ast(resolved = true))
      }
    }
  }

  /*
   * test infrastructure
   */
  private def resolvedLocalCall(
    procedureName: ProcedureName,
    inputSignature: Seq[LocalFieldSignature] = Seq.empty,
    outputSignature: Option[Seq[LocalFieldSignature]] = Some(Seq.empty),
    bodyContainsUpdates: Boolean = false,
    callArguments: Seq[Expression] = Seq.empty,
    callResults: Seq[(Option[String], String)] = Seq.empty,
    // true if given by the user originally
    declaredArguments: Boolean = true,
    // true if given by the user originally
    declaredResults: Boolean = true,
    // YIELD *
    yieldAll: Boolean = false,
    optionalState: OptionalState = NonOptional
  ): ResolvedLocalCall =
    ResolvedLocalCall(
      procedureName = procedureName,
      inputSignature = inputSignature,
      outputSignature = outputSignature,
      bodyContainsUpdates = bodyContainsUpdates,
      callArguments = callArguments,
      callResults = callResults.map {
        case (source, variable) =>
          ProcedureResultItem(source.map(col => ProcedureOutput(col)(pos)), varFor(variable))(pos)
      }.toIndexedSeq,
      declaredArguments = declaredArguments,
      declaredResults = declaredResults,
      yieldAll = yieldAll,
      optionalState = optionalState
    )(pos)

  private def resolvedNonLocalCall(
    procedureName: ProcedureName,
    bodyContainsUpdates: Boolean
  ): ResolvedNonLocalCall =
    ResolvedNonLocalCall(
      signature = ProcedureSignature(
        name = procedureName,
        inputSignature = IndexedSeq.empty,
        outputSignature = None,
        deprecationInfo = None,
        accessMode = if (bodyContainsUpdates) ProcedureReadWriteAccess else ProcedureReadOnlyAccess,
        id = 0
      ),
      callArguments = Seq.empty,
      callResults = IndexedSeq.empty,
      declaredArguments = true,
      declaredResults = false,
      yieldAll = false,
      optionalState = NonOptional
    )(pos)

  private val messageProvider: ErrorMessageProvider = NotImplementedErrorMessageProvider

  trait TestCase {
    def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase
    def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase
    def isRewrittenTo(ast: Statement): TestCase
    def hasResolvedLocalCallWithBodyContainsUpdates(expected: (String, Boolean)*): TestCase
  }

  trait TestCaseWithVersion extends TestCase {
    def cypherVersion: CypherVersion
  }

  case class NoOpTestCase(cypherVersion: CypherVersion) extends TestCaseWithVersion {
    def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase = this
    def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase = this
    def isRewrittenTo(ast: Statement): TestCase = this
    def hasResolvedLocalCallWithBodyContainsUpdates(expected: (String, Boolean)*): TestCase = this
  }

  case class FailedTestCase(
    cypherVersion: CypherVersion,
    query: String,
    astOpt: Option[Statement],
    exception: Throwable
  ) extends TestCaseWithVersion {
    def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase = fail()
    def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase = fail()
    def isRewrittenTo(ast: Statement): TestCase = fail()
    def hasResolvedLocalCallWithBodyContainsUpdates(expected: (String, Boolean)*): TestCase = fail()

    private def fail(): TestCase = {
      Assertions.fail(
        s"""Version: $cypherVersion
           |Query:
           |
           |$testName
           |
           |has exception:
           |
           |${exception.getClass.getCanonicalName}: ${exception.getMessage}
           |
           |AST:
           |
           |${astOpt.map(pprint.apply(_, width = astRenderWidth, height = astRenderHeight)).getOrElse("—")}
           |
           |AST prettified:
           |
           |${astOpt.map(prettify).getOrElse("—")}
           |""".stripMargin
      )
      NoOpTestCase(cypherVersion)
    }
  }

  case class RanTestCasePerVersion(
    cypherVersion: CypherVersion,
    query: String,
    statementBefore: Statement,
    statementAfter: Statement,
    localDefinitionsDirectory: LocalDefinitionsDirectory
  ) extends TestCaseWithVersion {

    override def hasExtractedLocalProcedures(expectedProcedures: (
      ProcedureName,
      LocalProcedureDefinition
    )*): TestCase = {
      withClue(s"[has extracted local procedures]") {
        val actualProcedureDefinitions = localDefinitionsDirectory.localProcedureDefinitions
        expectedProcedures.foreach {
          case (name, expected) =>
            withClue(s"[${name.fullName}]") {
              val actualProcedureDefinitionOpt = actualProcedureDefinitions.get(name)
              withClue(s"[is present]") {
                if (actualProcedureDefinitionOpt.isEmpty) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Procedure: ${name.fullName}
                       |
                       |Procedure is expected to be present in extraction but is not.
                       |
                       |AST Before:
                       |
                       |${pprint.apply(statementBefore, width = astRenderWidth, height = astRenderHeight)}
                       |
                       |AST After:
                       |
                       |${pprint.apply(statementAfter, width = astRenderWidth, height = astRenderHeight)}
                       |---
                       |Actual local procedure definitions:
                       |
                       |${pprint.apply(
                        localDefinitionsDirectory.localProcedureDefinitions.toSeq.sortBy(_._1.toString),
                        width = astRenderWidth,
                        height = astRenderHeight
                      )}
                       |---
                       |Expected local procedure definitions:
                       |
                       |${pprint.apply(
                        expectedProcedures.sortBy(_._1.toString),
                        width = astRenderWidth,
                        height = astRenderHeight
                      )}
                       |---""".stripMargin
                  )
                }
              }
              withClue(s"[with expected definition]") {
                val actual = actualProcedureDefinitionOpt.get
                if (actual != expected) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Procedure: ${name.fullName}
                       |
                       |Procedure definition not as expected:
                       |
                       |Diff condensed (expected -> actual):
                       |------------------------------------
                       |${DiffPrinter.render(
                        pprint.apply(expected, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        pprint.apply(actual, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        harmonize = harmonizeAstLine,
                        isCondensed = true
                      )}
                       |
                       |Diff full (expected -> actual):
                       |-------------------------------
                       |${DiffPrinter.render(
                        pprint.apply(expected, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        pprint.apply(actual, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        harmonize = harmonizeAstLine,
                        isCondensed = false
                      )}
                       |
                       |---
                       |Actual:
                       |
                       |${pprint.apply(actual, width = astRenderWidth, height = astRenderHeight)}
                       |---
                       |Expected:
                       |
                       |${pprint.apply(expected, width = astRenderWidth, height = astRenderHeight)}
                       |---""".stripMargin
                  )
                }
              }
            }
        }
      }
      this
    }

    override def hasExtractedLocalFunctions(expectedFunctions: (FunctionName, LocalFunctionDefinition)*): TestCase = {
      withClue(s"[has extracted local functions]") {
        val actualFunctionDefinitions = localDefinitionsDirectory.localFunctionDefinitions
        expectedFunctions.foreach {
          case (name, expected) =>
            withClue(s"[${name.fullName}]") {
              val actualFunctionDefinitionOpt = actualFunctionDefinitions.get(name)
              withClue(s"[is present]") {
                if (actualFunctionDefinitionOpt.isEmpty) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Function: ${name.fullName}
                       |
                       |Function is expected to be present in extraction but is not.
                       |
                       |AST Before:
                       |
                       |${pprint.apply(statementBefore, width = astRenderWidth, height = astRenderHeight)}
                       |
                       |AST After:
                       |
                       |${pprint.apply(statementAfter, width = astRenderWidth, height = astRenderHeight)}
                       |---
                       |Actual local function definitions:
                       |
                       |${pprint.apply(
                        localDefinitionsDirectory.localFunctionDefinitions.toSeq.sortBy(_._1.toString),
                        width = astRenderWidth,
                        height = astRenderHeight
                      )}
                       |---
                       |Expected local function definitions:
                       |
                       |${pprint.apply(
                        expectedFunctions.sortBy(_._1.toString),
                        width = astRenderWidth,
                        height = astRenderHeight
                      )}
                       |---""".stripMargin
                  )
                }
              }
              withClue(s"[with expected definition]") {
                val actual = actualFunctionDefinitionOpt.get
                if (actual != expected) {
                  fail(
                    s"""Version: $cypherVersion
                       |Query:
                       |
                       |$query
                       |
                       |Function: ${name.fullName}
                       |
                       |Function definition not as expected:
                       |
                       |Diff condensed (expected -> actual):
                       |------------------------------------
                       |${DiffPrinter.render(
                        pprint.apply(expected, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        pprint.apply(actual, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        harmonize = harmonizeAstLine,
                        isCondensed = true
                      )}
                       |
                       |Diff full (expected -> actual):
                       |-------------------------------
                       |${DiffPrinter.render(
                        pprint.apply(expected, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        pprint.apply(actual, width = astRenderWidth, height = astRenderHeightForDiff).render,
                        harmonize = harmonizeAstLine,
                        isCondensed = false
                      )}
                       |
                       |---
                       |Actual:
                       |
                       |${pprint.apply(actual, width = astRenderWidth, height = astRenderHeight)}
                       |---
                       |Expected:
                       |
                       |${pprint.apply(expected, width = astRenderWidth, height = astRenderHeight)}
                       |---""".stripMargin
                  )
                }
              }
            }
        }
      }
      this
    }

    override def isRewrittenTo(expectedStatement: Statement): TestCase = {
      withClue(s"[is rewritten to]") {
        val actual = normalize(statementAfter)
        val expected = normalize(expectedStatement)
        if (actual != expected) {
          fail(
            s"""Version: CYPHER $cypherVersion
               |Query:
               |------
               |${query}
               |
               |Query was not rewritten as expected
               |
               |Diff condensed (expected -> actual):
               |------------------------------------
               |${DiffPrinter.render(
                pprint.apply(expected, width = astRenderWidth, height = astRenderHeightForDiff).render,
                pprint.apply(actual, width = astRenderWidth, height = astRenderHeightForDiff).render,
                harmonize = harmonizeAstLine,
                isCondensed = true
              )}
               |
               |Diff full (expected -> actual):
               |-------------------------------
               |${DiffPrinter.render(
                pprint.apply(expected, width = astRenderWidth, height = astRenderHeightForDiff).render,
                pprint.apply(actual, width = astRenderWidth, height = astRenderHeightForDiff).render,
                harmonize = harmonizeAstLine,
                isCondensed = false
              )}
               |
               |Expected:
               |---------
               |${pprint.apply(expected, width = astRenderWidth, height = astRenderHeight)}
               |
               |Actual:
               |-------
               |${pprint.apply(actual, width = astRenderWidth, height = astRenderHeight)}
               |""".stripMargin
          )
        }
      }
      this
    }

    override def hasResolvedLocalCallWithBodyContainsUpdates(expected: (String, Boolean)*): TestCase = {
      val actual = statementAfter.folder.treeCollect {
        case call: ResolvedLocalCall => call.procedureName.fullName -> call.bodyContainsUpdates
      }.distinct.sortBy(_._1)
      val expectedSeq = expected.distinct.sortBy(_._1)
      def toString(seq: Seq[(String, Boolean)]): String = seq.map(p => s"  ${p._1} -> ${p._2}").mkString(
        "Seq(" + System.lineSeparator(),
        "," + System.lineSeparator(),
        System.lineSeparator() + ")"
      )
      if (actual != expectedSeq) {
        fail(
          s"""Version: CYPHER $cypherVersion
             |Query:
             |------
             |${query}
             |
             |Did not find expected resolved local calls.
             |
             |Diff (expected -> actual):
             |------------------------------------
             |${DiffPrinter.render(
              toString(expectedSeq),
              toString(actual)
            )}
             |
             |Expected:
             |---------
             |${toString(expectedSeq)}
             |
             |Actual:
             |-------
             |${toString(actual)}
             |""".stripMargin
        )
      }
      this
    }
  }

  case class RanTestCase(
    runsPerVersion: Seq[TestCaseWithVersion]
  ) extends TestCase {

    override def hasExtractedLocalProcedures(expectedProcedures: (
      ProcedureName,
      LocalProcedureDefinition
    )*): TestCase = {
      runsPerVersion.foreach(runPerVersion => {
        withClue(s"[Version: ${runPerVersion.cypherVersion}]") {
          runPerVersion.hasExtractedLocalProcedures(expectedProcedures: _*)
        }
      })
      this
    }

    override def hasExtractedLocalFunctions(expectedFunctions: (FunctionName, LocalFunctionDefinition)*): TestCase = {
      runsPerVersion.foreach(runPerVersion => {
        withClue(s"[Version: ${runPerVersion.cypherVersion}]") {
          runPerVersion.hasExtractedLocalFunctions(expectedFunctions: _*)
        }
      })
      this
    }

    override def isRewrittenTo(expectedStatement: Statement): TestCase = {
      runsPerVersion.foreach(runPerVersion => {
        withClue(s"[Version: ${runPerVersion.cypherVersion}]") {
          runPerVersion.isRewrittenTo(expectedStatement)
        }
      })
      this
    }

    override def hasResolvedLocalCallWithBodyContainsUpdates(expected: (String, Boolean)*): TestCase = {
      runsPerVersion.foreach(runPerVersion => {
        withClue(s"[Version: ${runPerVersion.cypherVersion}]") {
          runPerVersion.hasResolvedLocalCallWithBodyContainsUpdates(expected: _*)
        }
      })
      this
    }
  }

  private def normalize(statement: Statement): Statement = {
    statement.endoRewrite(bottomUp(Rewriter.lift {
      case x => x
    }))
  }

  case class TestName(
    cypherVersionFilter: CypherVersion => Boolean
  ) extends TestCase {

    private def ran(): TestCase = {
      var statementOpt: Option[Statement] = None
      val runsPerVersion = CypherVersion.values().filter(cypherVersionFilter).map { cypherVersion =>
        try {
          val context = initContext(cypherVersion)
          val stateAfterParsing = Parse.transform(initialStateWithQuery(testName), context)
          statementOpt = stateAfterParsing.maybeStatement
          val state = transformers(cypherVersion).transform(stateAfterParsing, context)
          RanTestCasePerVersion(
            cypherVersion,
            testName,
            stateAfterParsing.statement(),
            state.statement(),
            state.localDefinitions()
          )
        } catch {
          case ex: Neo4jException => FailedTestCase(cypherVersion, testName, statementOpt, ex)
        }
      }.toSeq
      RanTestCase(runsPerVersion)
    }

    override def hasExtractedLocalProcedures(procedures: (ProcedureName, LocalProcedureDefinition)*): TestCase = {
      ran().hasExtractedLocalProcedures(procedures: _*)
    }

    override def hasExtractedLocalFunctions(functions: (FunctionName, LocalFunctionDefinition)*): TestCase = {
      ran().hasExtractedLocalFunctions(functions: _*)
    }

    override def isRewrittenTo(ast: Statement): TestCase = {
      ran().isRewrittenTo(ast)
    }

    override def hasResolvedLocalCallWithBodyContainsUpdates(expected: (String, Boolean)*): TestCase = {
      ran().hasResolvedLocalCallWithBodyContainsUpdates(expected: _*)
    }
  }

  def cypher25OnwardsTestName: TestName = TestName(version => version != CypherVersion.Cypher5)

  private def transformers(version: CypherVersion): Transformer[BaseContext, BaseState, BaseState] =
    Parse andThen
      PreparatoryRewriting andThen
      ScopeSurveyor andThen
      ResolveLocalFunctions andThen
      ResolveLocalProceduresStep1 andThen
      TryResolveCallables(ScopedProcedureSignatureResolver.from(resolverMock, QueryLanguage.from(version))) andThen
      ResolveLocalProceduresStep2 andThen
      ScopeSurveyor andThen
      ExtractLocalDefinitions

  private object ResolverMockMarker {
    val NON_LOCAL: String = "nonLocal"
    val UNRESOLVED: String = "unresolved"
    val UPDATE: String = "update"
  }

  private val resolverMock: ProcedureSignatureResolver = new ProcedureSignatureResolver {

    override def procedureSignature(name: ProcedureName, scope: QueryLanguage): ProcedureSignature = {
      if (
        !name.fullName.contains(ResolverMockMarker.NON_LOCAL) || name.fullName.contains(ResolverMockMarker.UNRESOLVED)
      ) {
        throw ProcedureException.noSuchProcedure(new procs.QualifiedName(name.namespace.parts.toArray, name.name))
      } else {
        ProcedureSignature(
          name,
          inputSignature = IndexedSeq.empty[FieldSignature],
          outputSignature = None,
          deprecationInfo = None,
          accessMode = if (name.fullName.contains(ResolverMockMarker.UPDATE)) ProcedureReadWriteAccess
          else ProcedureReadOnlyAccess,
          description = None,
          warning = None,
          // eager = false,
          id = 0
          // systemProcedure = false,
          // allowExpiredCredentials = false,
          // threadSafe = true
        )
      }
    }

    override def functionSignature(name: FunctionName, scope: QueryLanguage): Option[UserFunctionSignature] = None

    override def procedureSignatureVersion: Long = 1
  }

  private def initContext(cypherVersion: CypherVersion) =
    new ErrorCollectingContext(cypherVersion) {
      override def errorMessageProvider: ErrorMessageProvider = messageProvider
    }

  private def initialStateWithQuery(query: String): InitialState =
    InitialState(query, NoPlannerName, new AnonymousVariableNameGenerator)

  def harmonizeAstLine(line: String): String = {
    val y = "\u001B[33m"
    val r = "\u001B[39m"
    Seq(s"${y}Vector${r}(", s"${y}ArraySeq${r}(", s"${y}List${r}(").foldLeft(line) {
      case (updatedLine, needle) =>
        updatedLine.replace(needle, s"${y}Seq${r}(")
    }
  }

  def prettify(astNode: ASTNode): String = astNode match {
    case s: Statement               => prettifier.asString(s)
    case d: LocalCallableDefinition => prettifier.asString(d)
    case f: LocalFieldSignature     => prettifier.asString(f)
    case c: Clause                  => prettifier.asString(SingleQuery(Seq(c))(InputPosition.NONE))
    case g: GroupBy                 => prettifier.asString(g)
    case s: Search                  => prettifier.asString(s)
    case ex: Expression             => prettifier.expr(ex)
    case p: Pattern                 => prettifier.expr.patterns(p)
    case p: PatternPart             => prettifier.expr.patterns(p)
    case p: PatternElement          => prettifier.expr.patterns(p)
    case p: RelationshipPattern     => prettifier.expr.patterns(p)
    case lex: LabelExpression       => prettifier.expr.stringifyLabelExpression(lex)
    case cqb @ ConditionalQueryBranch(Some(_), _) =>
      prettifier.asString(ConditionalQueryWhen(Seq(cqb), None)(InputPosition.NONE))
    case cqb @ ConditionalQueryBranch(None, _) =>
      prettifier.asString(ConditionalQueryWhen(Seq(), Some(cqb))(InputPosition.NONE))
    case x => x.toString
  }
}
