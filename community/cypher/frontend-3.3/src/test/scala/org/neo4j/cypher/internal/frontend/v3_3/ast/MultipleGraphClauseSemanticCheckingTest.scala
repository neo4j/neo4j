/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.helpers.StringHelper
import org.neo4j.cypher.internal.frontend.v3_3.parser.ParserTest
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, InputPosition, PlannerName, SemanticCheckResult, SemanticErrorDef, SemanticFeature, SemanticState, SemanticTable, ast, parser}
import org.parboiled.scala.Rule1

class MultipleGraphClauseSemanticCheckingTest
  extends ParserTest[ast.Statement, SemanticCheckResult]
  with parser.Statement
  with AstConstructionTestSupport {

  // INFO: Use result.dumpAndExit to debug these tests

  implicit val parser: Rule1[Query] = Query

  test("GRAPHS * keeps existing graphs in scope (1)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN * GRAPHS *
     """.stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        verify(result.formattedContexts) shouldEqualFixNewlines
          """// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |}"""
    }
  }

  test("GRAPHS * keeps existing graphs in scope (2)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN GRAPHS *
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        verify(result.formattedContexts) shouldEqualFixNewlines
          """// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// Return(false,DiscardCardinality(),Some(GraphReturnItems(true,List())),None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |}"""
    }
  }

  test("WITH * passes on whatever graphs are in scope") {
    parsing(
      """WITH GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH 1 AS a
        |RETURN * GRAPHS *
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        verify(result.formattedContexts) shouldEqualFixNewlines
          """// Start
            |--
            |// With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(true,List()),None,None,None,None)
            |source >> target
            |// Return(false,ReturnItems(true,List()),Some(GraphReturnItems(true,List())),None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |  { /* source >> target */
            |    a: 62
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |  {
            |    a: 62
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (1)") {
    parsing(
      """WITH GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN GRAPH foo AT 'url', GRAPH bar AT 'url'""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        verify(result.formattedContexts) shouldEqualFixNewlines
          """// Start
            |--
            |// With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// Return(false,DiscardCardinality(),Some(GraphReturnItems(false,List(ReturnedGraph(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(foo)))), ReturnedGraph(GraphAtAs(GraphUrl(Right(StringLiteral(url))),Some(Variable(bar))))))),None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    GRAPH source: 11
            |    GRAPH target: 36
            |  }
            |  {
            |    GRAPH bar: 85
            |    GRAPH foo: 65
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (2)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        verify(result.formattedContexts) shouldEqualFixNewlines
          """
            |// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set())
            |--
            |// End
            """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10 66
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 66 67
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (3)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH a GRAPH source
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        verify(result.formattedContexts) shouldEqualFixNewlines
          """// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(source),Some(Variable(source)))))),None,None,None,None)
            |source >> source
            |// Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10 64
            |    GRAPH source: 18 72
            |    GRAPH target: 43
            |  }
            |  { /* source >> source */
            |    a: 10 64 65 86
            |    GRAPH source: 72
            |  }
            |  {
            |    a: 10 64 65 86 87
            |  }
            |}"""
    }
  }

  test("WITH GRAPHS controls which graphs are in scope (4)") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH a GRAPH source
        |WITH a GRAPH target
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errorMessages should equal(Set("Variable `target` not defined"))
    }
  }

  test("Intermediary clauses don't lose graphs") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |MATCH (b)
        |WITH a
        |MATCH (c)
        |RETURN a, c""".stripMargin) shouldVerify { result: SemanticCheckResult =>
        result.errors shouldBe empty
        verify(result.formattedContexts) shouldEqualFixNewlines
          """
            |// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None)
            |source >> target
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),GraphReturnItems(true,List()),None,None,None,None)
            |source >> target
            |// Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(c)),List(),None)))),List(),None)
            |source >> target
            |// Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)), AliasedReturnItem(Variable(c),Variable(c)))),None,None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10 74
            |    b: 66
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  { /* source >> target */
            |    a: 10 74 75 93
            |    c: 83 96
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 74 75 93 94
            |    c: 83 96 97
            |  }
            |}"""
    }
  }

  test("FROM introduces new source and target graphs") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |FROM GRAPH new AT 'new'
        |MATCH (b)
        |RETURN a GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>
      verify(result.formattedContexts) shouldEqualFixNewlines
          """// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(new))),Some(Variable(new))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(new))),Some(Variable(new))))))),None,None,None,None)
            |new >> new
            |// Match(false,Pattern(List(EveryPath(NodePattern(Some(Variable(b)),List(),None)))),List(),None)
            |new >> new
            |// Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),Some(GraphReturnItems(true,List())),None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  { /* new >> new */
            |    a: 10 100
            |    b: 90
            |    GRAPH new: 70
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 100 101
            |    GRAPH new: 70
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |}"""
    }
  }

  test("INTO introduces new target graph") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |INTO GRAPH new AT 'new'
        |RETURN a""".stripMargin) shouldVerify { result: SemanticCheckResult =>

        verify(result.formattedContexts) shouldEqualFixNewlines
          """// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target))))))),None,None,None,None)
            |source >> target
            |// With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List(NewTargetGraph(GraphAtAs(GraphUrl(Right(StringLiteral(new))),Some(Variable(new)))))),None,None,None,None)
            |new >> new
            |// Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set())
            |--
            |// End
          """
        verify(result.formattedScopes) shouldEqualFixNewlines
          """{
            |  {
            |  }
            |  { /* source >> target */
            |    a: 10
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  { /* new >> new */
            |    a: 10 90
            |    GRAPH new: 70
            |    GRAPH source: 18
            |    GRAPH target: 43
            |  }
            |  {
            |    a: 10 90 91
            |  }
            |}"""
    }
  }

  test("graph name conflict") {
    parsing(
      """WITH GRAPH AT 'url' AS foo >> GRAPH AT 'url' AS bar
        |WITH GRAPHS >> foo AS foo, bar AS foo
        |RETURN 1
      """.stripMargin) shouldVerify { result: SemanticCheckResult =>
      result.errorMessages should equal(Set("Multiple result graphs with the same name are not supported"))
    }
  }

  test("with is allowed to refer to missing graphs at the start of query") {
    parsing(
      """WITH GRAPHS foo >> bar
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      verify(result.formattedContexts) shouldEqualFixNewlines
        """// Start
          |--
          |// With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo))),Some(GraphAs(Variable(bar),Some(Variable(bar))))))),None,None,None,None)
          |foo >> bar
          |// Return(false,ReturnItems(false,List(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(1)))),None,None,None,None,Set())
          |--
          |// End"""
      verify(result.formattedScopes) shouldEqualFixNewlines
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  {
          |    1: 31
          |  }
          |}"""
    }
  }

  test("with is not allowed to refer to missing graphs anywhere else") {
    parsing(
      """WITH GRAPHS foo >> bar
        |WITH GRAPHS zig >> zag
        |RETURN 1""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.errorMessages should equal(Set(
        "Variable `zig` not defined",
        "Variable `zag` not defined"
      ))
    }
  }
  
  test("graphs are not lost across update statements") {
    parsing(
      """WITH GRAPHS foo >> bar
        |CREATE (a)
        |MERGE (b {name: a.name})
        |CREATE (b)-->(b)
        |WITH *
        |DELETE (a)
        |RETURN GRAPHS *""".stripMargin) shouldVerify { result: SemanticCheckResult =>

      result.formattedContexts should equal(strip(
        """// Start
          |--
          |// With(false,DiscardCardinality(),GraphReturnItems(false,List(NewContextGraphs(GraphAs(Variable(foo),Some(Variable(foo))),Some(GraphAs(Variable(bar),Some(Variable(bar))))))),None,None,None,None)
          |foo >> bar
          |// With(false,ReturnItems(true,Vector()),GraphReturnItems(true,List()),None,None,None,None)
          |foo >> bar
          |// Return(false,DiscardCardinality(),Some(GraphReturnItems(true,List())),None,None,None,Set())
          |--
          |// End"""))
      result.formattedScopes should equal(strip(
        """{
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    a: 31 50
          |    b: 41 67 73
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  { /* foo >> bar */
          |    a: 31 50 91
          |    b: 41 67 73
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |  {
          |    GRAPH bar: 19
          |    GRAPH foo: 12
          |  }
          |}"""))
    }
  }

  implicit class verify(actual: String) {
    def shouldEqualFixNewlines(expected: String): Unit = {
      StringHelper.RichString(actual.trim.stripMargin).fixNewLines should equal(StringHelper.RichString(expected.trim.stripMargin).fixNewLines)
    }
  }

  private def fullScopeTree(state: SemanticState): String = state.scopeTree.toStringWithoutId.trim

  import scala.compat.Platform.EOL

  private def contextsByPosition(state: SemanticState): String = {
    val astNodes = state.recordedContextGraphs.keySet ++ state.recordedScopes.keySet
    val keys = astNodes.toSeq.sortBy(x => x.position.offset -> x.toString)
    val values = keys.map(ast => s"${state.recordedContextGraphs.getOrElse(ast, "--")}$EOL// $ast")
    val finalContext = state.currentScope.contextGraphs.map(_.toString).getOrElse("--")
    s"// Start$EOL${values.mkString(EOL)}$EOL$finalContext$EOL// End"
  }

  override def convert(astNode: ast.Statement): SemanticCheckResult = {
    val rewritten = PreparatoryRewriting.transform(TestState(astNode), TestContext).statement()
    val initialState = SemanticState.clean.withFeatures(SemanticFeature.MultipleGraphs, SemanticFeature.WithInitialQuerySignature)
    rewritten.semanticCheck(initialState)
  }

  implicit final class RichSemanticCheckResult(val result: SemanticCheckResult) {
    def state: SemanticState = result.state
    def errors: Seq[SemanticErrorDef] = result.errors
    def errorMessages: Set[String] = errors.map(_.msg).toSet

    def formattedContexts: String = contextsByPosition(state)
    def formattedScopes: String = fullScopeTree(state)

    // Useful for working on these tests

    def dumpAndExit(): Unit = {
      dump()
      System.exit(1)
    }

    def dumpAndFail(): Unit = {
      dump()
      fail()
    }

    def dumpAndFail(msg: String): Unit = {
      dump()
      fail(msg)
    }

    def dump(): Unit = {
      println("\n// *** ERRORS")
      println(errorMessages.mkString("\n"))
      println("\n// *** CONTEXTS")
      println(contextsByPosition(state))
      println("\n// *** SCOPES")
      println(fullScopeTree(state))
    }
  }

  //noinspection TypeAnnotation
  case class TestState(override val statement: ast.Statement) extends BaseState {
    override def queryText: String = statement.toString
    override def startPosition: None.type = None
    override object plannerName extends PlannerName {
      override def name: String = "Test"
      override def toTextOutput: String = name
    }
    override def maybeStatement = None
    override def maybeSemantics = None
    override def maybeExtractedParams = None
    override def maybeSemanticTable = None
    override def accumulatedConditions = Set.empty
    override def withStatement(s: Statement) = copy(s)
    override def withSemanticTable(s: SemanticTable) = ???
    override def withSemanticState(s: SemanticState) = ???
    override def withParams(p: Map[String, Any]) = ???
  }

  //noinspection TypeAnnotation
  object TestContext extends BaseContext {
    override def tracer = CompilationPhaseTracer.NO_TRACING
    override def notificationLogger = mock[InternalNotificationLogger]
    override object exceptionCreator extends ((String, InputPosition) => CypherException) {
      override def apply(msg: String, pos: InputPosition): CypherException = throw new CypherException() {
        override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T =
          mapper.internalException(msg, this)
      }
    }
    override def monitors = mock[Monitors]
    override def errorHandler = _ => ()
  }
}
