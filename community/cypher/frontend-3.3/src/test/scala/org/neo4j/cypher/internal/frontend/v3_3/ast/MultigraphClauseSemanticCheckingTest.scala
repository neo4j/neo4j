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

import org.neo4j.cypher.internal.frontend.v3_3.parser.ParserTest
import org.neo4j.cypher.internal.frontend.v3_3.phases._
import org.neo4j.cypher.internal.frontend.v3_3.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.frontend.v3_3.{CypherException, InputPosition, PlannerName, SemanticCheckResult, SemanticFeature, SemanticState, SemanticTable, ast, parser}
import org.parboiled.scala.Rule1

class MultigraphClauseSemanticCheckingTest
  extends ParserTest[ast.Statement, SemanticCheckResult]
  with parser.Statement
  with AstConstructionTestSupport {

  implicit val parser: Rule1[Query] = Query

// TODO: Temporarily disabled while we figure out how to do this right
//  test("CREATE GRAPH introduces new graphs") {
//    parsing("CREATE GRAPH foo AT 'url'") shouldVerify { case SemanticCheckResult(state, errors) =>
//      errors shouldBe(empty)
//      graphScopes(state) should equal("GraphScope(source >> target, foo) <= CreateRegularGraph(false,Variable(foo),None,GraphUrl(Right(StringLiteral(url))))")
//      scopeTreeGraphs(state) should equal("GraphScopeTree(source >> target: GraphScopeTree(- >> -, foo: ))")
//    }
//
//    parsing("CREATE >> GRAPH foo AT 'url'") shouldVerify { case SemanticCheckResult(state, errors) =>
//      errors shouldBe(empty)
//      graphScopes(state) should equal("GraphScope(source >> foo, target) <= CreateNewTargetGraph(false,Variable(foo),None,GraphUrl(Right(StringLiteral(url))))")
//      scopeTreeGraphs(state) should equal("GraphScopeTree(source >> target: GraphScopeTree(- >> foo: ))")
//    }
//
//    parsing("CREATE GRAPH foo AT 'url' >>") shouldVerify { case SemanticCheckResult(state, errors) =>
//      errors shouldBe(empty)
//      graphScopes(state) should equal("GraphScope(foo >> foo, source, target) <= CreateNewSourceGraph(false,Variable(foo),None,GraphUrl(Right(StringLiteral(url))))")
//      scopeTreeGraphs(state) should equal("GraphScopeTree(source >> target: GraphScopeTree(foo >> foo: ))")
//    }
//  }
//
  test("WITH GRAPHS controls which graphs are in scope") {
    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |RETURN a""".stripMargin) shouldVerify {
      case SemanticCheckResult(state, errors) =>
        errors shouldBe empty

        contextsByPosition(state) should equal(strip(
          """
            |// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),Some(GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)))))))),None,None,None,None)
            |source >> target
            |// Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set())
            |--
            |// End
            """))
        fullScopeTree(state) should equal(strip(
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
            |}"""))
    }


    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH a GRAPH source
        |RETURN a""".stripMargin) shouldVerify {
      case SemanticCheckResult(state, errors) =>
        errors shouldBe empty
        contextsByPosition(state) should equal(strip(
          """
            |// Start
            |--
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(SignedDecimalIntegerLiteral(1),Variable(a)))),Some(GraphReturnItems(false,List(NewContextGraphs(GraphAtAs(GraphUrl(Right(StringLiteral(src))),Some(Variable(source))),Some(GraphAtAs(GraphUrl(Right(StringLiteral(tgt))),Some(Variable(target)))))))),None,None,None,None)
            |source >> target
            |// With(false,ReturnItems(false,Vector(AliasedReturnItem(Variable(a),Variable(a)))),Some(GraphReturnItems(false,List(ReturnedGraph(GraphAs(Variable(source),Some(Variable(source))))))),None,None,None,None)
            |source >> source
            |// Return(false,ReturnItems(false,List(AliasedReturnItem(Variable(a),Variable(a)))),None,None,None,None,Set())
            |--
            |// End
          """))
        fullScopeTree(state) should equal(strip(
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
            |}"""))
    }

    parsing(
      """WITH 1 AS a GRAPH source AT 'src' >> GRAPH target AT 'tgt'
        |WITH a GRAPH source
        |WITH a GRAPH target
        |RETURN a""".stripMargin) shouldVerify {
      case SemanticCheckResult(state, errors) =>
        errors.map(_.msg).toSet should equal(Set("Variable `target` not defined"))
    }
  }

  private def strip(text: String) =
    org.neo4j.cypher.internal.frontend.v3_3.helpers.StringHelper.RichString(text.trim.stripMargin).fixNewLines

  private def fullScopeTree(state: SemanticState): String = state.scopeTree.toStringWithoutId.trim

  private def contextsByPosition(state: SemanticState): String = {
    val astNodes = state.recordedContextGraphs.keySet ++ state.recordedScopes.keySet
    val keys = astNodes.toSeq.sortBy(x => x.position.offset -> x.toString)
    val values = keys.map(ast => s"${state.recordedContextGraphs.getOrElse(ast, "--")}\n// $ast")
    val finalContext = state.currentScope.contextGraphs.map(_.toString).getOrElse("--")
    s"// Start\n${values.mkString("\n")}\n$finalContext\n// End"
  }

  override def convert(astNode: ast.Statement): SemanticCheckResult = {
    val rewritten = PreparatoryRewriting.transform(TestState(astNode), TestContext).statement()
    val initialState = SemanticState.withFeatures(SemanticFeature.MultipleGraphs)
    rewritten.semanticCheck(initialState)
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
