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

import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.normalizeGraphReturnItems
import org.neo4j.cypher.internal.frontend.v3_3.parser.ParserTest
import org.neo4j.cypher.internal.frontend.v3_3.{GraphScope, Scope, SemanticCheckResult, SemanticFeature, SemanticState, parser}

import scala.compat.Platform

class MultigraphClausesTest
  extends ParserTest[Query, SemanticCheckResult]
  with parser.Query
  with AstConstructionTestSupport {

  implicit val parser = Query

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
//  test("WITH GRAPHS controls which graphs are in scope") {
//    parsing("MATCH () WITH * GRAPHS source RETURN *") shouldVerify { case SemanticCheckResult(state, errors) =>
//      errors shouldBe(empty)
//      graphScopes(state) should equal(
//      """""".stripMargin)
//      println(graphScopes(state))
//      println(scopeTreeGraphs(state))
//    }
//  }

  private def graphScopes(state: SemanticState): String = {
    val recorded: Seq[(ASTNode, GraphScope)] = state.recordedGraphScopes.toSeq.sortBy(_._1.position.offset)
    val formatted = recorded.map(e => s"${e._2} <= ${e._1}")
    formatted.mkString(Platform.EOL)
  }

  private def scopeTreeGraphs(state: SemanticState): String =
    scopeTreeGraphs(state.scopeTree).toString

  private def scopeTreeGraphs(scope: Scope): ScopeTreeGraphs = {
    val graphs = scope.selectSymbolNames(_.graph)
    val source = scope.setSourceGraph
    val target = scope.setTargetGraph
    val childs = scope.children.map(scopeTreeGraphs)
    ScopeTreeGraphs(source -> target, graphs, childs)
  }


  override def convert(astNode: Query): SemanticCheckResult = {
    // Multigraph Cypher expects that there is always a source and a target graph until RETURN
    // (where they may actually be dropped)
    //
    // We manually feed a fake source and target here to test semantic checking
    //
    val rewritten = astNode.endoRewrite(normalizeGraphReturnItems)
    val state0 = SemanticState.withFeatures(SemanticFeature.MultipleGraphs)
    val Right(state1) = state0.declareGraphVariable(varFor("source"))
    val Right(state2) = state1.declareGraphVariable(varFor("target"))
    val Right(state3) = state2.updateSetContextGraphs(Some(varFor("source")), Some(varFor("target")))
    rewritten.semanticCheck(state3)
  }

  private final case class ScopeTreeGraphs(context: (Option[String], Option[String]), graphs: Set[String], childs: Seq[ScopeTreeGraphs]) {
    override def toString = {
      val source = context._1.getOrElse("-")
      val target = context._2.getOrElse("-")
      val head = s"$source >> $target"
      val elts = Set(head) ++ graphs.toSeq.sorted.map(_.toString).filter(_ != source).filter(_ != target)
      s"GraphScopeTree(${elts.mkString(", ")}: ${childs.toList.mkString("|")})"
    }
  }
}
