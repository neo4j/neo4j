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
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.neo4j.cypher.internal.frontend.v3_4.ast.{GraphUrl, SingleGraphAs}
import org.neo4j.cypher.internal.frontend.v3_4.{DummyPosition, InputPosition, ast}
import org.parboiled.scala.Rule1

import scala.language.implicitConversions

class GraphsParserTest
  extends ParserAstTest[ast.SingleGraphAs]
  with Graphs
  with Expressions {

  implicit val parser: Rule1[SingleGraphAs] = SingleGraph

  test("SOURCE GRAPH") {
    yields(ast.SourceGraphAs(None))
  }

  test("SOURCE GRAPH AS foo") {
    yields(ast.SourceGraphAs(Some(v("foo"))))
  }

  test("TARGET GRAPH") {
    yields(ast.TargetGraphAs(None))
  }

  test("TARGET GRAPH AS foo") {
    yields(ast.TargetGraphAs(Some(v("foo"))))
  }

  test("GRAPH foo") {
    yields(ast.GraphAs(v("foo"), None))
  }

  test("GRAPH foo AS bar") {
    yields(ast.GraphAs(v("foo"), Some(v("bar"))))
  }

  test("GRAPH AT 'url'") {
    yields(ast.GraphAtAs(url("url"), None))
  }

  test("GRAPH AT 'url' AS foo") {
    yields(ast.GraphAtAs(url("url"), Some(v("foo"))))
  }

  test("GRAPH OF ()") {
    yields(ast.GraphOfAs(nodePattern, None))
  }

  test("GRAPH OF p=(), q=()") {
    yields(ast.GraphOfAs(complexPattern, None))
  }

  test("GRAPH OF () AS foo") {
    yields(ast.GraphOfAs(nodePattern, Some(v("foo"))))
  }

  test("GRAPH OF p=(), q=() AS foo") {
    yields(ast.GraphOfAs(complexPattern, Some(v("foo"))))
  }

  test("GRAPH foo AT 'url'") {
    yields(ast.GraphAtAs(url("url"), Some(v("foo"))))
  }

  test("GRAPH foo OF ()") {
    yields(ast.GraphOfAs(nodePattern, Some(v("foo"))))
  }

  private def url(addr: String): GraphUrl = ast.GraphUrl(Right(ast.StringLiteral(addr)(pos)))(pos)
  private implicit val pos: InputPosition = DummyPosition(-1)
  private val nodePattern = ast.Pattern(List(ast.EveryPath(ast.NodePattern(None, List(), None)(pos))))(pos)
  private val complexPattern = ast.Pattern(List(
    ast.NamedPatternPart(v("p"), ast.EveryPath(ast.NodePattern(None, List(), None)(pos)))(pos),
    ast.NamedPatternPart(v("q"), ast.EveryPath(ast.NodePattern(None, List(), None)(pos)))(pos)
  ))(pos)
  private implicit def v(name: String): ast.Variable = ast.Variable(name)(pos)
}
