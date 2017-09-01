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
package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.neo4j.cypher.internal.frontend.v3_3.ast.{AstConstructionTestSupport, Clause}
import org.parboiled.scala.Rule1

import scala.language.implicitConversions

class MultipleGraphClausesParsingTest
  extends ParserAstTest[ast.Clause]
  with Query
  with Expressions
  with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  test("CREATE GRAPH foo AT 'url'") {
    yields(ast.CreateRegularGraph(snapshot = false, varFor("foo"), None, url("url")))
  }

  test("CREATE SNAPSHOT GRAPH foo AT 'url'") {
    yields(ast.CreateRegularGraph(snapshot = true, varFor("foo"), None, url("url")))
  }

  test("CREATE GRAPH foo OF () AT 'url'") {
    yields(ast.CreateRegularGraph(snapshot = false, varFor("foo"), Some(nodePattern), url("url")))
  }

  test("CREATE SNAPSHOT GRAPH foo OF () AT 'url'") {
    yields(ast.CreateRegularGraph(snapshot = true, varFor("foo"), Some(nodePattern), url("url")))
  }

  test("CREATE GRAPH foo OF p=(), q=() AT 'url'") {
    yields(ast.CreateRegularGraph(snapshot = false, varFor("foo"), Some(complexPattern), url("url")))
  }

  test("CREATE SNAPSHOT GRAPH foo OF p=(), q=() AT 'url'") {
    yields(ast.CreateRegularGraph(snapshot = true, varFor("foo"), Some(complexPattern), url("url")))
  }

  test("CREATE >> GRAPH foo AT 'url'") {
    yields(ast.CreateNewTargetGraph(snapshot = false, varFor("foo"), None, url("url")))
  }

  test("CREATE >> SNAPSHOT GRAPH foo AT 'url'") {
    yields(ast.CreateNewTargetGraph(snapshot = true, varFor("foo"), None, url("url")))
  }

  test("CREATE GRAPH foo AT 'url' >>") {
    yields(ast.CreateNewSourceGraph(snapshot = false, varFor("foo"), None, url("url")))
  }

  test("CREATE SNAPSHOT GRAPH foo AT 'url' >>") {
    yields(ast.CreateNewSourceGraph(snapshot = true, varFor("foo"), None, url("url")))
  }

  test("PERSIST GRAPH foo TO 'url'") {
    yields(ast.Persist(snapshot = false, graph("foo"), url("url")))
  }

  test("PERSIST SOURCE GRAPH TO 'url'") {
    yields(ast.Persist(snapshot = false, ast.SourceGraphAs(None)(pos), url("url")))
  }

  test("PERSIST TARGET GRAPH TO 'url'") {
    yields(ast.Persist(snapshot = false, ast.TargetGraphAs(None)(pos), url("url")))
  }

  test("PERSIST SNAPSHOT SOURCE GRAPH TO 'url'") {
    yields(ast.Persist(snapshot = true, ast.SourceGraphAs(None)(pos), url("url")))
  }

  test("PERSIST SNAPSHOT TARGET GRAPH TO 'url'") {
    yields(ast.Persist(snapshot = true, ast.TargetGraphAs(None)(pos), url("url")))
  }

  test("PERSIST SNAPSHOT GRAPH foo TO 'url'") {
    yields(ast.Persist(snapshot = true, graph("foo"), url("url")))
  }

  test("RELOCATE GRAPH foo TO 'url'") {
    yields(ast.Relocate(snapshot = false, graph("foo"), url("url")))
  }

  test("RELOCATE SNAPSHOT GRAPH foo TO 'url'") {
    yields(ast.Relocate(snapshot = true, graph("foo"), url("url")))
  }

  test("DELETE GRAPHS foo, bar") {
    yields(ast.DeleteGraphs(Seq(varFor("foo"), varFor("bar"))))
  }

  test("DELETE GRAPH foo") {
    yields(ast.DeleteGraphs(Seq(varFor("foo"))))
  }

  test("DELETE GRAPH foo, GRAPH bar") {
    yields(ast.DeleteGraphs(Seq(varFor("foo"), varFor("bar"))))
  }

  private val nodePattern = ast.Pattern(List(ast.EveryPath(ast.NodePattern(None, List(), None)(pos))))(pos)

  private val complexPattern = ast.Pattern(List(
    ast.NamedPatternPart(varFor("p"), ast.EveryPath(ast.NodePattern(None, List(), None)(pos)))(pos),
    ast.NamedPatternPart(varFor("q"), ast.EveryPath(ast.NodePattern(None, List(), None)(pos)))(pos)
  ))(pos)
}
