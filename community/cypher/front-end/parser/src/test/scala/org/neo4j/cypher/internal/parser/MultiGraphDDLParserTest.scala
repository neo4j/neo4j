/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.expressions

class MultiGraphDDLParserTest extends AdministrationCommandParserTestBase {

  private val singleQuery = ast.SingleQuery(Seq(ast.ConstructGraph()(pos)))(pos)
  private val returnGraph: ast.ReturnGraph = ast.ReturnGraph(None)(pos)

  test("CATALOG CREATE GRAPH foo.bar { RETURN GRAPH }") {
    val query = ast.SingleQuery(Seq(returnGraph))(pos)
    val graphName = ast.CatalogName("foo", List("bar"))

    yields(ast.CreateGraph(graphName, query))
  }

  test("CATALOG CREATE GRAPH `*` { RETURN GRAPH }") {
    val query = ast.SingleQuery(Seq(returnGraph))(pos)
    val graphName = ast.CatalogName("*", List())

    yields(ast.CreateGraph(graphName, query))
  }

  test("CATALOG CREATE GRAPH * { RETURN GRAPH }") {
    failsToParse
  }

  test("CATALOG CREATE GRAPH foo.bar { FROM GRAPH foo RETURN GRAPH UNION ALL FROM GRAPH bar RETURN GRAPH }") {
    val useGraph1 = ast.FromGraph(expressions.Variable("foo")(pos))(pos)
    val useGraph2 = ast.FromGraph(expressions.Variable("bar")(pos))(pos)
    val lhs = ast.SingleQuery(Seq(useGraph1, returnGraph))(pos)
    val rhs = ast.SingleQuery(Seq(useGraph2, returnGraph))(pos)
    val union = ast.UnionAll(lhs, rhs)(pos)
    val graphName = ast.CatalogName("foo", List("bar"))

    yields(ast.CreateGraph(graphName, union))
  }

  test("CATALOG CREATE GRAPH foo.bar { FROM GRAPH foo RETURN GRAPH UNION FROM GRAPH bar RETURN GRAPH }") {
    val useGraph1 = ast.FromGraph(expressions.Variable("foo")(pos))(pos)
    val useGraph2 = ast.FromGraph(expressions.Variable("bar")(pos))(pos)
    val lhs = ast.SingleQuery(Seq(useGraph1, returnGraph))(pos)
    val rhs = ast.SingleQuery(Seq(useGraph2, returnGraph))(pos)
    val union = ast.UnionDistinct(lhs, rhs)(pos)
    val graphName = ast.CatalogName("foo", List("bar"))

    yields(ast.CreateGraph(graphName, union))
  }

  test("CATALOG CREATE GRAPH foo.bar { CONSTRUCT }") {
    val graphName = ast.CatalogName("foo", List("bar"))

    yields(ast.CreateGraph(graphName, singleQuery))
  }

  // missing graph name
  test("CATALOG CREATE GRAPH { RETURN GRAPH }") {
    failsToParse
  }

  test("CATALOG CREATE GRAPH `foo.bar.baz.baz` { CONSTRUCT }"){
    yields(ast.CreateGraph(
      new ast.CatalogName(List("foo.bar.baz.baz")),
      singleQuery
    ))
  }

  test("CATALOG CREATE GRAPH `foo.bar`.baz { CONSTRUCT }"){
    yields(ast.CreateGraph(
      new ast.CatalogName(List("foo.bar", "baz")),
      singleQuery
    ))
  }

  test("CATALOG CREATE GRAPH foo.`bar.baz` { CONSTRUCT }"){
    yields(ast.CreateGraph(
      new ast.CatalogName(List("foo", "bar.baz")),
      singleQuery
    ))
  }

  test("CATALOG CREATE GRAPH `foo.bar`.`baz.baz` { CONSTRUCT }"){
    yields(ast.CreateGraph(
      new ast.CatalogName(List("foo.bar", "baz.baz")),
      singleQuery
    ))
  }

  // missing graph name
  test("CATALOG DROP GRAPH union") {
    val graphName = ast.CatalogName("union")

    yields(ast.DropGraph(graphName))
  }

  // missing graph name; doesn't fail because it's a valid query if GRAPH is a variable
  ignore("CATALOG DROP GRAPH") {
    failsToParse
  }
}
