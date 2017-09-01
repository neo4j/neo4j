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
import org.neo4j.cypher.internal.frontend.v3_3.ast.{AstConstructionTestSupport, Clause, GraphReturnItems, PassAllGraphReturnItems}
import org.parboiled.scala.Rule1

class ProjectionClauseParserTest
  extends ParserAstTest[ast.Clause]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  test("WITH *") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), PassAllGraphReturnItems(pos)))
  }

  test("WITH 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), PassAllGraphReturnItems(pos)))
  }

  test("WITH *, 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), PassAllGraphReturnItems(pos)))
  }

  test("WITH ") {
    failsToParse
  }

  test("WITH GRAPH *") {
    failsToParse
  }

  test("WITH * GRAPH AT 'url' AS foo >>") {
    yields(ast.With(
      ast.ReturnItems(includeExisting = true, Seq.empty)(pos),
      ast.GraphReturnItems(includeExisting = false, Seq(ast.NewContextGraphs(graphAt("foo", "url"))(pos)))(pos))
    )
  }

  test("WITH a GRAPHS foo, >> GRAPH AT 'url2' AS bar") {
    yields(ast.With(
      ast.ReturnItems(includeExisting = false, Seq(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos),
      ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graph("foo"))(pos), ast.NewTargetGraph(graphAt("bar", "url2"))(pos)))(pos)
    ))
  }

  test("WITH 1 AS a GRAPH AT 'url' AS foo, GRAPH AT 'url2' AS bar") {
    yields(ast.With(
      ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos),
      ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graphAt("foo", "url"))(pos), ast.ReturnedGraph(graphAt("bar", "url2"))(pos)))(pos)
    ))
  }

  ignore("WITH GRAPHS") {
    failsToParse
  }

  ignore("WITH GRAPH") {
    failsToParse
  }

  test("WITH GRAPHS a, b") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = false, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.With(graphs))
  }

  test("WITH GRAPHS *, a, b") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.With(graphs))
  }

  test("WITH 1 AS a GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq.empty)(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), graphs))
  }

  test("WITH * GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq.empty)(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), graphs))
  }

  test("WITH * GRAPH foo AT 'url'") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graphAt("foo", "url"))(pos)))(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), graphs))
  }

  test("WITH * GRAPH foo AT 'url' ORDER BY 2 LIMIT 1") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graphAt("foo", "url"))(pos)))(pos)
    yields(ast.With(distinct = false, ast.ReturnItems(includeExisting = true, Seq.empty)(pos), graphs, Some(ast.OrderBy(Seq(ast.AscSortItem(literalInt(2))(pos)))(pos)), None, Some(ast.Limit(literalInt(1))(pos)), None))
  }

  test("RETURN *") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), None))
  }

  test("RETURN 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), None))
  }

  test("RETURN *, 1 AS a") {
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), None))
  }

  test("RETURN ") {
    failsToParse
  }

  test("RETURN GRAPH *") {
    failsToParse
  }

  ignore("RETURN GRAPHS") {
    failsToParse
  }

  ignore("RETURN GRAPH") {
    failsToParse
  }

  test("RETURN GRAPHS a, b") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = false, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.Return(graphs))
  }

  test("RETURN GRAPHS *, a, b") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.Return(graphs))
  }

  test("RETURN 1 AS a GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq.empty)(pos)
    yields(ast.Return(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), Some(graphs)))
  }

  test("RETURN * GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq.empty)(pos)
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), Some(graphs)))
  }

  test("RETURN * GRAPH foo AT 'url' ORDER BY 2 LIMIT 1") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graphAt("foo", "url"))(pos)))(pos)
    yields(ast.Return(distinct = false, ast.ReturnItems(includeExisting = true, Seq.empty)(pos), Some(graphs), Some(ast.OrderBy(Seq(ast.AscSortItem(literalInt(2))(pos)))(pos)), None, Some(ast.Limit(literalInt(1))(pos))))
  }

  test("FROM GRAPH foo AT 'url'") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq(ast.NewContextGraphs(graphAt("foo", "url"), Some(graphAt("foo", "url")))(pos)))(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), graphs))
  }

  test("INTO GRAPH foo AT 'url'") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(includeExisting = true, Seq(ast.NewTargetGraph(graphAt("foo", "url"))(pos)))(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), graphs))
  }

}
