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

class MultipleGraphsParserTest
  extends ParserAstTest[ast.Query]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Query] = Query

  test("WITH *") {
    yields(ast.Query(None, ast.SingleQuery(Seq(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), PassAllGraphReturnItems(pos))(pos)))(pos)))
  }

  test("WITH 1 AS a WITH a GRAPHS foo, >> GRAPH AT 'url2' AS bar RETURN GRAPHS bar, foo") {
    yields(ast.Query(None, ast.SingleQuery(Seq(
      ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos),
        PassAllGraphReturnItems(pos)
      )(pos), ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos),
        ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graph("foo"))(pos), ast.NewTargetGraph(graphAt("bar", "url2"))(pos)))(pos)
      )(pos), ast.Return(
        ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graph("bar"))(pos), ast.ReturnedGraph(graph("foo"))(pos)))(pos)
      )(pos)
    ))(pos)))
  }

  test("WITH 1 AS a WITH a GRAPH AT 'url' AS foo >> foo WITH a GRAPHS foo, >> GRAPH AT 'url2' AS bar RETURN GRAPHS bar, foo") {
    yields(ast.Query(None, ast.SingleQuery(Seq(
      ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos),
        PassAllGraphReturnItems(pos)
      )(pos), ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos),
        ast.GraphReturnItems(includeExisting = false, Seq(ast.NewContextGraphs(graphAt("foo", "url"), Some(graph("foo")))(pos)))(pos)
      )(pos), ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos),
        ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graph("foo"))(pos), ast.NewTargetGraph(graphAt("bar", "url2"))(pos)))(pos)
      )(pos), ast.Return(
        ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graph("bar"))(pos), ast.ReturnedGraph(graph("foo"))(pos)))(pos)
      )(pos)
    ))(pos)))
  }

  // TODO: Doesn't work because the `>>` operator thinks `WITH a` is a new graph named `WITH` and expects `a` to be the start of `AT` or `AS`, and fails when the subsequent whitespace is not `t` or `s`.
  // Using explicit 2nd argument to >> works (see above), or using `FROM`
  ignore("WITH 1 AS a WITH a GRAPH AT 'url' AS foo >> WITH a GRAPHS foo, >> GRAPH AT 'url2' AS bar RETURN GRAPHS bar, foo") {
    yields(ast.Query(None, ast.SingleQuery(Seq(
      ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos),
        PassAllGraphReturnItems(pos)
      )(pos), ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos),
        ast.GraphReturnItems(includeExisting = false, Seq(ast.NewContextGraphs(graphAt("foo", "url"))(pos)))(pos)
      )(pos), ast.With(
        ast.ReturnItems(includeExisting = false, Seq(ast.UnaliasedReturnItem(varFor("a"), "a")(pos)))(pos),
        ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graph("foo"))(pos), ast.NewTargetGraph(graphAt("bar", "url2"))(pos)))(pos)
      )(pos), ast.Return(
        ast.GraphReturnItems(includeExisting = false, Seq(ast.ReturnedGraph(graph("bar"))(pos), ast.ReturnedGraph(graph("foo"))(pos)))(pos)
      )(pos)
    ))(pos)))
  }

}
