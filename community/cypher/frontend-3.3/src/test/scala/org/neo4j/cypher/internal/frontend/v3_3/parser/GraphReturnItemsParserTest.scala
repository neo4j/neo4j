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
import org.neo4j.cypher.internal.frontend.v3_3.ast.AstConstructionTestSupport

import scala.language.implicitConversions

class GraphReturnItemsParserTest
  extends ParserAstTest[ast.GraphReturnItems]
  with Graphs
  with Expressions
  with AstConstructionTestSupport {

  implicit val parser = GraphReturnItems

  test("must start with GRAPH or GRAPHS") {
    assertFails("foo, GRAPH a")
  }

  test("GRAPH foo >>") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.NewContextGraphs(graph("foo"))(pos)
    )))
  }

  test(">> GRAPH bar") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.NewTargetGraph(graph("bar"))(pos)
    )))
  }

  test("GRAPH foo >> GRAPH bar") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.NewContextGraphs(graph("foo"), Some(graph("bar")))(pos)
    )))
  }

  test("GRAPH a, GRAPH foo >> GRAPH bar, GRAPH b") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("a"))(pos),
      ast.NewContextGraphs(graph("foo"), Some(graph("bar")))(pos),
      ast.ReturnedGraph(graph("b"))(pos)
    )))
  }

  test("GRAPH baz, GRAPH foo >>, GRAPH bar") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("baz"))(pos),
      ast.NewContextGraphs(graph("foo"))(pos),
      ast.ReturnedGraph(graph("bar"))(pos)
    )))
  }

  test("GRAPH foo, >> GRAPH bar, GRAPH baz") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("foo"))(pos),
      ast.NewTargetGraph(graph("bar"))(pos),
      ast.ReturnedGraph(graph("baz"))(pos)
    )))
  }

  // graphs list

  test("GRAPHS foo >>") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.NewContextGraphs(graph("foo"))(pos)
    )))
  }

  test("GRAPHS >> bar") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.NewTargetGraph(graph("bar"))(pos)
    )))
  }

  test("GRAPHS foo >> bar") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.NewContextGraphs(graph("foo"), Some(graph("bar")))(pos)
    )))
  }

  test("GRAPHS a, foo >> bar, b") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("a"))(pos),
      ast.NewContextGraphs(graph("foo"), Some(graph("bar")))(pos),
      ast.ReturnedGraph(graph("b"))(pos)
    )))
  }

  test("GRAPHS baz, foo >>, bar") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("baz"))(pos),
      ast.NewContextGraphs(graph("foo"))(pos),
      ast.ReturnedGraph(graph("bar"))(pos)
    )))
  }

  test("GRAPHS foo, >> bar, baz") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("foo"))(pos),
      ast.NewTargetGraph(graph("bar"))(pos),
      ast.ReturnedGraph(graph("baz"))(pos)
    )))
  }

  // graphs star list

  test("GRAPHS *, foo >>") {
    yields(ast.GraphReturnItems(includeExisting = true, List(
      ast.NewContextGraphs(graph("foo"))(pos)
    )))
  }

  test("GRAPHS *, >> bar") {
    yields(ast.GraphReturnItems(includeExisting = true, List(
      ast.NewTargetGraph(graph("bar"))(pos)
    )))
  }

  test("GRAPHS *, foo >> bar") {
    yields(ast.GraphReturnItems(includeExisting = true, List(
      ast.NewContextGraphs(graph("foo"), Some(graph("bar")))(pos)
    )))
  }

  test("GRAPHS *, a, foo >> bar, b") {
    yields(ast.GraphReturnItems(includeExisting = true, List(
      ast.ReturnedGraph(graph("a"))(pos),
      ast.NewContextGraphs(graph("foo"), Some(graph("bar")))(pos),
      ast.ReturnedGraph(graph("b"))(pos)
    )))
  }

  // combo list

  test("GRAPHS baz, foo >> GRAPH AT 'url' AS bar") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("baz"))(pos),
      ast.NewContextGraphs(graph("foo"), Some(graphAt("bar", "url")))(pos)
    )))
  }

  test("GRAPHS foo, GRAPH AT 'url' AS moep >> bar, baz") {
    yields(ast.GraphReturnItems(includeExisting = false, List(
      ast.ReturnedGraph(graph("foo"))(pos),
      ast.NewContextGraphs(graphAt("moep", "url"), Some(graph("bar")))(pos),
      ast.ReturnedGraph(graph("baz"))(pos)
    )))
  }
}
