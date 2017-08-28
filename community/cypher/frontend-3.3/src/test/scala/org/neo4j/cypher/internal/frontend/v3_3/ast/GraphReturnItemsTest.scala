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

import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{SemanticFeature, SemanticState}

class GraphReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  val foo: BoundGraphAs = graph("foo")
  val bar: BoundGraphAs = graph("bar")
  val baz: BoundGraphAs = graph("baz")
  val moep: BoundGraphAs = graph("moep")

  test("set correct source and target") {
    val items = GraphReturnItems(star = false, List(
      ReturnedGraph(baz)(pos),
      NewContextGraphs(foo, Some(bar))(pos),
      ReturnedGraph(moep)(pos)
    ))(pos)

    items.newSource should equal(Some(foo))
    items.newTarget should equal(Some(bar))
  }

  test("set correct source graph") {
    val items = GraphReturnItems(star = false, List(
      ReturnedGraph(baz)(pos),
      NewContextGraphs(foo, None)(pos),
      ReturnedGraph(moep)(pos)
    ))(pos)

    items.newSource should equal(Some(foo))
    items.newTarget should equal(Some(foo))
  }

  test("set correct target graph after setting source graph only") {
    val items = GraphReturnItems(star = false, List(
      ReturnedGraph(baz)(pos),
      NewContextGraphs(foo, None)(pos),
      NewTargetGraph(bar)(pos),
      ReturnedGraph(moep)(pos)
    ))(pos)

    items.newSource should equal(Some(foo))
    items.newTarget should equal(Some(bar))
  }

  test("allow only setting one new source") {
    val items = GraphReturnItems(star = false, List(
      NewContextGraphs(foo, Some(bar))(pos),
      NewContextGraphs(baz, None)(pos)
    ))(pos)

    val result = items.semanticCheck(SemanticState.withFeatures(SemanticFeature.MultipleGraphs))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Setting multiple source graphs is not allowed")) should be(true)
  }

  test("allow only setting one new target") {
    val items = GraphReturnItems(star = false, List(
      NewTargetGraph(foo)(pos),
      NewTargetGraph(bar)(pos)
    ))(pos)

    val result = items.semanticCheck(SemanticState.withFeatures(SemanticFeature.MultipleGraphs))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Setting multiple target graphs is not allowed")) should be(true)
  }

  test("set correct source and target from single graph") {
    val items = GraphReturnItems(star = false, List(
      ReturnedGraph(baz)(pos)
    ))(pos)

    items.newSource should equal(Some(baz))
    items.newTarget should equal(Some(baz))
  }

  test("set correct source and target from single source") {
    val items = GraphReturnItems(star = false, List(
      NewContextGraphs(baz, Some(baz))(pos)
    ))(pos)

    items.newSource should equal(Some(baz))
    items.newTarget should equal(Some(baz))
  }

  test("set correct source and target from single target") {
    val items = GraphReturnItems(star = false, List(
      NewTargetGraph(baz)(pos)
    ))(pos)

    items.newSource should equal(Some(baz))
    items.newTarget should equal(Some(baz))
  }

  test("disallow declaring variable multiple times") {
    val items = GraphReturnItems(star = false, List(
      ReturnedGraph(graphAt("foo", "url"))(pos),
      ReturnedGraph(graphAt("foo", "url2"))(pos)
    ))(pos)

    val result = items.declareGraphs(None)(SemanticState.withFeatures(SemanticFeature.MultipleGraphs))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Variable `foo` already declared")) should be(true)
  }

  test("disallow multiple result graphs with same name") {
    val items = GraphReturnItems(star = false, List(
      ReturnedGraph(graphAt("foo", "url"))(pos),
      ReturnedGraph(graphAs("foo", "foo"))(pos)
    ))(pos)

    val result = items.semanticCheck(SemanticState.withFeatures(SemanticFeature.MultipleGraphs))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Multiple result graphs with the same name are not supported")) should be(true)
  }
}
