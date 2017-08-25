package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.SemanticState

class GraphReturnItemsTest extends CypherFunSuite with AstConstructionTestSupport {

  val foo = graph("foo")
  val bar = graph("bar")
  val baz = graph("baz")
  val moep = graph("moep")

  test("set correct source and target") {
    val items = GraphReturnItems(false, List(
      ReturnedGraph(baz)(pos),
      NewContextGraphs(foo, Some(bar))(pos),
      ReturnedGraph(moep)(pos)
    ))(pos)

    items.newSource should equal(Some(foo))
    items.newTarget should equal(Some(bar))
  }

  test("set correct source graph") {
    val items = GraphReturnItems(false, List(
      ReturnedGraph(baz)(pos),
      NewContextGraphs(foo, None)(pos),
      ReturnedGraph(moep)(pos)
    ))(pos)

    items.newSource should equal(Some(foo))
    items.newTarget should equal(Some(foo))
  }

  test("set correct target graph after setting source graph only") {
    val items = GraphReturnItems(false, List(
      ReturnedGraph(baz)(pos),
      NewContextGraphs(foo, None)(pos),
      NewTargetGraph(bar)(pos),
      ReturnedGraph(moep)(pos)
    ))(pos)

    items.newSource should equal(Some(foo))
    items.newTarget should equal(Some(bar))
  }

  test("allow only setting one new source") {
    val items = GraphReturnItems(false, List(
      NewContextGraphs(foo, Some(bar))(pos),
      NewContextGraphs(baz, None)(pos)
    ))(pos)

    val result = items.semanticCheck(SemanticState.clean.withFeature('multigraph))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Setting multiple source graphs is not allowed")) should be(true)
  }

  test("allow only setting one new target") {
    val items = GraphReturnItems(false, List(
      NewTargetGraph(foo)(pos),
      NewTargetGraph(bar)(pos)
    ))(pos)

    val result = items.semanticCheck(SemanticState.clean.withFeature('multigraph))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Setting multiple target graphs is not allowed")) should be(true)
  }

  test("set correct source and target from single graph") {
    val items = GraphReturnItems(false, List(
      ReturnedGraph(baz)(pos)
    ))(pos)

    items.newSource should equal(Some(baz))
    items.newTarget should equal(Some(baz))
  }

  test("set correct source and target from single source") {
    val items = GraphReturnItems(false, List(
      NewContextGraphs(baz, Some(baz))(pos)
    ))(pos)

    items.newSource should equal(Some(baz))
    items.newTarget should equal(Some(baz))
  }

  test("set correct source and target from single target") {
    val items = GraphReturnItems(false, List(
      NewTargetGraph(baz)(pos)
    ))(pos)

    items.newSource should equal(Some(baz))
    items.newTarget should equal(Some(baz))
  }

  test("disallow declaring variable multiple times") {
    val items = GraphReturnItems(false, List(
      ReturnedGraph(graphAt("foo", "url"))(pos),
      ReturnedGraph(graphAt("foo", "url2"))(pos)
    ))(pos)

    val result = items.semanticCheck(SemanticState.clean.withFeature('multigraph))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Variable `foo` already declared")) should be(true)
  }

  test("disallow multiple result graphs with same name") {
    val items = GraphReturnItems(false, List(
      ReturnedGraph(graphAt("foo", "url"))(pos),
      ReturnedGraph(graphAs("foo", "foo"))(pos)
    ))(pos)

    val result = items.semanticCheck(SemanticState.clean.withFeature('multigraph))
    val errors = result.errors.toSet

    errors.exists(_.msg.contains("Multiple result graphs with the same name are not supported")) should be(true)
  }
}
