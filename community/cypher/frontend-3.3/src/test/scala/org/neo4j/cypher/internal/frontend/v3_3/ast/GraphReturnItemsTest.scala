package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, InputPosition, SemanticState, ast}

class GraphReturnItemsTest extends CypherFunSuite {
  val foo = g("foo")
  val bar = g("bar")
  val baz = g("baz")
  val moep = g("moep")


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

  private implicit val pos: InputPosition = DummyPosition(-1)
  private implicit def v(name: String): ast.Variable = ast.Variable(name)(pos)

  private def url(addr: String): GraphUrl =
    ast.GraphUrl(Right(ast.StringLiteral(addr)(pos)))(pos)

  private def g(name: String): SingleGraphItem =
    ast.GraphRefAliasItem(ast.GraphRefAlias(ast.GraphRef(v(name))(pos), None)(pos))(pos)

  private def g(name: String, address: String): SingleGraphItem =
    ast.GraphAtItem(url(address), Some(v(name)))(pos)
}
