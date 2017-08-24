package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.ast.{GraphRefAliasItem, GraphUrl, SingleGraphItem}
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, InputPosition, ast}

import scala.language.implicitConversions

class GraphReturnItemsParserTest
  extends ParserAstTest[ast.GraphReturnItems]
  with Graphs
  with Expressions {

  implicit val parser = GraphReturnItems

  test("must start with GRAPH or GRAPHS") {
    assertFails("foo, GRAPH a")
  }

  test("GRAPH foo >>") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewContextGraphs(g("foo"))(pos)
    )))
  }

  test(">> GRAPH bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewTargetGraph(g("bar"))(pos)
    )))
  }

  test("GRAPH foo >> GRAPH bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewContextGraphs(g("foo"), Some(g("bar")))(pos)
    )))
  }

  test("GRAPH a, GRAPH foo >> GRAPH bar, GRAPH b") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("a"))(pos),
      ast.NewContextGraphs(g("foo"), Some(g("bar")))(pos),
      ast.ReturnedGraph(g("b"))(pos)
    )))
  }

  test("GRAPH baz, GRAPH foo >>, GRAPH bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("baz"))(pos),
      ast.NewContextGraphs(g("foo"))(pos),
      ast.ReturnedGraph(g("bar"))(pos)
    )))
  }

  test("GRAPH foo, >> GRAPH bar, GRAPH baz") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("foo"))(pos),
      ast.NewTargetGraph(g("bar"))(pos),
      ast.ReturnedGraph(g("baz"))(pos)
    )))
  }

  // graphs list

  test("GRAPHS foo >>") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewContextGraphs(g("foo"))(pos)
    )))
  }

  test("GRAPHS >> bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewTargetGraph(g("bar"))(pos)
    )))
  }

  test("GRAPHS foo >> bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewContextGraphs(g("foo"), Some(g("bar")))(pos)
    )))
  }

  test("GRAPHS a, foo >> bar, b") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("a"))(pos),
      ast.NewContextGraphs(g("foo"), Some(g("bar")))(pos),
      ast.ReturnedGraph(g("b"))(pos)
    )))
  }

  test("GRAPHS baz, foo >>, bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("baz"))(pos),
      ast.NewContextGraphs(g("foo"))(pos),
      ast.ReturnedGraph(g("bar"))(pos)
    )))
  }

  test("GRAPHS foo, >> bar, baz") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("foo"))(pos),
      ast.NewTargetGraph(g("bar"))(pos),
      ast.ReturnedGraph(g("baz"))(pos)
    )))
  }

  // graphs star list

  test("GRAPHS *, foo >>") {
    yields(ast.GraphReturnItems(star = true, List(
      ast.NewContextGraphs(g("foo"))(pos)
    )))
  }

  test("GRAPHS *, >> bar") {
    yields(ast.GraphReturnItems(star = true, List(
      ast.NewTargetGraph(g("bar"))(pos)
    )))
  }

  test("GRAPHS *, foo >> bar") {
    yields(ast.GraphReturnItems(star = true, List(
      ast.NewContextGraphs(g("foo"), Some(g("bar")))(pos)
    )))
  }

  test("GRAPHS *, a, foo >> bar, b") {
    yields(ast.GraphReturnItems(star = true, List(
      ast.ReturnedGraph(g("a"))(pos),
      ast.NewContextGraphs(g("foo"), Some(g("bar")))(pos),
      ast.ReturnedGraph(g("b"))(pos)
    )))
  }

  // combo list

  test("GRAPHS baz, foo >> GRAPH AT 'url' AS bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("baz"))(pos),
      ast.NewContextGraphs(g("foo"), Some(g("bar", "url")))(pos)
    )))
  }

  test("GRAPHS foo, GRAPH AT 'url' AS moep >> bar, baz") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.ReturnedGraph(g("foo"))(pos),
      ast.NewContextGraphs(g("moep", "url"), Some(g("bar")))(pos),
      ast.ReturnedGraph(g("baz"))(pos)
    )))
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
