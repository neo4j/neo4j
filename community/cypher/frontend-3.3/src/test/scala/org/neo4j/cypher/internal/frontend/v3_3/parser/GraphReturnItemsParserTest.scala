package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.ast.{GraphRefAliasItem, GraphUrl, SingleGraphItem}
import org.neo4j.cypher.internal.frontend.v3_3.{DummyPosition, InputPosition, ast}

import scala.language.implicitConversions

class GraphReturnItemsParserTest
  extends ParserAstTest[ast.GraphReturnItems]
  with Graphs
  with Expressions {

  implicit val parser = GraphReturnItems

  test("GRAPH foo >>") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewContextGraphs(g("foo"))(pos)
    )))
  }

  test(">> GRAPH bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewTargetGraph(g("foo"))(pos)
    )))
  }

  test("GRAPH foo >> GRAPH bar") {
    yields(ast.GraphReturnItems(star = false, List(
      ast.NewContextGraphs(g("foo"), Some(g("bar")))(pos)
    )))
  }

  test("GRAPH a, GRAPH foo >> GRAPH bar, GRAPH b") {
  }

  test("GRAPH baz, GRAPH foo >>, GRAPH bar") {
  }

  test("GRAPH foo, >> GRAPH bar, GRAPH baz") {
  }

  // graphs list

  test("GRAPHS foo >>") {
  }

  test("GRAPHS >> bar") {
  }

  test("GRAPHS foo >> bar") {
  }

  test("GRAPHS a, foo >> bar, b") {
  }

  test("GRAPHS baz, foo >>, bar") {
  }

  test("GRAPHS foo, >> bar, baz") {
  }

  // graphs star list

  test("GRAPHS *, foo >>") {
  }

  test("GRAPHS *, >> bar") {
  }

  test("GRAPHS *, foo >> bar") {
  }

  test("GRAPHS *, a, foo >> bar, b") {
  }

  /*
    INTO baz
    ...
    ...
    ..
    FROM foo INTO ba

    WITH GRAPHS bar, GRAPH OF ()-[]->() >>

   */
  test("GRAPHS *, baz, foo >>, bar") {
  }

  test("GRAPHS *, foo, >> bar, baz") {
  }

  // combo list

  test("GRAPHS a, foo >> bar, b") {
  }

  test("GRAPHS baz, foo >> GRAPH AT 'foo' AS bar") {
  }

  test("GRAPHS foo, GRAPH AT 'foo' >> bar, baz") {
  }

  private implicit val pos: InputPosition = DummyPosition(-1)
  private implicit def v(name: String): ast.Variable = ast.Variable(name)(pos)

  private def g(name: String): SingleGraphItem =
    ast.GraphRefAliasItem(ast.GraphRefAlias(ast.GraphRef(v(name))(pos), None)(pos))(pos)

  private def url(addr: String): GraphUrl =
    ast.GraphUrl(Right(ast.StringLiteral(addr)(pos)))(pos)
}
