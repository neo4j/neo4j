package org.neo4j.cypher.internal.frontend.v3_3.parser

import org.neo4j.cypher.internal.frontend.v3_3.ast
import org.neo4j.cypher.internal.frontend.v3_3.ast.{AstConstructionTestSupport, Clause, GraphReturnItems}
import org.parboiled.scala.Rule1

class ProjectionClauseParserTest
  extends ParserAstTest[ast.Clause]
    with Query
    with Expressions
    with AstConstructionTestSupport {

  implicit val parser: Rule1[Clause] = Clause

  test("WITH *") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), None))
  }

  test("WITH 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), None))
  }

  test("WITH *, 1 AS a") {
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), None))
  }

  test("WITH ") {
    failsToParse
  }

  test("WITH GRAPH *") {
    failsToParse
  }

  ignore("WITH GRAPHS") {
    failsToParse
  }

  ignore("WITH GRAPH") {
    failsToParse
  }

  test("WITH GRAPHS a, b") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = false, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.With(graphs))
  }

  test("WITH GRAPHS *, a, b") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = true, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.With(graphs))
  }

  test("WITH 1 AS a GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = true, Seq.empty)(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), Some(graphs)))
  }

  test("WITH * GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = true, Seq.empty)(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), Some(graphs)))
  }

  test("WITH * GRAPH foo AT 'url'") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = false, Seq(ast.ReturnedGraph(graphAt("foo", "url"))(pos)))(pos)
    yields(ast.With(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), Some(graphs)))
  }

  test("WITH * GRAPH foo AT 'url' ORDER BY 2 LIMIT 1") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = false, Seq(ast.ReturnedGraph(graphAt("foo", "url"))(pos)))(pos)
    yields(ast.With(distinct = false, ast.ReturnItems(includeExisting = true, Seq.empty)(pos), Some(graphs), Some(ast.OrderBy(Seq(ast.AscSortItem(literalInt(2))(pos)))(pos)), None, Some(ast.Limit(literalInt(1))(pos)), None))
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
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = false, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.Return(graphs))
  }

  test("RETURN GRAPHS *, a, b") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = true, Seq(
      ast.ReturnedGraph(ast.GraphAs(varFor("a"), None)(pos))(pos),
      ast.ReturnedGraph(ast.GraphAs(varFor("b"), None)(pos))(pos)
    ))(pos)
    yields(ast.Return(graphs))
  }

  test("RETURN 1 AS a GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = true, Seq.empty)(pos)
    yields(ast.Return(ast.ReturnItems(includeExisting = false, Seq(ast.AliasedReturnItem(literalInt(1), varFor("a"))(pos)))(pos), Some(graphs)))
  }

  test("RETURN * GRAPHS *") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = true, Seq.empty)(pos)
    yields(ast.Return(ast.ReturnItems(includeExisting = true, Seq.empty)(pos), Some(graphs)))
  }

  test("RETURN * GRAPH foo AT 'url' ORDER BY 2 LIMIT 1") {
    val graphs: GraphReturnItems = ast.GraphReturnItems(star = false, Seq(ast.ReturnedGraph(graphAt("foo", "url"))(pos)))(pos)
    yields(ast.Return(distinct = false, ast.ReturnItems(includeExisting = true, Seq.empty)(pos), Some(graphs), Some(ast.OrderBy(Seq(ast.AscSortItem(literalInt(2))(pos)))(pos)), None, Some(ast.Limit(literalInt(1))(pos))))
  }

}
