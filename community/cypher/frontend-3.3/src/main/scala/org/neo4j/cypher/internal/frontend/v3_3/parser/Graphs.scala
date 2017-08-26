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
import org.neo4j.cypher.internal.frontend.v3_3.ast.Variable
import org.parboiled.scala.{Parser, Rule1, Rule2}

trait Graphs
  extends Parser
  with Expressions {

  def GraphUrl: Rule1[ast.GraphUrl] = rule("<graph-url>") {
    ((Parameter ~~> (Left(_))) | (StringLiteral ~~> (Right(_)))) ~~>> (ast.GraphUrl(_))
  }

  def GraphRef: Rule1[ast.Variable] = Variable

  def GraphRefList: Rule1[List[ast.Variable]] =
    oneOrMore(GraphRef, separator = CommaSep)

  private def GraphAs: Rule1[ast.Variable] =
    keyword("AS") ~~ Variable

  private def GraphAlias: Rule2[Variable, Option[Variable]] = rule("<graph-ref> AS <name>") {
    GraphRef ~~ optional(GraphAs)
  }

  private def GraphAliasItem: Rule1[ast.GraphAlias] = rule("GRAPH <graph-ref> [AS <name>]") {
    keyword("GRAPH") ~~ GraphAlias ~~>> (ast.GraphAlias(_, _))
  }

  private def GraphOfItem: Rule1[ast.GraphOf] = rule("GRAPH OF <pattern> [AS <name>]") {
    keyword("GRAPH") ~~ keyword("OF") ~~ Pattern ~~ optional(GraphAs) ~~>> (ast.GraphOf(_, _))
  }

  private def GraphAtItem: Rule1[ast.GraphAt] = rule("GRAPH <graph-url> AT [AS <name>]") {
    keyword("GRAPH") ~~ keyword("AT") ~~ GraphUrl ~~ optional(GraphAs) ~~>>(ast.GraphAt(_, _))
  }

  private def SourceGraphItem: Rule1[ast.SourceGraph] = rule("SOURCE GRAPH [AS <name>]") {
    keyword("SOURCE") ~~ keyword("GRAPH") ~~ optional(GraphAs) ~~>> (ast.SourceGraph(_))
  }

  private def TargetGraphItem: Rule1[ast.TargetGraph] = rule("TARGET GRAPH [AS <name>]") {
    keyword("TARGET") ~~ keyword("GRAPH") ~~ optional(GraphAs) ~~>> (ast.TargetGraph(_))
  }

  private def GraphOfShorthand: Rule1[ast.SingleGraph] =
    keyword("GRAPH") ~~ GraphRef ~~ keyword("OF") ~~ Pattern ~~>> { (ref: ast.Variable, of: ast.Pattern) => ast.GraphOf(of, Some(ref)) }

  private def GraphAtShorthand: Rule1[ast.SingleGraph] =
    keyword("GRAPH") ~~ GraphRef ~~ keyword("AT") ~~ GraphUrl ~~>> { (ref: ast.Variable, url: ast.GraphUrl) => ast.GraphAt(url, Some(ref)) }

  private def GraphAliasFirstItem = GraphOfShorthand | GraphAtShorthand

  def SingleGraphItem: Rule1[ast.SingleGraph] =
    SourceGraphItem | TargetGraphItem | GraphAtItem | GraphOfItem | GraphAliasFirstItem | GraphAliasItem

  private def SingleGraphItemList: Rule1[List[ast.SingleGraph]] =
    oneOrMore(SingleGraphItem, separator = CommaSep)

  private def ShortGraphItem: Rule1[ast.SingleGraph] =
    GraphAlias ~~>> (ast.GraphAlias(_, _))

  private def GraphItem: Rule1[ast.SingleGraph] =
    SingleGraphItem | ShortGraphItem

  // a >> b?
  private def ShortNewContextGraphs: Rule1[ast.NewContextGraphs] =
    GraphItem ~~ keyword(">>") ~~ optional(GraphItem) ~~>> { (source: ast.SingleGraph, target: Option[ast.SingleGraph]) => ast.NewContextGraphs(source, target)}

  // >> b
  private def ShortNewTargetGraph: Rule1[ast.NewTargetGraph] =
    keyword(">>") ~~ GraphItem ~~>> { (target: ast.SingleGraph) => ast.NewTargetGraph(target) }

  // a
  private def ShortReturnedGraph: Rule1[ast.ReturnedGraph] =
    ShortGraphItem ~~>> { item: ast.SingleGraph => ast.ReturnedGraph(item) }

  // GRAPH a >> (GRAPH b)?
  private def NewContextGraphs: Rule1[ast.NewContextGraphs] =
    SingleGraphItem ~~ keyword(">>") ~~ optional(GraphItem) ~~>> { (source: ast.SingleGraph, target: Option[ast.SingleGraph]) => ast.NewContextGraphs(source, target)}

  // >> GRAPH b
  private def NewTargetGraph: Rule1[ast.NewTargetGraph] =
    keyword(">>") ~~ SingleGraphItem ~~>> { (target: ast.SingleGraph) => ast.NewTargetGraph(target) }

  // GRAPH a
  private def ReturnedGraph: Rule1[ast.ReturnedGraph] =
    SingleGraphItem ~~>> { item: ast.SingleGraph => ast.ReturnedGraph(item) }

  private def GraphReturnItem: Rule1[ast.GraphReturnItem] =
    NewContextGraphs | NewTargetGraph | ReturnedGraph | ShortNewContextGraphs | ShortNewTargetGraph | ShortReturnedGraph

  private def ShortGraphReturnItemList: Rule1[ast.GraphReturnItems] =
    keyword("GRAPHS") ~~ oneOrMore(
      GraphReturnItem,
      separator = CommaSep
    ) ~~>> { itemsList: List[ast.GraphReturnItem] => ast.GraphReturnItems(star = false, itemsList) }

  private def ShortGraphStarReturnItemList: Rule1[ast.GraphReturnItems] =
    keyword("GRAPHS") ~~ keyword("*") ~~ optional(
      CommaSep ~~ oneOrMore(
        GraphReturnItem,
        separator = CommaSep
      ) ~~>> { itemsList: List[ast.GraphReturnItem] => ast.GraphReturnItems(star = true, itemsList) }
    ) ~~>> { foo: Option[ast.GraphReturnItems] => (pos) => foo.getOrElse(ast.GraphReturnItems(star = true, List.empty)(pos)) }

  private def GraphReturnItemList: Rule1[ast.GraphReturnItems] =
    oneOrMore(
      NewContextGraphs | NewTargetGraph | ReturnedGraph ,
      separator = CommaSep
    ) ~~>> { graphReturnItems => ast.GraphReturnItems(star = false, graphReturnItems) }

   def GraphReturnItems: Rule1[ast.GraphReturnItems] =
     ShortGraphStarReturnItemList | ShortGraphReturnItemList | GraphReturnItemList
}
