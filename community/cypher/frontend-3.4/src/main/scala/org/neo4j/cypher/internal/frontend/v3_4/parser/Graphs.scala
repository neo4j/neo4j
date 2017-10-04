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
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.v3_4.{expressions => exp}
import org.parboiled.scala.{Parser, Rule1, Rule2}

trait Graphs
  extends Parser
  with Expressions {

  def GraphUrl: Rule1[ast.GraphUrl] = rule("<graph-url>") {
    ((Parameter ~~> (Left(_))) | (StringLiteral ~~> (Right(_)))) ~~>> (ast.GraphUrl(_))
  }

  def GraphRef: Rule1[exp.Variable] = !ReservedClauseStartKeyword ~~ Variable

  def GraphRefList: Rule1[List[exp.Variable]] =
    oneOrMore(GraphRef, separator = CommaSep)

  private def AsGraph: Rule1[exp.Variable] =
    keyword("AS") ~~ GraphRef

  private def GraphAlias: Rule2[exp.Variable, Option[exp.Variable]] = rule("<graph-ref> AS <name>") {
    GraphRef ~~ optional(AsGraph)
  }

  private def GraphAs: Rule1[ast.GraphAs] = rule("GRAPH <graph-ref> [AS <name>]") {
    keyword("GRAPH") ~~ GraphAlias ~~>> (ast.GraphAs(_, _))
  }

  private def GraphOfAs: Rule1[ast.GraphOfAs] = rule("GRAPH OF <pattern> [AS <name>]") {
    keyword("GRAPH") ~~ keyword("OF") ~~ Pattern ~~ optional(AsGraph) ~~>> (ast.GraphOfAs(_, _))
  }

  private def GraphAtAs: Rule1[ast.GraphAtAs] = rule("GRAPH AT <graph-url> [AS <name>]") {
    keyword("GRAPH") ~~ keyword("AT") ~~ GraphUrl ~~ optional(AsGraph) ~~>>(ast.GraphAtAs(_, _))
  }

  private def SourceGraphAs: Rule1[ast.SourceGraphAs] = rule("SOURCE GRAPH [AS <name>]") {
    keyword("SOURCE") ~~ keyword("GRAPH") ~~ optional(AsGraph) ~~>> (ast.SourceGraphAs(_))
  }

  private def TargetGraphAs: Rule1[ast.TargetGraphAs] = rule("TARGET GRAPH [AS <name>]") {
    keyword("TARGET") ~~ keyword("GRAPH") ~~ optional(AsGraph) ~~>> (ast.TargetGraphAs(_))
  }

  private def GraphOfShorthand: Rule1[ast.SingleGraphAs] =
    keyword("GRAPH") ~~ GraphRef ~~ keyword("OF") ~~ Pattern ~~>> { (ref: exp.Variable, of: exp.Pattern) => ast
      .GraphOfAs(of, Some(ref)) }

  private def GraphAtShorthand: Rule1[ast.SingleGraphAs] =
    keyword("GRAPH") ~~ GraphRef ~~ keyword("AT") ~~ GraphUrl ~~>> { (ref: exp.Variable, url: ast.GraphUrl) => ast
      .GraphAtAs(url, Some(ref)) }

  private def GraphShorthand = GraphOfShorthand | GraphAtShorthand

  def SingleGraph: Rule1[ast.SingleGraphAs] =
    SourceGraphAs | TargetGraphAs | GraphAtAs | GraphOfAs | GraphShorthand | GraphAs

  def BoundGraph: Rule1[ast.BoundGraphAs] =
    SourceGraphAs | TargetGraphAs | GraphAs

  private def SingleGraphAsList: Rule1[List[ast.SingleGraphAs]] =
    oneOrMore(SingleGraph, separator = CommaSep)

  private def ShortGraph: Rule1[ast.SingleGraphAs] =
    GraphAlias ~~>> (ast.GraphAs(_, _))

  private def GraphItem: Rule1[ast.SingleGraphAs] =
    SingleGraph | ShortGraph

  // a >> b?
  private def ShortNewContextGraphs: Rule1[ast.NewContextGraphs] =
    GraphItem ~~ keyword(">>") ~~ optional(GraphItem) ~~>> { (source: ast.SingleGraphAs, target: Option[ast.SingleGraphAs]) => ast.NewContextGraphs(source, target)}

  // >> b
  private def ShortNewTargetGraph: Rule1[ast.NewTargetGraph] =
    keyword(">>") ~~ GraphItem ~~>> { (target: ast.SingleGraphAs) => ast.NewTargetGraph(target) }

  // a
  private def ShortReturnedGraph: Rule1[ast.ReturnedGraph] =
    ShortGraph ~~>> { item: ast.SingleGraphAs => ast.ReturnedGraph(item) }

  // GRAPH a >> (GRAPH b)?
  private def NewContextGraphs: Rule1[ast.NewContextGraphs] =
    SingleGraph ~~ keyword(">>") ~~ optional(GraphItem) ~~>> { (source: ast.SingleGraphAs, target: Option[ast.SingleGraphAs]) => ast.NewContextGraphs(source, target)}

  // >> GRAPH b
  private def NewTargetGraph: Rule1[ast.NewTargetGraph] =
    keyword(">>") ~~ SingleGraph ~~>> { (target: ast.SingleGraphAs) => ast.NewTargetGraph(target) }

  // GRAPH a
  private def ReturnedGraph: Rule1[ast.ReturnedGraph] =
    SingleGraph ~~>> { item: ast.SingleGraphAs => ast.ReturnedGraph(item) }

  private def GraphReturnItem: Rule1[ast.GraphReturnItem] =
    NewContextGraphs | NewTargetGraph | ReturnedGraph | ShortNewContextGraphs | ShortNewTargetGraph | ShortReturnedGraph

  private def ShortGraphReturnItemList: Rule1[ast.GraphReturnItems] =
    keyword("GRAPHS") ~~ oneOrMore(
      GraphReturnItem,
      separator = CommaSep
    ) ~~>> { itemsList: List[ast.GraphReturnItem] => ast.GraphReturnItems(includeExisting = false, itemsList) }

  private def ShortGraphStarReturnItemList: Rule1[ast.GraphReturnItems] =
    keyword("GRAPHS") ~~ keyword("*") ~~ optional(
      CommaSep ~~ oneOrMore(
        GraphReturnItem,
        separator = CommaSep
      ) ~~>> { itemsList: List[ast.GraphReturnItem] => ast.GraphReturnItems(includeExisting = true, itemsList) }
    ) ~~>> { foo: Option[ast.GraphReturnItems] => (pos) => foo.getOrElse(ast.GraphReturnItems(includeExisting = true, List.empty)(pos)) }

  private def GraphReturnItemList: Rule1[ast.GraphReturnItems] =
    oneOrMore(
      NewContextGraphs | NewTargetGraph | ReturnedGraph ,
      separator = CommaSep
    ) ~~>> { graphReturnItems => ast.GraphReturnItems(includeExisting = false, graphReturnItems) }

   def GraphReturnItems: Rule1[ast.GraphReturnItems] =
     ShortGraphStarReturnItemList | ShortGraphReturnItemList | GraphReturnItemList
}
