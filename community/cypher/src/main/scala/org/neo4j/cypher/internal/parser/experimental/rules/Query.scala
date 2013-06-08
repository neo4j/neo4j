/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.parser.experimental.rules

import org.neo4j.cypher.internal.parser.experimental.ast
import org.parboiled.scala._

trait Query extends Parser
  with StartPoints
  with Patterns
  with Base {

  def Query : Rule1[ast.Query] = rule {
    SingleQuery ~~ zeroOrMore(Union)
  }

  def SingleQuery : Rule1[ast.SingleQuery] = rule {
    group(
      (Start ~ WS ~~> (Some(_)) | EMPTY ~ push(None)) ~
      (Match ~ WS ~~> (Some(_)) | EMPTY ~ push(None)) ~
      zeroOrMore(Hint ~ WS) ~
      (Where ~ WS ~~> (Some(_)) | EMPTY ~ push(None)) ~
      zeroOrMore(Updates ~ WS) ~
      ((Return | With) ~~> (Some(_)) | EMPTY ~ push(None))
    ) ~>> token ~~> ast.SingleQuery ~~~? (!_.isEmpty)
  }

  def Union : ReductionRule1[ast.Query, ast.Query] = rule("UNION") (
      keyword("UNION", "ALL") ~>> token ~~ SingleQuery ~~> ast.UnionAll
    | keyword("UNION") ~>> token ~~ SingleQuery ~~> ast.UnionDistinct
  )

  private def Start : Rule1[ast.Start] = rule("START") {
    group(keyword("START") ~~ oneOrMore(StartPoint, separator = CommaSep)) ~>> token ~~> ast.Start
  }

  private def Match : Rule1[ast.Match] = rule("MATCH") {
    group(keyword("MATCH") ~~ oneOrMore(Pattern, separator = CommaSep)) ~>> token ~~> ast.Match
  }

  def Merge : Rule1[ast.Merge] = rule("MERGE") {
    group(
      oneOrMore(keyword("MERGE") ~~ Pattern, separator = WS) ~~
      zeroOrMore((
          (group(keyword("ON", "MATCH") ~~ Identifier ~~ SetClause) ~>> token ~~> ast.OnMatch)
        | (group(keyword("ON", "CREATE") ~~ Identifier ~~ SetClause) ~>> token ~~> ast.OnCreate)
      ), separator = WS)
    ) ~>> token ~~> ast.Merge
  }

  private def Hint : Rule1[ast.Hint] = rule("USING") (
      group(keyword("USING", "INDEX") ~~ Identifier ~~ NodeLabel ~~ "(" ~~ Identifier ~~ ")") ~>> token ~~> ast.UsingIndexHint
    | group(keyword("USING", "SCAN") ~~ Identifier ~~ NodeLabel) ~>> token ~~> ast.UsingScanHint
  )

  private def Where : Rule1[ast.Where] = rule("WHERE") {
    group(keyword("WHERE") ~~ Expression) ~>> token ~~> ast.Where
  }

  private def Updates : Rule1[ast.UpdateClause] = rule("CREATE, DELETE, SET, REMOVE") (
      group(keyword("CREATE") ~~ oneOrMore(Pattern, separator = CommaSep)) ~>> token ~~> ast.Create
    | group(keyword("DELETE") ~~ oneOrMore(Expression, separator = CommaSep)) ~>> token ~~> ast.Delete
    | SetClause
    | group(keyword("REMOVE") ~~ oneOrMore(RemoveItem, separator = CommaSep)) ~>> token ~~> ast.Remove
  )

  private def SetClause : Rule1[ast.SetClause] = rule("SET") (
    group(keyword("SET") ~~ oneOrMore(SetItem, separator = CommaSep)) ~>> token ~~> ast.SetClause
  )

  private def SetItem : Rule1[ast.SetItem] = rule (
      Property ~~ operator("=") ~>> token ~~ Expression ~~> ast.SetPropertyItem
    | Identifier ~~ operator("=") ~>> token ~~ Parameter ~~> ast.SetNodeItem
    | group(Identifier ~~ NodeLabels) ~>> token ~~> ast.SetLabelItem
  )

  private def RemoveItem : Rule1[ast.RemoveItem] = rule {
    group(Identifier ~~ NodeLabels) ~>> token ~~> ast.RemoveLabelItem
  }

  private def With : Rule1[ast.With] = rule("WITH") (
      group(keyword("WITH", "DISTINCT") ~~ ReturnBody) ~>> token ~~ SingleQuery ~~> (new ast.With(_, _, _, _, _, _) with ast.DistinctQueryClose)
    | group(keyword("WITH") ~~ ReturnBody) ~>> token ~~ SingleQuery ~~> ast.With
  )

  private def Return : Rule1[ast.Return] = rule("RETURN") (
      group(keyword("RETURN", "DISTINCT") ~~ ReturnBody) ~>> token ~~> (new ast.Return(_, _, _, _, _) with ast.DistinctQueryClose)
    | group(keyword("RETURN") ~~ ReturnBody) ~>> token ~~> ast.Return
  )

  private def ReturnBody = {
    ReturnItems ~~
    (Order ~~> (Some(_)) | EMPTY ~ push(None)) ~~
    (Skip ~~> (Some(_)) | EMPTY ~ push(None)) ~~
    (Limit ~~> (Some(_)) | EMPTY ~ push(None))
  }

  private def ReturnItems : Rule1[ast.ReturnItems] = rule("'*', an expression") (
      "*" ~>> token ~~> ast.ReturnAll
    | oneOrMore(ReturnItem, separator = CommaSep) ~>> token ~~> ast.ListedReturnItems
  )

  private def ReturnItem : Rule1[ast.ReturnItem] = rule (
      group(Expression ~~ keyword("AS") ~~ Identifier) ~>> token ~~> ast.AliasedReturnItem
    | Expression ~>> token ~~> ast.UnaliasedReturnItem
  )

  private def Order : Rule1[ast.OrderBy] = rule {
    group(keyword("ORDER", "BY") ~~ oneOrMore(SortItem, separator = CommaSep)) ~>> token ~~> ast.OrderBy
  }

  private def SortItem : Rule1[ast.SortItem] = rule (
      group(Expression ~~ (keyword("DESCENDING") | keyword("DESC"))) ~>> token ~~> ast.DescSortItem
    | group(Expression ~~ optional(keyword("ASCENDING") | keyword("ASC"))) ~>> token ~~> ast.AscSortItem
  )

  private def Skip : Rule1[ast.Skip] = rule {
    group(keyword("SKIP") ~~ (UnsignedIntegerLiteral | Parameter)) ~>> token ~~> ast.Skip
  }

  private def Limit : Rule1[ast.Limit] = rule {
    group(keyword("LIMIT") ~~ (UnsignedIntegerLiteral | Parameter)) ~>> token ~~> ast.Limit
  }
}
