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
package org.neo4j.cypher.internal.parser.v2_0.rules

import org.neo4j.cypher.internal.parser.v2_0.ast
import org.parboiled.scala._

trait Clauses extends Parser
  with StartPoints
  with Patterns
  with Expressions
  with Base {

  def Clause : Rule1[ast.Clause]

  def Start : Rule1[ast.Start] = rule("START") {
    group(
      keyword("START") ~~ oneOrMore(StartPoint, separator = CommaSep) ~~ optional(Where)
    ) ~>> token ~~> ast.Start
  }

  def Match : Rule1[ast.Match] = rule("MATCH") {
    group((
        keyword("OPTIONAL", "MATCH") ~ push(true)
      | keyword("MATCH") ~ push(false)
    ) ~~ Pattern ~~ zeroOrMore(Hint, separator = WS) ~~ optional(Where)) ~>> token ~~> ast.Match
  }

  def Merge : Rule1[ast.Merge] = rule("MERGE") {
    group(
      oneOrMore(keyword("MERGE") ~~ PatternPart, separator = WS) ~~ zeroOrMore(MergeAction, separator = WS)
    ) ~>> token ~~> ast.Merge
  }

  def Create : Rule1[ast.Clause] = rule("CREATE") (
      group(keyword("CREATE", "UNIQUE") ~~ Pattern) ~>> token ~~> ast.CreateUnique
    | group(keyword("CREATE") ~~ Pattern) ~>> token ~~> ast.Create
  )

  def SetClause : Rule1[ast.SetClause] = rule("SET") {
    group(keyword("SET") ~~ oneOrMore(SetItem, separator = CommaSep)) ~>> token ~~> ast.SetClause
  }

  def Delete : Rule1[ast.Delete] = rule("DELETE") {
    group(keyword("DELETE") ~~ oneOrMore(Expression, separator = CommaSep)) ~>> token ~~> ast.Delete
  }

  def Remove : Rule1[ast.Remove] = rule("REMOVE") {
    group(keyword("REMOVE") ~~ oneOrMore(RemoveItem, separator = CommaSep)) ~>> token ~~> ast.Remove
  }

  def Foreach : Rule1[ast.Foreach] = rule("FOREACH") {
    group(
      keyword("FOREACH") ~~ "(" ~~ Identifier ~~ keyword("IN") ~~ Expression ~~ "|" ~~
      oneOrMore(Clause, separator = WS) ~~ ")") ~>> token ~~> ast.Foreach
  }

  def With : Rule1[ast.With] = rule("WITH") (
      group(keyword("WITH", "DISTINCT") ~~ ReturnBody ~~ optional(Where)) ~>> token ~~> (new ast.With(_, _, _, _, _, _) with ast.DistinctClosingClause)
    | group(keyword("WITH") ~~ ReturnBody ~~ optional(Where)) ~>> token ~~> ast.With
  )

  def Return : Rule1[ast.Return] = rule("RETURN") (
      group(keyword("RETURN", "DISTINCT") ~~ ReturnBody) ~>> token ~~> (new ast.Return(_, _, _, _, _) with ast.DistinctClosingClause)
    | group(keyword("RETURN") ~~ ReturnBody) ~>> token ~~> ast.Return
  )

  private def Where : Rule1[ast.Where] = rule("WHERE") {
    group(keyword("WHERE") ~~ Expression) ~>> token ~~> ast.Where
  }

  private def Hint : Rule1[ast.Hint] = rule("USING") (
      group(keyword("USING", "INDEX") ~~ Identifier ~~ NodeLabel ~~ "(" ~~ Identifier ~~ ")") ~>> token ~~> ast.UsingIndexHint
    | group(keyword("USING", "SCAN") ~~ Identifier ~~ NodeLabel) ~>> token ~~> ast.UsingScanHint
  )

  private def MergeAction = rule("ON") (
      group(keyword("ON", "MATCH") ~~ Identifier ~~ SetClause) ~>> token ~~> ast.OnMatch
    | group(keyword("ON", "CREATE") ~~ Identifier ~~ SetClause) ~>> token ~~> ast.OnCreate
  )

  private def SetItem : Rule1[ast.SetItem] = rule (
      PropertyExpression ~~ group(operator("=") ~~ Expression) ~>> token ~~> ast.SetPropertyItem
    | Identifier ~~ group(operator("=") ~~ Expression) ~>> token ~~> ast.SetNodeItem
    | group(Identifier ~~ NodeLabels) ~>> token ~~> ast.SetLabelItem
  )

  private def RemoveItem : Rule1[ast.RemoveItem] = rule (
      group(Identifier ~~ NodeLabels) ~>> token ~~> ast.RemoveLabelItem
    | PropertyExpression ~~> ast.RemovePropertyItem
  )

  private def ReturnBody = {
    ReturnItems ~~
    optional(Order) ~~
    optional(Skip) ~~
    optional(Limit)
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
