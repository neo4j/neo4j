/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.parser

import org.neo4j.cypher.internal.compiler.v2_0.ast
import org.parboiled.scala._
import org.neo4j.cypher.internal.compiler.v2_0.ast.AliasedReturnItem

trait Clauses extends Parser
  with StartPoints
  with Patterns
  with Expressions
  with Base {

  def Clause: Rule1[ast.Clause]

  def LoadCSV: Rule1[ast.LoadCSV] = rule("LOAD CSV") {
      keyword("LOAD CSV") ~~
      group(keyword("WITH HEADERS") ~ push(true) | push(false)) ~~
      keyword("FROM") ~~ StringLiteral ~~
      keyword("AS") ~~ Identifier ~~
      optional(keyword("FIELDTERMINATOR") ~~ StringLiteral) ~~
      optional(keyword("ROWTERMINATOR") ~~ StringLiteral) ~~>>
      (ast.LoadCSV(_, _, _, _, _))
  }

  def Start: Rule1[ast.Start] = rule("START") {
    group(
      keyword("START") ~~ oneOrMore(StartPoint, separator = CommaSep) ~~ optional(Where)
    ) ~~>> (ast.Start(_, _))
  }

  def Match: Rule1[ast.Match] = rule("MATCH") {
    group((
        keyword("OPTIONAL MATCH") ~ push(true)
      | keyword("MATCH") ~ push(false)
    ) ~~ Pattern ~~ zeroOrMore(Hint, separator = WS) ~~ optional(Where)) ~~>> (ast.Match(_, _, _, _))
  }

  def Merge: Rule1[ast.Merge] = rule("MERGE") {
    group(
      group(keyword("MERGE") ~~ PatternPart) ~~>> (p => ast.Pattern(Seq(p))) ~~ zeroOrMore(MergeAction, separator = WS)
    ) ~~>> (ast.Merge(_, _))
  }

  def Create: Rule1[ast.Clause] = rule("CREATE") (
      group(keyword("CREATE UNIQUE") ~~ Pattern) ~~>> (ast.CreateUnique(_))
    | group(keyword("CREATE") ~~ Pattern) ~~>> (ast.Create(_))
  )

  def SetClause: Rule1[ast.SetClause] = rule("SET") {
    group(keyword("SET") ~~ oneOrMore(SetItem, separator = CommaSep)) ~~>> (ast.SetClause(_))
  }

  def Delete: Rule1[ast.Delete] = rule("DELETE") {
    group(keyword("DELETE") ~~ oneOrMore(Expression, separator = CommaSep)) ~~>> (ast.Delete(_))
  }

  def Remove: Rule1[ast.Remove] = rule("REMOVE") {
    group(keyword("REMOVE") ~~ oneOrMore(RemoveItem, separator = CommaSep)) ~~>> (ast.Remove(_))
  }

  def Foreach: Rule1[ast.Foreach] = rule("FOREACH") {
    group(
      keyword("FOREACH") ~~ "(" ~~ Identifier ~~ keyword("IN") ~~ Expression ~~ "|" ~~
      oneOrMore(Clause, separator = WS) ~~ ")") ~~>> (ast.Foreach(_, _, _))
  }

  def With: Rule1[ast.With] = rule("WITH") (
      group(keyword("WITH DISTINCT") ~~ ReturnBody ~~ optional(Where)) ~~>> (ast.With(distinct = true, _, _, _, _, _))
    | group(keyword("WITH") ~~ ReturnBody ~~ optional(Where)) ~~>> (ast.With(distinct = false, _, _, _, _, _))
  )

  def Unwind: Rule1[ast.Unwind] = rule("UNWIND") (
    group(keyword("UNWIND") ~~ Expression ~~ keyword("AS") ~~ Identifier) ~~>> (ast.Unwind(_,_))
  )

  def Return: Rule1[ast.Return] = rule("RETURN") (
      group(keyword("RETURN DISTINCT") ~~ ReturnBody) ~~>> (ast.Return(distinct = true, _, _, _, _))
    | group(keyword("RETURN") ~~ ReturnBody) ~~>> (ast.Return(distinct = false, _, _, _, _))
  )

  private def Where: Rule1[ast.Where] = rule("WHERE") {
    group(keyword("WHERE") ~~ Expression) ~~>> (ast.Where(_))
  }

  private def Hint: Rule1[ast.Hint] = rule("USING") (
      group(keyword("USING INDEX") ~~ Identifier ~~ NodeLabel ~~ "(" ~~ Identifier ~~ ")") ~~>> (ast.UsingIndexHint(_, _, _))
    | group(keyword("USING SCAN") ~~ Identifier ~~ NodeLabel) ~~>> (ast.UsingScanHint(_, _))
  )

  private def MergeAction = rule("ON") (
      group(keyword("ON MATCH") ~~ SetClause) ~~>> (ast.OnMatch(_))
    | group(keyword("ON CREATE") ~~ SetClause) ~~>> (ast.OnCreate(_))
  )

  private def SetItem: Rule1[ast.SetItem] = rule (
      PropertyExpression ~~ group(operator("=") ~~ Expression) ~~>> (ast.SetPropertyItem(_, _))
    | Identifier ~~ group(operator("=") ~~ Expression) ~~>> (ast.SetPropertiesFromMapItem(_, _))
    | group(Identifier ~~ NodeLabels) ~~>> (ast.SetLabelItem(_, _))
  )

  private def RemoveItem: Rule1[ast.RemoveItem] = rule (
      group(Identifier ~~ NodeLabels) ~~>> (ast.RemoveLabelItem(_, _))
    | PropertyExpression ~~> ast.RemovePropertyItem
  )

  private def ReturnBody = {
    ReturnItems ~~
    optional(Order) ~~
    optional(Skip) ~~
    optional(Limit)
  }

  private def ReturnItems: Rule1[ast.ReturnItems] = rule("'*', an expression") (
      "*" ~ push(ast.ReturnAll()(_))
    | oneOrMore(ReturnItem, separator = CommaSep) ~~>> (ast.ListedReturnItems(_))
  )

  private def ReturnItem: Rule1[ast.ReturnItem] = rule (
      group(Expression ~~ keyword("AS") ~~ Identifier) ~~>> (ast.AliasedReturnItem(_, _))
    | group(Expression ~> (s => s)) ~~>> (ast.UnaliasedReturnItem(_, _))
  )

  private def Order: Rule1[ast.OrderBy] = rule {
    group(keyword("ORDER BY") ~~ oneOrMore(SortItem, separator = CommaSep)) ~~>> (ast.OrderBy(_))
  }

  private def SortItem: Rule1[ast.SortItem] = rule (
      group(Expression ~~ (keyword("DESCENDING") | keyword("DESC"))) ~~>> (ast.DescSortItem(_))
    | group(Expression ~~ optional(keyword("ASCENDING") | keyword("ASC"))) ~~>> (ast.AscSortItem(_))
  )

  private def Skip: Rule1[ast.Skip] = rule {
    group(keyword("SKIP") ~~ (UnsignedIntegerLiteral | Parameter)) ~~>> (ast.Skip(_))
  }

  private def Limit: Rule1[ast.Limit] = rule {
    group(keyword("LIMIT") ~~ (UnsignedIntegerLiteral | Parameter)) ~~>> (ast.Limit(_))
  }
}
