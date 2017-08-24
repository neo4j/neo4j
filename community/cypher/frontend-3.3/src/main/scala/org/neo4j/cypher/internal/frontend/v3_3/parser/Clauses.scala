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
import org.parboiled.scala._

trait Clauses extends Parser
  with StartPoints
  with Graphs
  with Patterns
  with Expressions
  with Base
  with ProcedureCalls {

  def Clause: Rule1[ast.Clause]

  def LoadCSV: Rule1[ast.LoadCSV] = rule("LOAD CSV") {
    keyword("LOAD CSV") ~~
      group(keyword("WITH HEADERS") ~ push(true) | push(false)) ~~
      keyword("FROM") ~~ (Expression) ~~
      keyword("AS") ~~ Variable ~~
      optional(keyword("FIELDTERMINATOR") ~~ StringLiteral) ~~>>
      (ast.LoadCSV(_, _, _, _))
  }

  def FromGraph: Rule1[ast.FromGraph] = rule("FROM") {
    keyword("FROM") ~~ SingleGraphItem ~~>> (ast.FromGraph(_))
  }

  def IntoGraph: Rule1[ast.IntoGraph] = rule("INTO") {
    keyword("INTO") ~~ SingleGraphItem ~~>> (ast.IntoGraph(_))
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

  def Create: Rule1[ast.Clause] = rule("CREATE")(
    group(keyword("CREATE UNIQUE") ~~ Pattern) ~~>> (ast.CreateUnique(_))
      | group(keyword("CREATE") ~~ Pattern) ~~>> (ast.Create(_))
  )

  def SetClause: Rule1[ast.SetClause] = rule("SET") {
    group(keyword("SET") ~~ oneOrMore(SetItem, separator = CommaSep)) ~~>> (ast.SetClause(_))
  }

  def Delete: Rule1[ast.Delete] = rule("DELETE")(
    group(keyword("DELETE") ~~ oneOrMore(Expression, separator = CommaSep)) ~~>> (ast.Delete(_, forced = false))
      | group(keyword("DETACH DELETE") ~~ oneOrMore(Expression, separator = CommaSep)) ~~>> (ast.Delete(_, forced = true))
  )

  def Remove: Rule1[ast.Remove] = rule("REMOVE") {
    group(keyword("REMOVE") ~~ oneOrMore(RemoveItem, separator = CommaSep)) ~~>> (ast.Remove(_))
  }

  def Foreach: Rule1[ast.Foreach] = rule("FOREACH") {
    group(
      keyword("FOREACH") ~~ "(" ~~ Variable ~~ keyword("IN") ~~ Expression ~~ "|" ~~
        oneOrMore(Clause, separator = WS) ~~ ")") ~~>> (ast.Foreach(_, _, _))
  }

  def With: Rule1[ast.With] = rule("WITH")(
    group(keyword("WITH DISTINCT") ~~ ReturnBody ~~ optional(Where)) ~~>> (ast.With(distinct = true, _, _, _, _, _, _))
      | group(keyword("WITH") ~~ ReturnBody ~~ optional(Where)) ~~>> (ast.With(distinct = false, _, _, _, _, _, _))
  )

  def Unwind: Rule1[ast.Unwind] = rule("UNWIND")(
    group(keyword("UNWIND") ~~ Expression ~~ keyword("AS") ~~ Variable) ~~>> (ast.Unwind(_, _))
  )

  def Return: Rule1[ast.Return] = rule("RETURN")(
    group(keyword("RETURN DISTINCT") ~~ ReturnBody) ~~>> (ast.Return(distinct = true, _, _, _, _, _))
      | group(keyword("RETURN") ~~ ReturnBody) ~~>> (ast.Return(distinct = false, _, _, _, _, _))
  )

  def Pragma: Rule1[ast.Clause] = rule("") {
    keyword("_PRAGMA") ~~ (
      group(
        keyword("WITH NONE") ~ push(ast.ReturnItems(includeExisting = false, Seq())(_)) ~~ optional(Skip) ~~ optional(
          Limit) ~~ optional(Where)) ~~>> (ast.With(distinct = false, _, None, None, _, _, _))
        | group(keyword("WITHOUT") ~~ oneOrMore(Variable, separator = CommaSep)) ~~>> (ast.PragmaWithout(_))
      )
  }

  private def Where: Rule1[ast.Where] = rule("WHERE") {
    group(keyword("WHERE") ~~ Expression) ~~>> (ast.Where(_))
  }

  def PeriodicCommitHint: Rule1[ast.PeriodicCommitHint] = rule("USING PERIODIC COMMIT")(
    group(keyword("USING PERIODIC COMMIT") ~~ optional(SignedIntegerLiteral)) ~~>> (ast.PeriodicCommitHint(_))
  )

  private def Hint: Rule1[ast.UsingHint] = rule("USING")(
    group(keyword("USING INDEX") ~~ Variable ~~ NodeLabel ~~ "(" ~~ oneOrMore(PropertyKeyName, separator = CommaSep) ~~ ")") ~~>> (ast.UsingIndexHint(_, _, _))
      | group(keyword("USING JOIN ON") ~~ oneOrMore(Variable, separator = CommaSep)) ~~>> (ast.UsingJoinHint(_))
      | group(keyword("USING SCAN") ~~ Variable ~~ NodeLabel) ~~>> (ast.UsingScanHint(_, _))
  )

  private def MergeAction = rule("ON")(
    group(keyword("ON MATCH") ~~ SetClause) ~~>> (ast.OnMatch(_))
      | group(keyword("ON CREATE") ~~ SetClause) ~~>> (ast.OnCreate(_))
  )

  private def SetItem: Rule1[ast.SetItem] = rule(
    PropertyExpression ~~ group(operator("=") ~~ Expression) ~~>> (ast.SetPropertyItem(_, _))
      | Variable ~~ group(operator("=") ~~ Expression) ~~>> (ast.SetExactPropertiesFromMapItem(_, _))
      | Variable ~~ group(operator("+=") ~~ Expression) ~~>> (ast.SetIncludingPropertiesFromMapItem(_, _))
      | group(Variable ~~ NodeLabels) ~~>> (ast.SetLabelItem(_, _))
  )

  private def RemoveItem: Rule1[ast.RemoveItem] = rule(
    group(Variable ~~ NodeLabels) ~~>> (ast.RemoveLabelItem(_, _))
      | PropertyExpression ~~> ast.RemovePropertyItem
  )

  private def ReturnBody = {
    ReturnItems ~~
      // TODO: Put last
      optional(GraphReturnItems) ~~
      optional(Order) ~~
      optional(Skip) ~~
      optional(Limit)
  }

  private def ReturnItems: Rule1[ast.ReturnItemsDef] = rule("'*', an expression")(
    str("-") ~~~> ast.ReturnItems.empty
      | "*" ~ zeroOrMore(CommaSep ~ ReturnItem) ~~>> (ast.ReturnItems(includeExisting = true, _))
      | oneOrMore(ReturnItem, separator = CommaSep) ~~>> (ast.ReturnItems(includeExisting = false, _))
  )

  private def ReturnItem: Rule1[ast.ReturnItem] = rule(
    group(Expression ~~ keyword("AS") ~~ Variable) ~~>> (ast.AliasedReturnItem(_, _))
      | group(Expression ~> (s => s)) ~~>> (ast.UnaliasedReturnItem(_, _))
  )

  private def Order: Rule1[ast.OrderBy] = rule("ORDER") {
    group(keyword("ORDER BY") ~~ oneOrMore(SortItem, separator = CommaSep)) ~~>> (ast.OrderBy(_))
  }

  private def SortItem: Rule1[ast.SortItem] = rule(
    group(Expression ~~ (keyword("DESCENDING") | keyword("DESC"))) ~~>> (ast.DescSortItem(_))
      | group(Expression ~~ optional(keyword("ASCENDING") | keyword("ASC"))) ~~>> (ast.AscSortItem(_))
  )

  private def Skip: Rule1[ast.Skip] = rule("SKIP") {
    group(keyword("SKIP") ~~ Expression) ~~>> (ast.Skip(_))
  }

  private def Limit: Rule1[ast.Limit] = rule("LIMIT") {
    group(keyword("LIMIT") ~~ Expression) ~~>> (ast.Limit(_))
  }
}

