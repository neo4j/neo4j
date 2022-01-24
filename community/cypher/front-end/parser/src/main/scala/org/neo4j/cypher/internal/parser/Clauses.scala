/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule4
import org.parboiled.scala.Rule5
import org.parboiled.scala.group

trait Clauses extends Parser
  with Patterns
  with Expressions
  with Base
  with ProcedureCalls
  with GraphSelection {
  self: Query =>

  def Clause: Rule1[ast.Clause]

  def LoadCSV: Rule1[ast.LoadCSV] = rule("LOAD CSV") {
    keyword("LOAD CSV") ~~
      group(keyword("WITH HEADERS") ~ push(true) | push(false)) ~~
      keyword("FROM") ~~ Expression ~~
      keyword("AS") ~~ Variable ~~
      optional(keyword("FIELDTERMINATOR") ~~ StringLiteral) ~~>>
      (ast.LoadCSV(_, _, _, _))
  }

  def Match: Rule1[ast.Match] = rule("MATCH") {
    group((
      keyword("OPTIONAL MATCH") ~ push(true)
        | keyword("MATCH") ~ push(false)
      ) ~~ Pattern ~~ zeroOrMore(Hint, separator = WS) ~~ optional(Where)) ~~>> (ast.Match(_, _, _, _))
  }

  def Merge: Rule1[ast.Merge] = rule("MERGE") {
    group(
      group(keyword("MERGE") ~~ PatternPart) ~~ zeroOrMore(MergeAction,
        separator = WS)
    ) ~~>> (ast.Merge(_, _))
  }

  def Create: Rule1[ast.Create] = rule("CREATE") {
    group(keyword("CREATE") ~~ Pattern) ~~>> (ast.Create(_))
  }

  def CreateUnique: Rule1[ast.CreateUnique] = rule("CREATE UNIQUE") {
    group(keyword("CREATE UNIQUE") ~~ Pattern) ~~>> (ast.CreateUnique(_))
  }

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
    group(keyword("WITH DISTINCT") ~~ WithBody ~~ optional(Where)) ~~>> (ast.With(distinct = true, _, _, _, _, _))
      | group(keyword("WITH") ~~ WithBody ~~ optional(Where)) ~~>> (ast.With(distinct = false, _, _, _, _, _))
  )

  def Unwind: Rule1[ast.Unwind] = rule("UNWIND")(
    group(keyword("UNWIND") ~~ Expression ~~ keyword("AS") ~~ Variable) ~~>> (ast.Unwind(_, _))
  )

  def Return: Rule1[ast.Return] = rule("RETURN")(
    group(keyword("RETURN DISTINCT") ~~ ReturnBody) ~~>> (ast.Return(distinct = true, _, _, _, _))
      | group(keyword("RETURN") ~~ ReturnBody) ~~>> (ast.Return(distinct = false, _, _, _, _))
  )

  def Where: Rule1[ast.Where] = rule("WHERE") {
    group(keyword("WHERE") ~~ Expression) ~~>> (ast.Where(_))
  }

  def PeriodicCommitHint: Rule1[ast.PeriodicCommitHint] = rule("USING PERIODIC COMMIT")(
    group(keyword("USING PERIODIC COMMIT") ~~ optional(SignedIntegerLiteral)) ~~>> (ast.PeriodicCommitHint(_))
  )

  private def Hint: Rule1[ast.UsingHint] = rule("USING")(
    group(keyword("USING INDEX SEEK") ~~ Variable ~~ NodeLabelOrRelType ~~ "(" ~~ oneOrMore(PropertyKeyName, separator = CommaSep) ~~ ")") ~~>> (ast.UsingIndexHint(_, _, _, ast.SeekOnly))
      | group(keyword("USING INDEX") ~~ Variable ~~ NodeLabelOrRelType ~~ "(" ~~ oneOrMore(PropertyKeyName, separator = CommaSep) ~~ ")") ~~>> (ast.UsingIndexHint(_, _, _, ast.SeekOrScan))
      | group(keyword("USING JOIN ON") ~~ oneOrMore(Variable, separator = CommaSep)) ~~>> (ast.UsingJoinHint(_))
      | group(keyword("USING SCAN") ~~ Variable ~~ NodeLabelOrRelType) ~~>> (ast.UsingScanHint(_, _))
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

  private def WithBody: Rule4[ast.ReturnItems, Option[ast.OrderBy], Option[ast.Skip], Option[ast.Limit]] = {
    ReturnItems ~~
      optional(Order) ~~
      optional(Skip) ~~
      optional(Limit)
  }

  def ReturnBody: Rule4[ast.ReturnItems, Option[ast.OrderBy], Option[ast.Skip], Option[ast.Limit]] = {
    ReturnItems ~~
      optional(Order) ~~
      optional(Skip) ~~
      optional(Limit)
  }

  private def ReturnItems: Rule1[ast.ReturnItems] = rule("'*', an expression")(
    "*" ~ zeroOrMore(CommaSep ~ ReturnItem) ~~>> (ast.ReturnItems(includeExisting = true, _))
      | oneOrMore(ReturnItem, separator = CommaSep) ~~>> (ast.ReturnItems(includeExisting = false, _))
  )

  private def ReturnItem: Rule1[ast.ReturnItem] = rule(
    group(Expression ~~ keyword("AS") ~~ Variable) ~~>> ((v, alias) => ast.AliasedReturnItem(v, alias)(_, isAutoAliased = false))
      | group(Expression ~> (s => s)) ~~>> (ast.UnaliasedReturnItem(_, _))
  )

  def Yield: Rule1[Yield] = (keyword("YIELD") ~~ YieldBody) ~~>> (ast.Yield(_, _, _, _, _))

  def YieldBody: Rule5[ast.ReturnItems, Option[ast.OrderBy], Option[ast.Skip], Option[ast.Limit], Option[ast.Where]] = {
    YieldItems ~~
      optional(Order) ~~
      optional(group(keyword("SKIP") ~~ SignedIntegerLiteral) ~~>> (ast.Skip(_))) ~~
      optional(group(keyword("LIMIT") ~~ SignedIntegerLiteral) ~~>> (ast.Limit(_))) ~~
      optional(Where)
  }

  private def YieldItems: Rule1[ast.ReturnItems] = rule("'*', an expression")(
    keyword("*") ~~~> ast.ReturnItems(includeExisting = true, List())
      | oneOrMore(YieldItem, separator = CommaSep) ~~>> (ast.ReturnItems(includeExisting = false, _))
  )
  private def YieldItem: Rule1[ast.ReturnItem] = rule(
    group(Variable ~~ keyword("AS") ~~ Variable) ~~>> ((expr, alias) => ast.AliasedReturnItem(expr, alias)(_, isAutoAliased = false))
      | group(Variable ~> (s => s)) ~~>> (ast.UnaliasedReturnItem(_, _))
  )

  def Order: Rule1[ast.OrderBy] = rule("ORDER") {
    group(keyword("ORDER BY") ~~ oneOrMore(SortItem, separator = CommaSep)) ~~>> (ast.OrderBy(_))
  }

  private def SortItem: Rule1[ast.SortItem] = rule(
    group(Expression ~~ (keyword("DESCENDING") | keyword("DESC"))) ~~>> (expr => ast.DescSortItem(expr)(_))
      | group(Expression ~~ optional(keyword("ASCENDING") | keyword("ASC"))) ~~>> (expr => ast.AscSortItem(expr)(_))
  )

  private def Skip: Rule1[ast.Skip] = rule("SKIP") {
    group(keyword("SKIP") ~~ Expression) ~~>> (ast.Skip(_))
  }

  private def Limit: Rule1[ast.Limit] = rule("LIMIT") {
    group(keyword("LIMIT") ~~ Expression) ~~>> (ast.Limit(_))
  }

  def SubqueryCall: Rule1[ast.SubqueryCall] = rule("CALL") {
    group(keyword("CALL") ~~ group("{" ~~ QueryPart ~~ "}")) ~~>> (part => ast.SubqueryCall(part, None))
  }
}
