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

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.v3_4.expressions.{Pattern => ASTPattern, Variable}
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
      keyword("FROM") ~~ Expression ~~
      keyword("AS") ~~ Variable ~~
      optional(keyword("FIELDTERMINATOR") ~~ StringLiteral) ~~>>
      (ast.LoadCSV(_, _, _, _))
  }

  def From: Rule1[ast.With] = rule("FROM") {
    keyword("FROM") ~~ SingleGraph ~~>> (ast.From(_))
  }

  def Into: Rule1[ast.With] = rule("INTO") {
    keyword("INTO") ~~ SingleGraph ~~>> (ast.Into(_))
  }

  def CreateGraph: Rule1[ast.CreateGraphClause] =
    CreateNewSourceGraph | CreateNewTargetGraph | CreateRegularGraph

  private def CreateRegularGraph: Rule1[ast.CreateRegularGraph] = rule("CREATE GRAPH") {
    keyword("CREATE") ~~ CreatedGraph ~~>>(t => ast.CreateRegularGraph(t._1, t._2, t._3, t._4))
  }

  private def CreateNewSourceGraph: Rule1[ast.CreateNewSourceGraph] = rule("CREATE GRAPH >>") {
    keyword("CREATE") ~~ CreatedGraph ~~ keyword(">>") ~~>>(t => ast.CreateNewSourceGraph(t._1, t._2, t._3, t._4))
  }

  private def CreateNewTargetGraph: Rule1[ast.CreateNewTargetGraph] = rule("CREATE >> GRAPH") {
    keyword("CREATE") ~~ keyword(">>")  ~~ CreatedGraph ~~>>(t => ast.CreateNewTargetGraph(t._1, t._2, t._3, t._4))
  }

  private def CreatedGraph: Rule1[(Boolean, Variable, Option[ASTPattern], ast.GraphUrl)] =
    CreatedGraphAt | CreatedGraphAs

  private def CreatedGraphAt: Rule1[(Boolean,  Variable, Option[ASTPattern], ast.GraphUrl)] =
    OptSnapshot ~~ keyword("GRAPH") ~~ Variable ~~ optional(keyword("OF") ~~ Pattern) ~~ keyword("AT") ~~ GraphUrl ~~> {
      (snapshot, graph, pattern, url) => (snapshot, graph, pattern, url)
    }

  private def CreatedGraphAs: Rule1[(Boolean,  Variable, Option[ASTPattern], ast.GraphUrl)] =
    OptSnapshot ~~ keyword("GRAPH") ~~ optional(keyword("OF") ~~ Pattern) ~~ keyword("AT") ~~ GraphUrl ~~ keyword("AS") ~~ Variable ~~> {
      (snapshot, pattern, url, graph) => (snapshot, graph, pattern, url)
    }

  private def OptSnapshot: Rule1[Boolean] = rule("[SNAPSHOT]") {
    optional(keyword("SNAPSHOT") ~~ push(true)) ~~>(_.getOrElse(false))
  }

  def Persist: Rule1[ast.Persist] = rule("PERSIST") {
    keyword("PERSIST") ~~ BoundGraph ~~ keyword("TO") ~~ GraphUrl ~~>>(ast.Persist(_, _))
  }

  def Snapshot: Rule1[ast.Snapshot] = rule("SNAPSHOT") {
    keyword("SNAPSHOT") ~~ BoundGraph ~~ keyword("TO") ~~ GraphUrl ~~>>(ast.Snapshot(_, _))
  }

  def Relocate: Rule1[ast.Relocate] = rule("RELOCATE") {
    keyword("RELOCATE") ~~ BoundGraph ~~ keyword("TO") ~~ GraphUrl ~~>>(ast.Relocate(_, _))
  }

  def DeleteGraphs: Rule1[ast.DeleteGraphs] = rule("DELETE GRAPHS") {
    keyword("DELETE") ~~ (
      (keyword("GRAPHS") ~~ oneOrMore(Variable, separator = CommaSep) ~~>>(ast.DeleteGraphs(_)))
      |
      (oneOrMore(keyword("GRAPH") ~~ Variable, separator = CommaSep) ~~>>(ast.DeleteGraphs(_)))
    )
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
      group(keyword("MERGE") ~~ PatternPart) ~~>> (p => ASTPattern(Seq(p))) ~~ zeroOrMore(MergeAction,
        separator = WS)
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
    group(keyword("WITH DISTINCT") ~~ WithBody ~~ optional(Where)) ~~>> (ast.With(distinct = true, _, _, _, _, _, _))
      | group(keyword("WITH") ~~ GraphReturnItems) ~~>> (ast.With(_))
      | group(keyword("WITH") ~~ WithBody ~~ optional(Where)) ~~>> (ast.With(distinct = false, _, _, _, _, _, _))
  )

  def Unwind: Rule1[ast.Unwind] = rule("UNWIND")(
    group(keyword("UNWIND") ~~ Expression ~~ keyword("AS") ~~ Variable) ~~>> (ast.Unwind(_, _))
  )

  def Return: Rule1[ast.Return] = rule("RETURN")(
    group(keyword("RETURN DISTINCT") ~~ ReturnBody) ~~>> (ast.Return(distinct = true, _, _, _, _, _))
      | group(keyword("RETURN") ~~ GraphReturnItems) ~~>> (ast.Return(_))
      | group(keyword("RETURN") ~~ ReturnBody) ~~>> (ast.Return(distinct = false, _, _, _, _, _))
  )

  def Pragma: Rule1[ast.Clause] = rule("") {
    keyword("_PRAGMA") ~~ (
      group(
        keyword("WITH NONE") ~ push(ast.ReturnItems(includeExisting = false, Seq())(_)) ~~ optional(Skip) ~~ optional(
          Limit) ~~ optional(Where)) ~~>> (ast.With(distinct = false, _, ast.PassAllGraphReturnItems(InputPosition.NONE), None, _, _, _))
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

  private def WithBody: Rule5[ast.ReturnItemsDef, ast.GraphReturnItems, Option[ast.OrderBy], Option[ast.Skip], Option[ast.Limit]] = {
    ReturnItems ~~
      FakeMandatoryGraphReturnItems ~~
      optional(Order) ~~
      optional(Skip) ~~
      optional(Limit)
  }

  private def FakeMandatoryGraphReturnItems: Rule1[ast.GraphReturnItems] =
    optional(GraphReturnItems) ~~>> { (optItem) => (pos: InputPosition) => optItem.getOrElse(ast.PassAllGraphReturnItems(pos)) }

  private def ReturnBody: Rule5[ast.ReturnItemsDef, Option[ast.GraphReturnItems], Option[ast.OrderBy], Option[ast.Skip], Option[ast.Limit]] = {
    ReturnItems ~~
      optional(GraphReturnItems) ~~
      optional(Order) ~~
      optional(Skip) ~~
      optional(Limit)
  }

  private def ReturnItems: Rule1[ast.ReturnItemsDef] = rule("'*', an expression")(
    "*" ~ zeroOrMore(CommaSep ~ ReturnItem) ~~>> (ast.ReturnItems(includeExisting = true, _))
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

