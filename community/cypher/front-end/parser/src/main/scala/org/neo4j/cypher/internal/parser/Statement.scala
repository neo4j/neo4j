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
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1
import org.parboiled.scala.group

//noinspection ConvertibleToMethodValue
// Can't convert since that breaks parsing
trait Statement extends Parser
  with GraphSelection
  with Query
  with SchemaCommand
  with AdministrationCommand
  with Base {

  def Statement: Rule1[ast.Statement] = AdministrationCommand | MultiGraphCommand | SchemaCommand | Query

  // Graph/View commands

  def MultiGraphCommand: Rule1[ast.MultiGraphDDL] = rule("Multi graph DDL statement") {
    CreateGraph | DropGraph | CreateView | DropView
  }

  def CreateGraph: Rule1[ast.CreateGraph] = rule("CATALOG CREATE GRAPH") {
    group(keyword("CATALOG CREATE GRAPH") ~~ CatalogName ~~ "{" ~~
      RegularQuery ~~
      "}") ~~>> (ast.CreateGraph(_, _))
  }

  def DropGraph: Rule1[ast.DropGraph] = rule("CATALOG DROP GRAPH") {
    group(keyword("CATALOG DROP GRAPH") ~~ CatalogName) ~~>> (ast.DropGraph(_))
  }

  def CreateView: Rule1[ast.CreateView] = rule("CATALOG CREATE VIEW") {
    group((keyword("CATALOG CREATE VIEW") | keyword("CATALOG CREATE QUERY")) ~~
      CatalogName ~~ optional("(" ~~ zeroOrMore(Parameter, separator = CommaSep) ~~ ")") ~~ "{" ~~
      captureString(RegularQuery) ~~
      "}") ~~>> { case (name, params, (query, string)) => ast.CreateView(name, params.getOrElse(Seq.empty), query, string) }
  }

  def DropView: Rule1[ast.DropView] = rule("CATALOG DROP VIEW") {
    group((keyword("CATALOG DROP VIEW") | keyword("CATALOG DROP QUERY")) ~~ CatalogName) ~~>> (ast.DropView(_))
  }

}
