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
import org.parboiled.scala.ReductionRule1
import org.parboiled.scala.Rule1
import org.parboiled.scala.group

trait Query extends Parser
  with Clauses
  with Base {

  def Query: Rule1[ast.Query] = (
    RegularQuery
      | BulkImportQuery
    )

  def RegularQuery: Rule1[ast.Query] = rule {
    QueryPart ~~>> (ast.Query(None, _))
  }

  def QueryPart: Rule1[ast.QueryPart] = rule {
    SingleQuery ~ zeroOrMore(WS ~ Union)
  }

  def SingleQuery: Rule1[ast.SingleQuery] = rule {
    oneOrMore(Clause, separator = WS) ~~>> (ast.SingleQuery(_))
  }

  def BulkImportQuery: Rule1[ast.Query] = rule {
    group(PeriodicCommitHint ~ WS ~ LoadCSVQuery) ~~>> ((hint, query) => ast.Query(Some(hint), query))
  }

  def LoadCSVQuery: Rule1[ast.SingleQuery] = rule {
    LoadCSV ~ WS ~ zeroOrMore(Clause, separator = WS) ~~>> ((loadCSV, tail) => ast.SingleQuery(Seq(loadCSV) ++ tail))
  }

  def Clause: Rule1[ast.Clause] = (
    UseGraph
      | LoadCSV
      | Match
      | Unwind
      | Merge
      | CreateUnique
      | Create
      | SetClause
      | Delete
      | Remove
      | Foreach
      | With
      | Call
      | Return
      | SubqueryCall
  )

  def Union: ReductionRule1[ast.QueryPart, ast.QueryPart] = rule("UNION")(
    keyword("UNION ALL") ~>> position ~~ SingleQuery ~~> ((q: ast.QueryPart, p, sq) => ast.UnionAll(q, sq)(p))
      | keyword("UNION") ~>> position ~~ SingleQuery ~~> ((q: ast.QueryPart, p, sq) => ast.UnionDistinct(q, sq)(p))
  )
}
