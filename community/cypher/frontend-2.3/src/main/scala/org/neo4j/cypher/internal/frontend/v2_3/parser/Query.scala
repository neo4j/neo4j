/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.parser

import org.neo4j.cypher.internal.frontend.v2_3.ast
import org.parboiled.scala._

trait Query extends Parser
  with Clauses
  with Base {

  def Query: Rule1[ast.Query] = (
      RegularQuery
    | BulkImportQuery
  )

  def RegularQuery: Rule1[ast.Query] = rule {
    SingleQuery ~ zeroOrMore(WS ~ Union) ~~>> (ast.Query(None, _))
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
      LoadCSV
    | Start
    | Match
    | Unwind
    | Merge
    | Create
    | SetClause
    | Delete
    | Remove
    | Foreach
    | With
    | Return
    | Pragma
  )

  def Union: ReductionRule1[ast.QueryPart, ast.QueryPart] = rule("UNION") (
      keyword("UNION ALL") ~>> position ~~ SingleQuery ~~> ((q: ast.QueryPart, p, sq) => ast.UnionAll(q, sq)(p))
    | keyword("UNION") ~>> position ~~ SingleQuery ~~> ((q: ast.QueryPart, p, sq) => ast.UnionDistinct(q, sq)(p))
  )
}
