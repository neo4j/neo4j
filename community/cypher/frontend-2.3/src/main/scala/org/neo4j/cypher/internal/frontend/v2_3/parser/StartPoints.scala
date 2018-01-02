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

import org.neo4j.cypher.internal.frontend.v2_3.{InputPosition, ast}
import org.parboiled.scala._

trait StartPoints extends Parser
  with Literals
  with Base {

  def StartPoint: Rule1[ast.StartItem] = rule {
    Identifier ~>> position ~~ operator("=") ~~ Lookup
  }

  private def Lookup: ReductionRule2[ast.Identifier, InputPosition, ast.StartItem] = {
    NodeLookup | RelationshipLookup
  }

  private def NodeLookup: ReductionRule2[ast.Identifier, InputPosition, ast.StartItem] = {
    keyword("NODE") ~~ (NodeIndexLookup | NodeIndexQuery | NodeIdLookup)
  }

  private def NodeIdLookup: ReductionRule2[ast.Identifier, InputPosition, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~~> ((i: ast.Identifier, p: InputPosition, ids) => ast.NodeByIds(i, ids)(p))
      | Parameter ~~> ((i: ast.Identifier, p: InputPosition, param) => ast.NodeByParameter(i, param)(p))
      | "*" ~~> ((i: ast.Identifier, p: InputPosition) => ast.AllNodes(i)(p))
    ) ~~ ")"
  }

  private def NodeIndexLookup: ReductionRule2[ast.Identifier, InputPosition, ast.NodeByIdentifiedIndex] = {
    IdentifiedIndexLookup ~~> ((i, p, index, key, value) => ast.NodeByIdentifiedIndex(i, index, key, value)(p))
  }

  private def NodeIndexQuery: ReductionRule2[ast.Identifier, InputPosition, ast.NodeByIndexQuery] = rule {
    IndexQuery ~~> ((i: ast.Identifier, p: InputPosition, index, query) => ast.NodeByIndexQuery(i, index, query)(p))
  }

  private def RelationshipLookup: ReductionRule2[ast.Identifier, InputPosition, ast.StartItem] = {
    (keyword("RELATIONSHIP") | keyword("REL")).label("RELATIONSHIP") ~~ (RelationshipIndexLookup | RelationshipIndexQuery | RelationshipIdLookup)
  }

  private def RelationshipIdLookup: ReductionRule2[ast.Identifier, InputPosition, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~~> ((i: ast.Identifier, p: InputPosition, ids) => ast.RelationshipByIds(i, ids)(p))
      | Parameter ~~> ((i: ast.Identifier, p: InputPosition, param) => ast.RelationshipByParameter(i, param)(p))
      | "*" ~~> ((i: ast.Identifier, p: InputPosition) => ast.AllRelationships(i)(p))
    ) ~~ ")"
  }

  private def RelationshipIndexLookup: ReductionRule2[ast.Identifier, InputPosition, ast.RelationshipByIdentifiedIndex] = {
    IdentifiedIndexLookup ~~> ((i, p, index, key, value) => ast.RelationshipByIdentifiedIndex(i, index, key, value)(p))
  }

  private def RelationshipIndexQuery: ReductionRule2[ast.Identifier, InputPosition, ast.RelationshipByIndexQuery] = rule {
    IndexQuery ~~> ((i: ast.Identifier, p: InputPosition, index, query) => ast.RelationshipByIndexQuery(i, index, query)(p))
  }

  private def IdentifiedIndexLookup: Rule3[String, String, ast.Expression] = rule {
    ":" ~~ SymbolicNameString ~~ "(" ~~ SymbolicNameString ~~ operator("=") ~~ (StringLiteral | Parameter) ~~ ")"
  }

  private def IndexQuery: Rule2[String, ast.Expression] = rule {
    ":" ~~ SymbolicNameString ~~ "(" ~~ (StringLiteral | Parameter) ~~ ")"
  }

  private def LiteralIds: Rule1[Seq[ast.UnsignedIntegerLiteral]] = rule("an unsigned integer") {
    oneOrMore(UnsignedIntegerLiteral, separator = CommaSep)
  }
}
