/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

trait StartPoints extends Parser
  with Literals
  with Base {

  def StartPoint: Rule1[ast.StartItem] = rule {
    Identifier ~>> (_.start) ~~ operator("=") ~~ Lookup
  }

  private def Lookup: ReductionRule2[ast.Identifier, Int, ast.StartItem] = {
    NodeLookup | RelationshipLookup
  }

  private def NodeLookup: ReductionRule2[ast.Identifier, Int, ast.StartItem] = {
    keyword("NODE") ~~ (NodeIndexLookup | NodeIndexQuery | NodeIdLookup)
  }

  private def NodeIdLookup: ReductionRule2[ast.Identifier, Int, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~>> (_.end) ~~>> (ast.NodeByIds(_: ast.Identifier, _))
      | Parameter ~>> (_.end) ~~>> (ast.NodeByParameter(_: ast.Identifier, _))
      | "*" ~>> (_.end) ~~>> (ast.AllNodes(_: ast.Identifier))
    ) ~~ ")"
  }

  private def NodeIndexLookup: ReductionRule2[ast.Identifier, Int, ast.NodeByIndex] = {
    IdentifiedIndexLookup ~>> (_.end) ~~>> (ast.NodeByIdentifiedIndex(_: ast.Identifier, _, _, _))
  }

  private def NodeIndexQuery: ReductionRule2[ast.Identifier, Int, ast.NodeByIndexQuery] = rule {
    IndexQuery ~>> (_.end) ~~>> (ast.NodeByIndexQuery(_: ast.Identifier, _, _))
  }

  private def RelationshipLookup: ReductionRule2[ast.Identifier, Int, ast.StartItem] = {
    (keyword("RELATIONSHIP") | keyword("REL")).label("RELATIONSHIP") ~~ (RelationshipIndexLookup | RelationshipIndexQuery | RelationshipIdLookup)
  }

  private def RelationshipIdLookup: ReductionRule2[ast.Identifier, Int, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~>> (_.end) ~~>> (ast.RelationshipByIds(_: ast.Identifier, _))
      | Parameter ~>> (_.end) ~~>> (ast.RelationshipByParameter(_: ast.Identifier, _))
      | "*" ~>> (_.end) ~~>> (ast.AllRelationships(_: ast.Identifier))
    ) ~~ ")"
  }

  private def RelationshipIndexLookup: ReductionRule2[ast.Identifier, Int, ast.StartItem] = {
    IdentifiedIndexLookup ~>> (_.end) ~~>> (ast.RelationshipByIdentifiedIndex(_: ast.Identifier, _, _, _))
  }

  private def RelationshipIndexQuery: ReductionRule2[ast.Identifier, Int, ast.RelationshipByIndexQuery] = rule {
    IndexQuery ~>> (_.end) ~~>> (ast.RelationshipByIndexQuery(_: ast.Identifier, _, _))
  }

  private def IdentifiedIndexLookup: Rule3[ast.Identifier, ast.Identifier, ast.Expression] = rule {
    ":" ~~ Identifier ~~ "(" ~~ Identifier ~~ operator("=") ~~ (StringLiteral | Parameter) ~~ ")"
  }

  private def IndexQuery: Rule2[ast.Identifier, ast.Expression] = rule {
    ":" ~~ Identifier ~~ "(" ~~ (StringLiteral | Parameter) ~~ ")"
  }

  private def LiteralIds: Rule1[Seq[ast.UnsignedIntegerLiteral]] = rule("an unsigned integer") {
    oneOrMore(UnsignedIntegerLiteral, separator = CommaSep)
  }
}
