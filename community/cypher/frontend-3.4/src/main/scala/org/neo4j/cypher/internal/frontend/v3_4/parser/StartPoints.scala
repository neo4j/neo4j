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

import org.neo4j.cypher.internal.apa.v3_4.InputPosition
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.v3_4.{expressions => exp}
import org.parboiled.scala._

trait StartPoints extends Parser
  with Literals
  with Base {

  def StartPoint: Rule1[ast.StartItem] = rule {
    Variable ~>> position ~~ operator("=") ~~ Lookup
  }

  private def Lookup: ReductionRule2[exp.Variable, InputPosition, ast.StartItem] = {
    NodeLookup | RelationshipLookup
  }

  private def NodeLookup: ReductionRule2[exp.Variable, InputPosition, ast.StartItem] = {
    keyword("NODE") ~~ (NodeIndexLookup | NodeIndexQuery | NodeIdLookup)
  }

  private def NodeIdLookup: ReductionRule2[exp.Variable, InputPosition, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~~> ((i: exp.Variable, p: InputPosition, ids) => ast.NodeByIds(i, ids)(p))
      | Parameter ~~> ((i: exp.Variable, p: InputPosition, param) => ast.NodeByParameter(i, param)(p))
      | "*" ~~> ((i: exp.Variable, p: InputPosition) => ast.AllNodes(i)(p))
    ) ~~ ")"
  }

  private def NodeIndexLookup: ReductionRule2[exp.Variable, InputPosition, ast.NodeByIdentifiedIndex] = {
    IdentifiedIndexLookup ~~> ((i, p, index, key, value) => ast.NodeByIdentifiedIndex(i, index, key, value)(p))
  }

  private def NodeIndexQuery: ReductionRule2[exp.Variable, InputPosition, ast.NodeByIndexQuery] = rule {
    IndexQuery ~~> ((i: exp.Variable, p: InputPosition, index, query) => ast.NodeByIndexQuery(i, index, query)(p))
  }

  private def RelationshipLookup: ReductionRule2[exp.Variable, InputPosition, ast.StartItem] = {
    (keyword("RELATIONSHIP") | keyword("REL")).label("RELATIONSHIP") ~~ (RelationshipIndexLookup | RelationshipIndexQuery | RelationshipIdLookup)
  }

  private def RelationshipIdLookup: ReductionRule2[exp.Variable, InputPosition, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~~> ((i: exp.Variable, p: InputPosition, ids) => ast.RelationshipByIds(i, ids)(p))
      | Parameter ~~> ((i: exp.Variable, p: InputPosition, param) => ast.RelationshipByParameter(i, param)(p))
      | "*" ~~> ((i: exp.Variable, p: InputPosition) => ast.AllRelationships(i)(p))
    ) ~~ ")"
  }

  private def RelationshipIndexLookup: ReductionRule2[exp.Variable, InputPosition, ast.RelationshipByIdentifiedIndex]
  = {
    IdentifiedIndexLookup ~~> ((i, p, index, key, value) => ast.RelationshipByIdentifiedIndex(i, index, key, value)(p))
  }

  private def RelationshipIndexQuery: ReductionRule2[exp.Variable, InputPosition, ast.RelationshipByIndexQuery] = rule {
    IndexQuery ~~> ((i: exp.Variable, p: InputPosition, index, query) => ast.RelationshipByIndexQuery(i, index, query)
    (p))
  }

  private def IdentifiedIndexLookup: Rule3[String, String, exp.Expression] = rule {
    ":" ~~ SymbolicNameString ~~ "(" ~~ SymbolicNameString ~~ operator("=") ~~ (StringLiteral | Parameter) ~~ ")"
  }

  private def IndexQuery: Rule2[String, exp.Expression] = rule {
    ":" ~~ SymbolicNameString ~~ "(" ~~ (StringLiteral | Parameter) ~~ ")"
  }

  private def LiteralIds: Rule1[Seq[exp.UnsignedIntegerLiteral]] = rule("an unsigned integer") {
    oneOrMore(UnsignedIntegerLiteral, separator = CommaSep)
  }
}
