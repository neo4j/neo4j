/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast
import org.neo4j.cypher.internal.v4_0.expressions.Variable
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.parboiled.scala.{Parser, ReductionRule2, Rule1, _}

trait StartPoints extends Parser
  with Literals
  with Base {

  def StartPoint: Rule1[ast.StartItem] = rule {
    Variable ~>> position ~~ operator("=") ~~ Lookup
  }

  private def Lookup: ReductionRule2[Variable, InputPosition, ast.StartItem] = {
    NodeLookup | RelationshipLookup
  }

  private def NodeLookup: ReductionRule2[Variable, InputPosition, ast.StartItem] = {
    keyword("NODE") ~~ NodeIdLookup
  }

  private def NodeIdLookup: ReductionRule2[Variable, InputPosition, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~~> ((i: Variable, p: InputPosition, ids) => ast.NodeByIds(i, ids)(p))
      | Parameter ~~> ((i: Variable, p: InputPosition, param) => ast.NodeByParameter(i, param)(p))
      | OldParameter ~~> ((i: Variable, p: InputPosition, param) => ast.NodeByParameter(i, param)(p))
      | "*" ~~> ((i: Variable, p: InputPosition) => ast.AllNodes(i)(p))
    ) ~~ ")"
  }

  private def RelationshipLookup: ReductionRule2[Variable, InputPosition, ast.StartItem] = {
    (keyword("RELATIONSHIP") | keyword("REL")).label("RELATIONSHIP") ~~ RelationshipIdLookup
  }

  private def RelationshipIdLookup: ReductionRule2[Variable, InputPosition, ast.StartItem] = rule {
    "(" ~~ (
        LiteralIds ~~> ((i: Variable, p: InputPosition, ids) => ast.RelationshipByIds(i, ids)(p))
      | Parameter ~~> ((i: Variable, p: InputPosition, param) => ast.RelationshipByParameter(i, param)(p))
      | OldParameter ~~> ((i: Variable, p: InputPosition, param) => ast.RelationshipByParameter(i, param)(p))
      | "*" ~~> ((i: Variable, p: InputPosition) => ast.AllRelationships(i)(p))
    ) ~~ ")"
  }

  private def LiteralIds: Rule1[Seq[org.neo4j.cypher.internal.v4_0.expressions.UnsignedIntegerLiteral]] = rule("an unsigned integer") {
    oneOrMore(UnsignedIntegerLiteral, separator = CommaSep)
  }
}
