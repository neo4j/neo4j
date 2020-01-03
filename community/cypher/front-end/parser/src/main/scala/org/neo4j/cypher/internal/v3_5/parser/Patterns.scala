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
package org.neo4j.cypher.internal.v3_5.parser

/*
 *|                                            NamedPatternPart                                            |
 *|Variable |                                      AnonymousPatternPart                                     |
 *                           |                                 PatternElement                              |
 *                           |               PatternElement               |        RelationshipChain       |
 *                           |NodePattern |        RelationshipChain      ||RelationshipPattern|NodePattern|
 *                                        |RelationshipPattern|NodePattern|
 *    p =      shortestPath(    (a)             -[r1]->           (b)            -[r2]->           (c)       )
 */

import org.neo4j.cypher.internal.v3_5.expressions.SemanticDirection
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.{expressions => ast}
import org.parboiled.scala._

trait Patterns extends Parser
  with Literals
  with Base {

  def Pattern: Rule1[org.neo4j.cypher.internal.v3_5.expressions.Pattern] = rule("a pattern") {
    oneOrMore(PatternPart, separator = CommaSep) ~~>> (ast.Pattern(_))
  }

  def PatternPart: Rule1[org.neo4j.cypher.internal.v3_5.expressions.PatternPart] = rule("a pattern") (
      group(Variable ~~ operator("=") ~~ AnonymousPatternPart) ~~>> (ast.NamedPatternPart(_, _))
    | AnonymousPatternPart
  )

  private def AnonymousPatternPart: Rule1[org.neo4j.cypher.internal.v3_5.expressions.AnonymousPatternPart] = rule (
      ShortestPathPattern
    | PatternElement ~~> ast.EveryPath
  )

  def ShortestPathPattern: Rule1[org.neo4j.cypher.internal.v3_5.expressions.ShortestPaths] = rule (
      (group(keyword("shortestPath") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~~>> (ast.ShortestPaths(_, single = true))
    | (group(keyword("allShortestPaths") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~~>> (ast.ShortestPaths(_, single = false))
  ).memoMismatches

  def RelationshipsPattern: Rule1[org.neo4j.cypher.internal.v3_5.expressions.RelationshipsPattern] = rule {
    group(NodePattern ~ oneOrMore(WS ~ PatternElementChain)) ~~>> (ast.RelationshipsPattern(_))
  }.memoMismatches

  private def PatternElement: Rule1[org.neo4j.cypher.internal.v3_5.expressions.PatternElement] = rule (
      NodePattern ~ zeroOrMore(WS ~ PatternElementChain)
    | "(" ~~ PatternElement ~~ ")"
  )

  private def PatternElementChain: ReductionRule1[org.neo4j.cypher.internal.v3_5.expressions.PatternElement, org.neo4j.cypher.internal.v3_5.expressions.RelationshipChain] = rule("a relationship pattern") {
    group(RelationshipPattern ~~ NodePattern) ~~>> (ast.RelationshipChain(_, _, _))
  }

  private def RelationshipPattern: Rule1[org.neo4j.cypher.internal.v3_5.expressions.RelationshipPattern] = rule {
    (
        LeftArrowHead ~~ Dash ~~ RelationshipDetail ~~ Dash ~~ RightArrowHead ~ push(SemanticDirection.BOTH)
      | LeftArrowHead ~~ Dash ~~ RelationshipDetail ~~ Dash ~ push(SemanticDirection.INCOMING)
      | Dash ~~ RelationshipDetail ~~ Dash ~~ RightArrowHead ~ push(SemanticDirection.OUTGOING)
      | Dash ~~ RelationshipDetail ~~ Dash ~ push(SemanticDirection.BOTH)
    ) ~~>> ((variable, base, relTypes, range, props, dir) => ast.RelationshipPattern(variable, relTypes.types, range,
      props, dir, relTypes.legacySeparator, base))
  }

  private def RelationshipDetail: Rule5[
      Option[ast.Variable],
      Option[org.neo4j.cypher.internal.v3_5.expressions.Variable],
      MaybeLegacyRelTypes,
      Option[Option[org.neo4j.cypher.internal.v3_5.expressions.Range]],
      Option[org.neo4j.cypher.internal.v3_5.expressions.Expression]] = rule("[") {
    (
        "[" ~~
          MaybeVariableWithBase ~~
          RelationshipTypes ~~ MaybeVariableLength ~
          MaybeProperties ~~
        "]"
      | EMPTY ~ push(None) ~ push(None) ~ push(MaybeLegacyRelTypes()) ~ push(None) ~ push(None)
    )
  }

  private def RelationshipTypes: Rule1[MaybeLegacyRelTypes] = rule("relationship types") (
    (":" ~~ RelTypeName ~~ zeroOrMore(WS ~ "|" ~~ LegacyCompatibleRelTypeName)) ~~>> (
      (first: org.neo4j.cypher.internal.v3_5.expressions.RelTypeName, more: List[(Boolean, org.neo4j.cypher.internal.v3_5.expressions.RelTypeName)]) => (pos: InputPosition) => {
        MaybeLegacyRelTypes(first +: more.map(_._2), more.exists(_._1))
      })
    | EMPTY ~ push(MaybeLegacyRelTypes())
  )

  private def LegacyCompatibleRelTypeName: Rule1[(Boolean, org.neo4j.cypher.internal.v3_5.expressions.RelTypeName)] =
    ((":" ~ push(true)) | EMPTY ~ push(false)) ~~ RelTypeName ~~>> (
      (legacy: Boolean, name: org.neo4j.cypher.internal.v3_5.expressions.RelTypeName) => (pos: InputPosition) => (legacy,name))

  private def MaybeVariableLength: Rule1[Option[Option[org.neo4j.cypher.internal.v3_5.expressions.Range]]] = rule("a length specification") (
      "*" ~~ (
          RangeLiteral ~~> (r => Some(Some(r)))
        | EMPTY ~ push(Some(None))
      )
    | EMPTY ~ push(None)
  )

  private def NodePattern: Rule1[org.neo4j.cypher.internal.v3_5.expressions.NodePattern] = rule("a node pattern") (
    group("(" ~~ MaybeVariableWithBase ~ MaybeNodeLabels ~ MaybeProperties ~~ ")") ~~>> { (v, base, labels, props) => ast.NodePattern(v, labels, props, base)}
    | group(Variable ~ MaybeNodeLabels ~ MaybeProperties)  ~~>> (ast.InvalidNodePattern(_, _, _)) // Here to give nice error messages
  )

  private def MaybeVariableWithBase: Rule2[Option[ast.Variable], Option[ast.Variable]] = rule("a variable") {
    optional(!keyword("COPY OF") ~ Variable) ~~ optional(keyword("COPY OF") ~~ Variable)
  }

  private def MaybeNodeLabels: Rule1[Seq[org.neo4j.cypher.internal.v3_5.expressions.LabelName]] = rule("node labels") (
    WS ~ NodeLabels | EMPTY ~ push(Seq())
  )

  private def MaybeProperties: Rule1[Option[org.neo4j.cypher.internal.v3_5.expressions.Expression]] = rule("a property map") (
    optional(WS ~ (MapLiteral | Parameter | OldParameter))
  )
}

case class MaybeLegacyRelTypes(types: Seq[org.neo4j.cypher.internal.v3_5.expressions.RelTypeName] = Seq.empty, legacySeparator: Boolean = false)
