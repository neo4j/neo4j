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

/*
 *|                                            NamedPatternPart                                            |
 *|Variable |                                      AnonymousPatternPart                                     |
 *                           |                                 PatternElement                              |
 *                           |               PatternElement               |        RelationshipChain       |
 *                           |NodePattern |        RelationshipChain      ||RelationshipPattern|NodePattern|
 *                                        |RelationshipPattern|NodePattern|
 *    p =      shortestPath(    (a)             -[r1]->           (b)            -[r2]->           (c)       )
 */

import org.neo4j.cypher.internal.util.v3_4.InputPosition
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.parboiled.scala._

trait Patterns extends Parser
  with Literals
  with Base {

  def Pattern: Rule1[ast.Pattern] = rule("a pattern") {
    oneOrMore(PatternPart, separator = CommaSep) ~~>> (ast.Pattern(_))
  }

  def PatternPart: Rule1[ast.PatternPart] = rule("a pattern") (
      group(Variable ~~ operator("=") ~~ AnonymousPatternPart) ~~>> (ast.NamedPatternPart(_, _))
    | AnonymousPatternPart
  )

  private def AnonymousPatternPart: Rule1[ast.AnonymousPatternPart] = rule (
      ShortestPathPattern
    | PatternElement ~~> ast.EveryPath
  )

  def ShortestPathPattern: Rule1[ast.ShortestPaths] = rule (
      (group(keyword("shortestPath") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~~>> (ast.ShortestPaths(_, single = true))
    | (group(keyword("allShortestPaths") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~~>> (ast.ShortestPaths(_, single = false))
  ).memoMismatches

  def RelationshipsPattern: Rule1[ast.RelationshipsPattern] = rule {
    group(NodePattern ~ oneOrMore(WS ~ PatternElementChain)) ~~>> (ast.RelationshipsPattern(_))
  }.memoMismatches

  private def PatternElement: Rule1[ast.PatternElement] = rule (
      NodePattern ~ zeroOrMore(WS ~ PatternElementChain)
    | "(" ~~ PatternElement ~~ ")"
  )

  private def PatternElementChain: ReductionRule1[ast.PatternElement, ast.RelationshipChain] = rule("a relationship pattern") {
    group(RelationshipPattern ~~ NodePattern) ~~>> (ast.RelationshipChain(_, _, _))
  }

  private def RelationshipPattern: Rule1[ast.RelationshipPattern] = rule {
    (
        LeftArrowHead ~~ Dash ~~ RelationshipDetail ~~ Dash ~~ RightArrowHead ~ push(SemanticDirection.BOTH)
      | LeftArrowHead ~~ Dash ~~ RelationshipDetail ~~ Dash ~ push(SemanticDirection.INCOMING)
      | Dash ~~ RelationshipDetail ~~ Dash ~~ RightArrowHead ~ push(SemanticDirection.OUTGOING)
      | Dash ~~ RelationshipDetail ~~ Dash ~ push(SemanticDirection.BOTH)
    ) ~~>> ((variable, relTypes, range, props, dir) => ast.RelationshipPattern(variable, relTypes.types, range,
      props, dir, relTypes.legacySeparator))
  }

  private def RelationshipDetail: Rule4[
      Option[ast.Variable],
      MaybeLegacyRelTypes,
      Option[Option[ast.Range]],
      Option[ast.Expression]] = rule("[") {
    (
        "[" ~~
          MaybeVariable ~~
          RelationshipTypes ~~ MaybeVariableLength ~
          MaybeProperties ~~
        "]"
      | EMPTY ~ push(None) ~ push(MaybeLegacyRelTypes()) ~ push(None) ~ push(None)
    )
  }

  private def RelationshipTypes: Rule1[MaybeLegacyRelTypes] = rule("relationship types") (
    (":" ~~ RelTypeName ~~ zeroOrMore(WS ~ "|" ~~ LegacyCompatibleRelTypeName)) ~~>> (
      (first: ast.RelTypeName, more: List[(Boolean, ast.RelTypeName)]) => (pos: InputPosition) => {
        MaybeLegacyRelTypes(first +: more.map(_._2), more.exists(_._1))
      })
    | EMPTY ~ push(MaybeLegacyRelTypes())
  )

  private def LegacyCompatibleRelTypeName: Rule1[(Boolean, ast.RelTypeName)] =
    ((":" ~ push(true)) | EMPTY ~ push(false)) ~~ RelTypeName ~~>> (
      (legacy: Boolean, name: ast.RelTypeName) => (pos: InputPosition) => (legacy,name))

  private def MaybeVariableLength: Rule1[Option[Option[ast.Range]]] = rule("a length specification") (
      "*" ~~ (
          RangeLiteral ~~> (r => Some(Some(r)))
        | EMPTY ~ push(Some(None))
      )
    | EMPTY ~ push(None)
  )

  private def NodePattern: Rule1[ast.NodePattern] = rule("a node pattern") (
      group("(" ~~ MaybeVariable ~ MaybeNodeLabels ~ MaybeProperties ~~ ")") ~~>> (ast.NodePattern(_, _, _))
    | group(Variable ~ MaybeNodeLabels ~ MaybeProperties)  ~~>> (ast.InvalidNodePattern(_, _, _)) // Here to give nice error messages
  )

  private def MaybeVariable: Rule1[Option[ast.Variable]] = rule("a variable") {
    optional(Variable)
  }

  private def MaybeNodeLabels: Rule1[Seq[ast.LabelName]] = rule("node labels") (
    WS ~ NodeLabels | EMPTY ~ push(Seq())
  )

  private def MaybeProperties: Rule1[Option[ast.Expression]] = rule("a property map") (
    optional(WS ~ (MapLiteral | Parameter))
  )
}

case class MaybeLegacyRelTypes(types: Seq[ast.RelTypeName] = Seq.empty, legacySeparator: Boolean = false)
