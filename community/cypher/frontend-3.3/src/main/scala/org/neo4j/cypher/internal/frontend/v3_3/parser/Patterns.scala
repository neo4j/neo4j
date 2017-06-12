/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_3.parser

/*
 *|                                            NamedPatternPart                                            |
 *|Variable |                                      AnonymousPatternPart                                     |
 *                           |                                 PatternElement                              |
 *                           |               PatternElement               |        RelationshipChain       |
 *                           |NodePattern |        RelationshipChain      ||RelationshipPattern|NodePattern|
 *                                        |RelationshipPattern|NodePattern|
 *    p =      shortestPath(    (a)             -[r1]->           (b)            -[r2]->           (c)       )
 */

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticDirection, ast}
import org.parboiled.scala._

trait Patterns extends Parser
  with Literals
  with Base {

  def Pattern: Rule1[ast.Pattern] = rule("a pattern") {
    optional(Variable ~~ operator(":=")) ~~ oneOrMore(PatternPart, separator = CommaSep) ~~>> (ast.Pattern(_, _))
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
    ) ~~>> ((variable, relTypes, range, props, dir) => ast.RelationshipPattern(variable, relTypes.types, range, props, dir, relTypes.legacySeparator))
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
