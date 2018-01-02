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

/*
 *|                                             NamedPatternPart                                            |
 *|Identifier|                                     AnonymousPatternPart                                     |
 *                           |                                 PatternElement                              |
 *                           |               PatternElement               |        RelationshipChain       |
 *                           |NodePattern |        RelationshipChain      ||RelationshipPattern|NodePattern|
 *                                        |RelationshipPattern|NodePattern|
 *    p =      shortestPath(    (a)             -[r1]->           (b)            -[r2]->           (c)       )
 */

import org.neo4j.cypher.internal.frontend.v2_3.{SemanticDirection, ast}
import org.parboiled.scala._

trait Patterns extends Parser
  with Literals
  with Base {

  def Pattern: Rule1[ast.Pattern] = rule("a pattern") {
    oneOrMore(PatternPart, separator = CommaSep) ~~>> (ast.Pattern(_))
  }

  def PatternPart: Rule1[ast.PatternPart] = rule("a pattern") (
      group(Identifier ~~ operator("=") ~~ AnonymousPatternPart) ~~>> (ast.NamedPatternPart(_, _))
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
    ) ~~>> (ast.RelationshipPattern(_, _, _, _, _, _))
  }

  private def RelationshipDetail: Rule5[
      Option[ast.Identifier],
      Boolean,
      Seq[ast.RelTypeName],
      Option[Option[ast.Range]],
      Option[ast.Expression]] = rule("[") {
    (
        "[" ~~
          MaybeIdentifier ~~
          ("?" ~ push(true) | EMPTY ~ push(false)) ~~
          RelationshipTypes ~~ MaybeVariableLength ~
          MaybeProperties ~~
        "]"
      | EMPTY ~ push(None) ~ push(false) ~ push(Seq()) ~ push(None) ~ push(None)
    )
  }

  private def RelationshipTypes: Rule1[Seq[ast.RelTypeName]] = rule("relationship types") (
      ":" ~~ oneOrMore(RelTypeName, separator = WS ~ "|" ~~ optional(":") ~ WS)
    | EMPTY ~ push(Seq())
  )

  private def MaybeVariableLength: Rule1[Option[Option[ast.Range]]] = rule("a length specification") (
      "*" ~~ (
          RangeLiteral ~~> (r => Some(Some(r)))
        | EMPTY ~ push(Some(None))
      )
    | EMPTY ~ push(None)
  )

  private def NodePattern: Rule1[ast.NodePattern] = rule("a node pattern") (
      group("(" ~~ MaybeIdentifier ~ MaybeNodeLabels ~ MaybeProperties ~~ ")") ~~>> (ast.NodePattern(_, _, _, naked = false))
    | group(MaybeIdentifier ~ MaybeNodeLabels ~ MaybeProperties ~~~? ((i, l, p) => !(i.isEmpty && l.isEmpty && p.isEmpty))) ~~>> (ast.NodePattern(_, _, _, naked = true))
  )

  private def MaybeIdentifier: Rule1[Option[ast.Identifier]] = rule("an identifier") {
    optional(Identifier)
  }

  private def MaybeNodeLabels: Rule1[Seq[ast.LabelName]] = rule("node labels") (
    WS ~ NodeLabels | EMPTY ~ push(Seq())
  )

  private def MaybeProperties: Rule1[Option[ast.Expression]] = rule("a property map") (
    optional(WS ~ (MapLiteral | Parameter))
  )
}
