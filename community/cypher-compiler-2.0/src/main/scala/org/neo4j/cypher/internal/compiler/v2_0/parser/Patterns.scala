/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

/*
 *|                                             NamedPatternPart                                            |
 *|Identifier|                                     AnonymousPatternPart                                     |
 *                           |                                 PatternElement                              |
 *                           |               PatternElement               |        RelationshipChain       |
 *                           |NodePattern |        RelationshipChain      ||RelationshipPattern|NodePattern|
 *                                        |RelationshipPattern|NodePattern|
 *    p =      shortestPath(    (a)             -[r1]->           (b)            -[r2]->           (c)       )
 */

import org.neo4j.cypher.internal.compiler.v2_0.InputToken
import org.neo4j.cypher.internal.compiler.v2_0.ast
import org.parboiled.scala._
import org.neo4j.graphdb.Direction

trait Patterns extends Parser
  with Literals
  with Base {

  def Pattern : Rule1[ast.Pattern] = rule("a pattern") {
    oneOrMore(PatternPart, separator = CommaSep) ~>> token ~~> (ast.Pattern(_, _))
  }

  def PatternPart : Rule1[ast.PatternPart] = rule("a pattern") (
      group(Identifier ~~ operator("=") ~~ AnonymousPatternPart) ~>> token ~~> ast.NamedPatternPart
    | AnonymousPatternPart
  )

  private def AnonymousPatternPart : Rule1[ast.AnonymousPatternPart] = rule (
      ShortestPathPattern
    | PatternElement ~~> ast.EveryPath
  )

  def ShortestPathPattern : Rule1[ast.ShortestPath] = rule (
      (group(keyword("shortestPath") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~>> token ~~> ast.SingleShortestPath
    | (group(keyword("allShortestPaths") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~>> token ~~> ast.AllShortestPaths
  ).memoMismatches

  def RelationshipsPattern : Rule1[ast.RelationshipsPattern] = rule {
    group(NodePattern ~ oneOrMore(WS ~ PatternElementChain)) ~>> token ~~> ast.RelationshipsPattern
  }.memoMismatches

  private def PatternElement : Rule1[ast.PatternElement] = rule (
      NodePattern ~ zeroOrMore(WS ~ PatternElementChain)
    | "(" ~~ PatternElement ~~ ")"
  )

  private def PatternElementChain : ReductionRule1[ast.PatternElement, ast.RelationshipChain] = rule("a relationship pattern") {
    group(RelationshipPattern ~~ NodePattern) ~>> token ~~> ast.RelationshipChain
  }

  private def RelationshipPattern : Rule1[ast.RelationshipPattern] = rule {
    (
        "<" ~~ "-" ~~ RelationshipDetail ~~ "-" ~~ ">" ~ push(Direction.BOTH)
      | "<" ~~ "-" ~~ RelationshipDetail ~~ "-" ~ push(Direction.INCOMING)
      | "-" ~~ RelationshipDetail ~~ "-" ~~ ">" ~ push(Direction.OUTGOING)
      | "-" ~~ RelationshipDetail ~~ "-" ~ push(Direction.BOTH)
    ) ~>> token ~~> toRelationshipPattern _
  }

  private def RelationshipDetail : Rule5[
      Option[ast.Identifier],
      Boolean,
      Seq[ast.Identifier],
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

  private def RelationshipTypes : Rule1[Seq[ast.Identifier]] = rule("relationship types") (
      ":" ~~ oneOrMore(Identifier, separator = (WS ~ "|" ~~ optional(":") ~ WS))
    | EMPTY ~ push(Seq())
  )

  private def MaybeVariableLength : Rule1[Option[Option[ast.Range]]] = rule("a length specification") (
      "*" ~~ (
          RangeLiteral ~~> (r => Some(Some(r)))
        | EMPTY ~ push(Some(None))
      )
    | EMPTY ~ push(None)
  )

  private def NodePattern : Rule1[ast.NodePattern] = rule("a node pattern") (
      group("(" ~~ MaybeIdentifier ~ MaybeNodeLabels ~ MaybeProperties ~~ ")" ~ push(false)) ~>> token ~~> toNodePattern _
    | group(MaybeIdentifier ~ MaybeNodeLabels ~ MaybeProperties ~~~? ((i, l, p) => !(i.isEmpty && l.isEmpty && p.isEmpty)) ~ push(true)) ~>> token ~~> toNodePattern _
  )

  private def MaybeIdentifier : Rule1[Option[ast.Identifier]] = rule("an identifier") {
    optional(Identifier)
  }

  private def MaybeNodeLabels : Rule1[Seq[ast.Identifier]] = rule("node labels") {
    (WS ~ NodeLabels | EMPTY ~ push(Seq()))
  }

  private def MaybeProperties : Rule1[Option[ast.Expression]] = rule("a property map") (
    optional(WS ~ (MapLiteral | Parameter))
  )

  private def toNodePattern(
      name: Option[ast.Identifier],
      labels: Seq[ast.Identifier],
      params: Option[ast.Expression],
      naked: Boolean,
      token: InputToken) = name match {
    case Some(i) => ast.NamedNodePattern(i, labels, params, naked, token)
    case None => ast.AnonymousNodePattern(labels, params, naked, token)
  }

  private def toRelationshipPattern(
      name: Option[ast.Identifier],
      optional: Boolean,
      types: Seq[ast.Identifier],
      length: Option[Option[ast.Range]],
      params: Option[ast.Expression],
      direction: Direction,
      token: InputToken) = name match {
    case Some(n) => new ast.NamedRelationshipPattern(n, direction, types, length, optional, params, token)
    case None => new ast.AnonymousRelationshipPattern(direction, types, length, optional, params, token)
  }
}
