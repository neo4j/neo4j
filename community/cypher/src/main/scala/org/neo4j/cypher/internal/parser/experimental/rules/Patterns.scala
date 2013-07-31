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
package org.neo4j.cypher.internal.parser.experimental.rules

/*
 *|                                                 Pattern                                                |
 *|Identifier|                                         PathPattern                                         |
 *                           |                                 PatternElement                              |
 *                           |               PatternElement               |        RelationshipChain       |
 *                           |NodePattern |        RelationshipChain      ||RelationshipPattern|NodePattern|
 *                                        |RelationshipPattern|NodePattern|
 *    p =      shortestPath(    (a)             -[r1]->           (b)            -[r2]->           (c)       )
 */

import org.neo4j.cypher.internal.parser.experimental.InputToken
import org.neo4j.cypher.internal.parser.experimental.ast
import org.parboiled.scala._
import org.neo4j.graphdb.Direction

trait Patterns extends Parser
  with Literals
  with Base {

  def Pattern : Rule1[ast.Pattern] = rule("a pattern") (
      group(Identifier ~~ operator("=") ~~ PathPattern) ~>> token ~~> ast.NamedPattern
    | PathPattern ~~> ast.AnonymousPattern
  )

  private def PathPattern : Rule1[ast.PathPattern] = rule (
      group(keyword("shortestPath") ~~ "(" ~~ PatternElement ~~ ")") ~>> token ~~> ast.ShortestPath
    | group(keyword("allShortestPaths") ~~ "(" ~~ PatternElement ~~ ")") ~>> token ~~> ast.AllShortestPaths
    | PatternElement ~~> ast.EveryPath
  )

  def RelationshipsPattern : Rule1[ast.RelationshipsPattern] = rule {
    group(NodePattern ~ oneOrMore(WS ~ PatternElementChain)) ~>> token ~~> ast.RelationshipsPattern
  }

  private def PatternElement : Rule1[ast.PatternElement] = rule (
      NodePattern ~ zeroOrMore(WS ~ PatternElementChain)
    | "(" ~~ PatternElement ~~ ")"
  )

  private def PatternElementChain : ReductionRule1[ast.PatternElement, ast.PatternElement] = rule("a relationship pattern") {
    group(RelationshipPattern ~~ NodePattern) ~>> token ~~> ast.RelationshipChain
  }

  private def RelationshipPattern : Rule1[ast.RelationshipPattern] = rule {
    (
        "<" ~~ "-" ~~ RelationshipDetail ~~ "-" ~~ ">" ~ push(Direction.BOTH)
      | "<" ~~ "-" ~~ RelationshipDetail ~~ "-" ~ push(Direction.INCOMING)
      | "-" ~~ RelationshipDetail ~~ "-" ~~ ">" ~ push(Direction.OUTGOING)
      | "-" ~~ RelationshipDetail ~~ "-" ~ push(Direction.BOTH)
    ) ~>> token ~~> toRelationshipPattern
  }

  private def RelationshipDetail : Rule5[
      Option[ast.Identifier],
      Boolean,
      Seq[ast.Identifier],
      Option[Option[ast.Range]],
      Option[ast.Expression]] = rule {
    (
        "[" ~~
          MaybeIdentifier ~~
          ("?" ~ push(true) | EMPTY ~ push(false)) ~~
          RelationshipTypes ~~ MaybeVariableLength ~~
          MaybeProperties ~~
        "]"
      | EMPTY ~ push(None) ~ push(false) ~ push(Seq()) ~ push(None) ~ push(None)
    )
  }

  private def RelationshipTypes : Rule1[Seq[ast.Identifier]] = rule("relationship types") (
      ":" ~~ oneOrMore(Identifier, separator = (WS ~ "|" ~~ optional(":") ~ WS))
    | EMPTY ~ push(Seq())
  )

  private def MaybeVariableLength : Rule1[Option[Option[ast.Range]]] = rule("length specification") (
      "*" ~~ (
          RangeLiteral ~~> (r => Some(Some(r)))
        | EMPTY ~ push(Some(None))
      )
    | EMPTY ~ push(None)
  )

  private def NodePattern : Rule1[ast.NodePattern] = rule("a node pattern") (
      group("(" ~~ MaybeIdentifier ~~ MaybeNodeLabels ~~ MaybeProperties ~~ ")") ~~> t(toNodePattern _)
    | group(Identifier ~~ MaybeNodeLabels ~~ MaybeProperties) ~~> t(ast.NamedNodePattern(_, _, _, _))
  )

  private def MaybeIdentifier : Rule1[Option[ast.Identifier]] = rule("an identifier") {
    (Identifier ~~> (Some(_)) | EMPTY ~ push(None))
  }

  private def MaybeNodeLabels : Rule1[Seq[ast.Identifier]] = rule("node labels") {
    (NodeLabels | EMPTY ~ push(Seq()))
  }

  private def MaybeProperties : Rule1[Option[ast.Expression]] = rule("property map") (
      MapLiteral ~~> (Some(_))
    | Parameter ~~> (Some(_))
    | EMPTY ~ push(None)
  )

  private def toNodePattern(
      name: Option[ast.Identifier],
      labels: Seq[ast.Identifier],
      params: Option[ast.Expression],
      token: InputToken) = name match {
    case Some(i) => ast.NamedNodePattern(i, labels, params, token)
    case None => ast.AnonymousNodePattern(labels, params, token)
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
