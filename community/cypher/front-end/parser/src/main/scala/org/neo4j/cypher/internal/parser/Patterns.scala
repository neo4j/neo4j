/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.util.InputPosition
import org.parboiled.scala.EMPTY
import org.parboiled.scala.Parser
import org.parboiled.scala.ReductionRule1
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule5
import org.parboiled.scala.group

import scala.language.postfixOps

/*
 *|                                            NamedPatternPart                                            |
 *|Variable |                                      AnonymousPatternPart                                    |
 *                           |                                 PatternElement                              |
 *                           |               PatternElement               |        RelationshipChain       |
 *                           |NodePattern |        RelationshipChain      ||RelationshipPattern|NodePattern|
 *                                        |RelationshipPattern|NodePattern|
 *    p =      shortestPath(    (a)             -[r1]->           (b)            -[r2]->           (c)       )
 */

trait Patterns extends Parser
  with Literals
  with Base {

  def Pattern: Rule1[expressions.Pattern] = rule("a pattern") {
    oneOrMore(PatternPart, separator = CommaSep) ~~>> (expressions.Pattern(_))
  }

  def PatternPart: Rule1[expressions.PatternPart] = rule("a pattern") (
      group(Variable ~~ operator("=") ~~ AnonymousPatternPart) ~~>> (expressions.NamedPatternPart(_, _))
    | AnonymousPatternPart
  )

  private def AnonymousPatternPart: Rule1[expressions.AnonymousPatternPart] = rule (
      ShortestPathPattern
    | PatternElement ~~> expressions.EveryPath
  )

  def ShortestPathPattern: Rule1[expressions.ShortestPaths] = rule (
      (group(keyword("shortestPath") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~~>> (expressions.ShortestPaths(_, single = true))
    | (group(keyword("allShortestPaths") ~~ "(" ~~ PatternElement ~~ ")") memoMismatches) ~~>> (expressions.ShortestPaths(_, single = false))
  ).memoMismatches

  def RelationshipsPattern: Rule1[expressions.RelationshipsPattern] = rule {
    group(NodePattern ~ oneOrMore(WS ~ PatternElementChain)) ~~>> (expressions.RelationshipsPattern(_))
  }.memoMismatches

  private def PatternElement: Rule1[expressions.PatternElement] = rule (
      NodePattern ~ zeroOrMore(WS ~ PatternElementChain)
    | "(" ~~ PatternElement ~~ ")"
  )

  private def PatternElementChain: ReductionRule1[expressions.PatternElement, expressions.RelationshipChain] = rule("a relationship pattern") {
    group(RelationshipPattern ~~ NodePattern) ~~>> (expressions.RelationshipChain(_, _, _))
  }

  private def RelationshipPattern: Rule1[expressions.RelationshipPattern] = rule {
    (
        LeftArrowHead ~~ Dash ~~ RelationshipDetail ~~ Dash ~~ RightArrowHead ~ push(SemanticDirection.BOTH)
      | LeftArrowHead ~~ Dash ~~ RelationshipDetail ~~ Dash ~ push(SemanticDirection.INCOMING)
      | Dash ~~ RelationshipDetail ~~ Dash ~~ RightArrowHead ~ push(SemanticDirection.OUTGOING)
      | Dash ~~ RelationshipDetail ~~ Dash ~ push(SemanticDirection.BOTH)
    ) ~~>> ((variable, relTypes, range, props, predicate, dir) =>
      expressions.RelationshipPattern(variable, relTypes.types, range, props, predicate, dir, relTypes.legacySeparator))
  }

  private def RelationshipDetail: Rule5[
      Option[expressions.Variable],
      MaybeLegacyRelTypes,
      Option[Option[expressions.Range]],
      Option[expressions.Expression],
      Option[expressions.Expression]] = rule("[") {
    (
        "[" ~~
          MaybeVariable ~~
          RelationshipTypes ~~ MaybeVariableLength ~
          MaybeProperties ~~
          MaybeWhereSubClause ~~
        "]"
      | EMPTY ~ push(None) ~ push(MaybeLegacyRelTypes()) ~ push(None) ~ push(None) ~ push(None)
    )
  }

  private def RelationshipTypes: Rule1[MaybeLegacyRelTypes] = rule("relationship types") (
    (":" ~~ RelTypeName ~~ zeroOrMore(WS ~ "|" ~~ LegacyCompatibleRelTypeName)) ~~>> (
      (first: expressions.RelTypeName, more: List[(Boolean, expressions.RelTypeName)]) => (pos: InputPosition) => {
        MaybeLegacyRelTypes(first +: more.map(_._2), more.exists(_._1))
      })
    | EMPTY ~ push(MaybeLegacyRelTypes())
  )

  private def LegacyCompatibleRelTypeName: Rule1[(Boolean, expressions.RelTypeName)] =
    ((":" ~ push(true)) | EMPTY ~ push(false)) ~~ RelTypeName ~~>> (
      (legacy: Boolean, name: expressions.RelTypeName) => (pos: InputPosition) => (legacy,name))

  private def MaybeVariableLength: Rule1[Option[Option[expressions.Range]]] = rule("a length specification") (
      "*" ~~ (
          RangeLiteral ~~> (r => Some(Some(r)))
        | EMPTY ~ push(Some(None))
      )
    | EMPTY ~ push(None)
  )

  private def NodePattern: Rule1[expressions.NodePattern] = rule("a node pattern") (
    group("(" ~~ Variable ~ MaybeNodeLabels ~ MaybeProperties ~ MaybeWhereSubClause ~~ ")") ~~>>
      { (v, labels, props, where) => expressions.NodePattern(Some(v), labels, None, props, where)}
    | group("(" ~~ MaybeNodeLabels ~ MaybeProperties ~~ ")") ~~>>
      { (labels, props) => expressions.NodePattern(None, labels, None, props, None)}
    | group(Variable ~ MaybeNodeLabels ~ MaybeProperties)  ~~>>
      (expressions.InvalidNodePattern(_, _, _)) // Here to give nice error messages
  )

  private def MaybeVariable: Rule1[Option[expressions.Variable]] = rule("a variable") {
    optional(Variable)
  }

  private def MaybeNodeLabels: Rule1[Seq[expressions.LabelName]] = rule("node labels") (
    WS ~ NodeLabels | EMPTY ~ push(Seq())
  )

  private def MaybeProperties: Rule1[Option[expressions.Expression]] = rule("a property map") (
    optional(WS ~ (MapLiteral | Parameter))
  )

  private def MaybeWhereSubClause: Rule1[Option[expressions.Expression]] = rule("a WHERE subclause") (
    optional(WS ~ keyword("WHERE") ~ WS ~ Expression)
  )
}

case class MaybeLegacyRelTypes(types: Seq[expressions.RelTypeName] = Seq.empty, legacySeparator: Boolean = false)
