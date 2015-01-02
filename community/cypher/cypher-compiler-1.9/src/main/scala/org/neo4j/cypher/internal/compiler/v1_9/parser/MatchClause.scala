/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v1_9.parser

import org.neo4j.cypher.internal.compiler.v1_9.commands._
import expressions.{Identifier, Expression}
import collection.Map
import org.neo4j.cypher.internal.helpers.CastSupport.sift

trait MatchClause extends Base with ParserPattern {

  def matching: Parser[(Seq[Pattern], Seq[NamedPath])] = ignoreCase("match") ~> usePattern(matchTranslator) ^^ {
    case matches =>
      val namedPaths = sift[NamedPath](matches)
      val patterns = sift[List[Pattern]](matches).flatten ++ sift[Pattern](matches) ++ namedPaths.flatMap(_.pathPattern)

      (patterns.distinct, namedPaths)
  }

  type TransformType = (ParsedEntity, ParsedEntity, Map[String, Expression], (String, String) => Pattern) => Maybe[Pattern]

  private def successIfIdentifiers[T](left: ParsedEntity,
                                      right: ParsedEntity,
                                      relProps: Map[String, Expression],
                                      f: (String, String) => T): Maybe[T] = {
    def checkProps(props: Map[String, Expression]): Maybe[T] =
      if (props.nonEmpty)
        No(Seq("Properties on pattern elements are not allowed in MATCH."))
      else
        Yes(Seq())

    def checkExpressions(x: ParsedEntity): Maybe[T] =
      if (x.expression.isInstanceOf[Identifier])
        Yes(Seq())
      else
        No(Seq("MATCH end points have to be node identifiers - found: " + x.expression))

    val props: Maybe[T] = checkProps(left.props) ++ checkProps(right.props) ++ checkProps(relProps)
    val expressions = checkExpressions(left) ++ checkExpressions(right)

    (props ++ expressions).seqMap(s => Seq(f(left.name, right.name)))
  }

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any] =
    matchTranslator(successIfIdentifiers, abstractPattern)

  def matchTranslator(transform: TransformType, abstractPattern: AbstractPattern): Maybe[Any] = {
    val f = matchFunction(transform)
    if (f.isDefinedAt(abstractPattern))
      f(abstractPattern)
    else
      No(Seq("failed to parse MATCH pattern"))
  }

  def matchFunction(transform: TransformType): PartialFunction[AbstractPattern, Maybe[Any]] =
    matchNamePath(transform) orElse
    matchRelation(transform) orElse
    matchVarLengthRelation(transform) orElse
    matchShortestPath(transform) orElse
    matchEntity(transform)

  def matchNamePath(transform: TransformType): PartialFunction[AbstractPattern, Maybe[Any]] = {
    case ParsedNamedPath(name, patterns) =>
      parsedPath(name, patterns, transform)
  }

  def matchRelation(transform: TransformType): PartialFunction[AbstractPattern, Maybe[Any]] = {
    case ParsedRelation(name, props, left, right, relType, dir, optional, predicate) =>
      transform(left, right, props, (l, r) => RelatedTo(left = l, right = r, relName = name, relTypes = relType,
        direction = dir, optional = optional, predicate = True()))
  }

  def matchVarLengthRelation(transform: TransformType): PartialFunction[AbstractPattern, Maybe[Any]] = {
    case ParsedVarLengthRelation(name, props, left, right, relType, dir, optional, predicate, min, max, relIterator) =>
      transform(left, right, props, (l, r) => VarLengthRelatedTo(pathName = name, start = l, end = r, minHops = min,
        maxHops = max, relTypes = relType, direction = dir, relIterator = relIterator, optional = optional,
        predicate = predicate))
  }

  def matchShortestPath(transform: TransformType): PartialFunction[AbstractPattern, Maybe[Any]] = {
    case ParsedShortestPath(name, props, left, right, relType, dir, optional, predicate, max, single, relIterator) =>
      transform(left, right, props, (l, r) => ShortestPath(pathName = name, start = l, end = r, relTypes = relType,
        dir = dir, maxDepth = max, optional = optional, single = single, relIterator = relIterator,
        predicate = predicate))

  }

  def matchEntity(transform: TransformType): PartialFunction[AbstractPattern, Maybe[Any]] = {
    case ParsedEntity(name, _, _, _) =>
      Yes(Seq(SingleNode(name)))
  }

  private def parsedPath(name: String, patterns: Seq[AbstractPattern], transform: TransformType): Maybe[NamedPath] = {
    val namedPathPatterns = patterns.map(matchTranslator(transform, _))
    val result = namedPathPatterns.reduce(_ ++ _)
    result.seqMap(p => Seq(NamedPath(name, p.map(_.asInstanceOf[Pattern]): _*)))
  }
}
