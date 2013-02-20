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
package org.neo4j.cypher.internal.parser.v2_0

import org.neo4j.cypher.internal.commands._
import expressions.{Literal, Identifier, Expression}
import collection.Map
import org.neo4j.cypher.internal.helpers.CastSupport._
import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.commands.ShortestPath
import org.neo4j.cypher.internal.commands.True
import values.LabelName

trait MatchClause extends Base with ParserPattern {
  def matching: Parser[(Seq[Pattern], Seq[NamedPath], Predicate)] = MATCH ~> usePattern(labelTranslator) ^^ {
    case results: Seq[(Any, Predicate)] =>
      val matches = results.map(_._1)
      val predicates = results.map(_._2)
      val namedPaths = sift[NamedPath](matches)
      val patterns = sift[List[Pattern]](matches).flatten ++ sift[Pattern](matches) ++ namedPaths.flatMap(_.pathPattern)

      (patterns.distinct, namedPaths, True().andWith(predicates:_*))
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

    def checkExpressions(x: ParsedEntity): Maybe[T] = x.expression match {
      case _: Identifier         => Yes(Seq())
      case Literal(_: LabelName) => Yes(Seq())
      case e                     => No(Seq(s"MATCH end points have to be node identifiers - found: $e"))
    }

    val props: Maybe[T] = checkProps(left.props) ++ checkProps(right.props) ++ checkProps(relProps)
    val expressions = checkExpressions(left) ++ checkExpressions(right)

    (props ++ expressions).seqMap(s => Seq(f(left.name, right.name)))
  }

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any] = matchTranslator(successIfIdentifiers, abstractPattern)

  def matchTranslator(transform: TransformType, abstractPattern: AbstractPattern): Maybe[Any] =
    abstractPattern match {
      case ParsedNamedPath(name, patterns) =>
        parsedPath(name, patterns, transform)

      case ParsedRelation(name, props, left, right, relType, dir, optional) =>
        transform(left, right, props, (l, r) =>
          RelatedTo(left = l, right = r, relName = name, relTypes = relType, direction = dir, optional = optional))

      case ParsedVarLengthRelation(name, props, left, right, relType, dir, optional, min, max, relIterator) =>
        transform(left, right, props, (l, r) =>
          VarLengthRelatedTo(pathName = name, start = l, end = r, minHops = min, maxHops = max, relTypes = relType, direction = dir, relIterator = relIterator, optional = optional))

      case ParsedShortestPath(name, props, left, right, relType, dir, optional, max, single, relIterator) =>
        transform(left, right, props, (l, r) =>
          ShortestPath(pathName = name, start = l, end = r, relTypes = relType, dir = dir, maxDepth = max, optional = optional, single = single, relIterator = relIterator))

      case x => No(Seq("failed to parse MATCH pattern"))
  }

  def labelTranslator(abstractPattern: AbstractPattern): Maybe[(Any, Predicate)] =
    matchTranslator(abstractPattern).map { (pat: Any) =>
      val predicates = abstractPattern.parsedLabelPredicates
      (pat,  True().andWith(predicates: _*) )
    }


  private def parsedPath(name: String, patterns: Seq[AbstractPattern], transform: TransformType): Maybe[NamedPath] = {
    val namedPathPatterns = patterns.map(matchTranslator(transform, _))
    val result = namedPathPatterns.reduce(_ ++ _)
    result.seqMap(p => Seq(NamedPath(name, p.map(_.asInstanceOf[Pattern]): _*)))
  }
}
