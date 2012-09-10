/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.parser.v1_9

import org.neo4j.cypher.internal.commands._
import expressions.{Identifier, Expression}

trait MatchClause extends Base with ParserPattern {
  def matching: Parser[(Seq[Pattern], Seq[NamedPath])] = ignoreCase("match") ~> usePattern(matchTranslator) ^^ {
    case matching =>
      val namedPaths = matching.filter(_.isInstanceOf[NamedPath]).map(_.asInstanceOf[NamedPath])
      val unnamedPaths = matching.filter(_.isInstanceOf[List[Pattern]]).map(_.asInstanceOf[List[Pattern]]).flatten ++ matching.filter(_.isInstanceOf[Pattern]).map(_.asInstanceOf[Pattern])

      (unnamedPaths, namedPaths)
  }

  private def successIfEntities[T](l: Expression, r: Expression)(f: (String, String) => T): Maybe[T] = (l, r) match {
    case (Identifier(lName), Identifier(rName)) => Yes(Seq(f(lName, rName)))
    case (x, Identifier(_)) => No(Seq("MATCH end points have to be node identifiers - found: " + x))
    case (Identifier(_), x) => No(Seq("MATCH end points have to be node identifiers - found: " + x))
    case (x, y) => No(Seq("MATCH end points have to be node identifiers - found: " + x + " and " + y))
  }

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any] = abstractPattern match {
    case ParsedNamedPath(name, patterns) => parsedPath(name, patterns)

    case ParsedRelation(name, props, ParsedEntity(left, startProps, True()), ParsedEntity(right, endProps, True()), relType, dir, optional, predicate) =>
      if (props.isEmpty && startProps.isEmpty && endProps.isEmpty)
        successIfEntities(left, right)((l, r) => RelatedTo(left = l, right = r, relName = name, relTypes = relType, direction = dir, optional = optional, predicate = True()))
      else
        No(Seq("Properties on pattern elements are not allowed in MATCH"))

    case ParsedVarLengthRelation(name, props, ParsedEntity(left, startProps, True()), ParsedEntity(right, endProps, True()), relType, dir, optional, predicate, min, max, relIterator) =>
      if (props.isEmpty && startProps.isEmpty && endProps.isEmpty)
        successIfEntities(left, right)((l, r) => RelatedTo(left = l, right = r, relName = name, relTypes = relType, direction = dir, optional = optional, predicate = True()))
      else
        No(Seq("Properties on pattern elements are not allowed in MATCH"))
      successIfEntities(left, right)((l, r) => VarLengthRelatedTo(pathName = name, start = l, end = r, minHops = min, maxHops = max, relTypes = relType, direction = dir, relIterator = relIterator, optional = optional, predicate = predicate))

    case ParsedShortestPath(name, props, ParsedEntity(left, startProps, True()), ParsedEntity(right, endProps, True()), relType, dir, optional, predicate, max, single, relIterator) =>
      if (props.isEmpty && startProps.isEmpty && endProps.isEmpty)
        successIfEntities(left, right)((l, r) => RelatedTo(left = l, right = r, relName = name, relTypes = relType, direction = dir, optional = optional, predicate = True()))
      else
        No(Seq("Properties on pattern elements are not allowed in MATCH"))
      successIfEntities(left, right)((l, r) => ShortestPath(pathName = name, start = l, end = r, relTypes = relType, dir = dir, maxDepth = max, optional = optional, single = single, relIterator = relIterator, predicate = predicate))

    case x => No(Seq("failed to parse MATCH pattern"))
  }

  private def parsedPath(name: String, patterns: Seq[AbstractPattern]): Maybe[NamedPath] = {
    val namedPathPatterns = patterns.map(matchTranslator)
    val result = namedPathPatterns.reduce(_ ++ _)
    result.seqMap(p => Seq(NamedPath(name, p.map(_.asInstanceOf[Pattern]): _*)))
  }
}
