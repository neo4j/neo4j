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
package org.neo4j.cypher.internal.parser.v1_8

import org.neo4j.cypher.internal.commands._

trait MatchClause extends Base with ParserPattern {
  def matching: Parser[(Match, NamedPaths)] = ignoreCase("match") ~> usePattern(matchTranslator) ^^ {
    case matching =>
      val namedPaths = matching.filter(_.isInstanceOf[NamedPath]).map(_.asInstanceOf[NamedPath])
      val unnamedPaths = matching.filter(_.isInstanceOf[List[Pattern]]).map(_.asInstanceOf[List[Pattern]]).flatten ++ matching.filter(_.isInstanceOf[Pattern]).map(_.asInstanceOf[Pattern])

      (Match(unnamedPaths: _*), NamedPaths(namedPaths: _*))
  }

  private def successIfEntities[T](l: Expression, r: Expression)(f: (String, String) => T): Maybe[T] = (l, r) match {
    case (Entity(lName), Entity(rName)) => Yes(f(lName, rName))
    case (x, Entity(_)) => No("MATCH end points have to be node identifiers - found: " + x)
    case (Entity(_), x) => No("MATCH end points have to be node identifiers - found: " + x)
    case (x, y) => No("MATCH end points have to be node identifiers - found: " + x + " and " + y)
  }

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any] = abstractPattern match {
    case ParsedNamedPath(name, patterns) =>
      val namedPathPatterns = patterns.map(matchTranslator)

      val find = namedPathPatterns.find(!_.success)
      find match {
        case None => Yes(NamedPath(name, namedPathPatterns.map(_.value.asInstanceOf[Pattern]): _*))
        case Some(No(msg)) => No(msg)
      }

    case ParsedRelation(name, props, ParsedEntity(left, startProps, True()), ParsedEntity(right, endProps, True()), relType, dir, optional, predicate) =>
      successIfEntities(left, right)((l, r) => RelatedTo(left = l, right = r, relName = name, relTypes = relType, direction = dir, optional = optional, predicate = True()))

    case ParsedVarLengthRelation(name, props, ParsedEntity(left, startProps, True()), ParsedEntity(right, endProps, True()), relType, dir, optional, predicate, min, max, relIterator) =>
      successIfEntities(left, right)((l, r) => VarLengthRelatedTo(pathName = name, start = l, end = r, minHops = min, maxHops = max, relTypes = relType, direction = dir, relIterator = relIterator, optional = optional, predicate = predicate))

    case ParsedShortestPath(name, props, ParsedEntity(left, startProps, True()), ParsedEntity(right, endProps, True()), relType, dir, optional, predicate, max, single, relIterator) =>
      successIfEntities(left, right)((l, r) => ShortestPath(pathName = name, start = l, end = r, relTypes = relType, dir = dir, maxDepth = max, optional = optional, single = single, relIterator = relIterator, predicate = predicate))

    case x => No("failed to parse MATCH pattern")
  }
}
