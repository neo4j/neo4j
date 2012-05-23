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
  def matching: Parser[(Match, NamedPaths)] = ignoreCase("match") ~> usePattern(translate) ^^ {
    case matching =>
      val namedPaths = matching.filter(_.isInstanceOf[NamedPath]).map(_.asInstanceOf[NamedPath])
      val unnamedPaths = matching.filter(_.isInstanceOf[List[Pattern]]).map(_.asInstanceOf[List[Pattern]]).flatten ++ matching.filter(_.isInstanceOf[Pattern]).map(_.asInstanceOf[Pattern])

      (Match(unnamedPaths: _*), NamedPaths(namedPaths: _*))
  }

  private def translate(abstractPattern: AbstractPattern): Maybe[Any] = abstractPattern match {
    case ParsedNamedPath(name, patterns) =>
      val namedPathPatterns = patterns.map(translate)

      val find = namedPathPatterns.find(!_.success)
      find match {
        case None => Yes(NamedPath(name, namedPathPatterns.map(_.value.asInstanceOf[Pattern]): _*))
        case Some(No(msg)) => No(msg)
      }

    case ParsedRelation(name, props, ParsedEntity(Entity(left), startProps, True()), ParsedEntity(Entity(right), endProps, True()), relType, dir, optional, predicate) =>
      Yes(RelatedTo(left = left, right = right, relName = name, relTypes = relType, direction = dir, optional = optional, predicate = True()))

    case ParsedVarLengthRelation(name, props, ParsedEntity(Entity(left), startProps, True()), ParsedEntity(Entity(right), endProps, True()), relType, dir, optional, predicate, min, max, relIterator) =>
      Yes(VarLengthRelatedTo(pathName = name, start = left, end = right, minHops = min, maxHops = max, relTypes = relType, direction = dir, relIterator = relIterator, optional = optional, predicate = predicate))

    case ParsedShortestPath(name, props, ParsedEntity(Entity(left), startProps, True()), ParsedEntity(Entity(right), endProps, True()), relType, dir, optional, predicate, max, single, relIterator) =>
      Yes(ShortestPath(pathName = name, start = left, end = right, relTypes = relType, dir = dir, maxDepth = max, optional = optional, single = single, relIterator = relIterator, predicate = predicate))

    case _ => No("")
  }
}
