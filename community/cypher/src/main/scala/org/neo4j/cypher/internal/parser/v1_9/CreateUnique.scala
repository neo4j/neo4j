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
package org.neo4j.cypher.internal.parser.v1_9

import org.neo4j.cypher.internal.commands._
import expressions.Expression
import org.neo4j.cypher.internal.mutation.UniqueLink
import org.neo4j.cypher.internal.mutation.NamedExpectation
import collection.Map
import org.neo4j.cypher.internal.parser._
import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.parser.ParsedEntity
import org.neo4j.cypher.internal.mutation.CreateUniqueAction
import org.neo4j.cypher.internal.commands.CreateUniqueStartItem
import org.neo4j.cypher.internal.parser.ParsedNamedPath
import org.neo4j.cypher.internal.parser.ParsedRelation

trait CreateUnique extends Base with ParserPattern {
  case class PathAndRelateLink(path:Option[NamedPath], links:Seq[UniqueLink])

  def relate: Parser[(Seq[StartItem], Seq[NamedPath])] = ignoreCase("create unique") ~> usePattern(translate) ^^
    (patterns => {
      val (links, path) = reduce(patterns.map {
        case PathAndRelateLink(p, l) => (l, p.toSeq)
      })

      (Seq(CreateUniqueStartItem(CreateUniqueAction(links: _*))), path)
    })

  private def translate(abstractPattern: AbstractPattern): Maybe[PathAndRelateLink] = {

    def acceptAll[T](l: ParsedEntity, r: ParsedEntity, props: Map[String, Expression], f: (String, String) => T): Maybe[T] =
      Yes(Seq(f(l.name, r.name)))

    abstractPattern match {
      case ParsedNamedPath(name, patterns) =>
        val namedPathPatterns = patterns.map(matchTranslator(acceptAll, _)).reduce(_ ++ _)
        val startItems = patterns.map(translate).reduce(_ ++ _)

        startItems match {
          case No(msg)    => No(msg)
          case Yes(links) => namedPathPatterns.seqMap(p => {
            val namedPath = NamedPath(name, patterns: _*)
            Seq(PathAndRelateLink(Some(namedPath), links.flatMap(_.links)))
          })
        }

      case ParsedRelation(name, props,
      ParsedEntity(startName, startExp, startProps, _, _),
      ParsedEntity(endName, endExp, endProps, _, _), typ, dir, map) if typ.size == 1 =>
        val link = UniqueLink(
          start = NamedExpectation(startName, startExp, startProps, bare = startProps.isEmpty),
          end = NamedExpectation(endName, endExp, endProps, bare = endProps.isEmpty),
          rel = NamedExpectation(name, props, bare = props.isEmpty),
          relType = typ.head,
          dir = dir
        )

        Yes(Seq(PathAndRelateLink(None, Seq(link))))
      case _                                                                                   => No(Seq())
    }
  }
  def matchTranslator(transform: (ParsedEntity, ParsedEntity, Map[String, Expression], (String, String) => Pattern) => Maybe[Pattern], abstractPattern: AbstractPattern): Maybe[Any]
}
