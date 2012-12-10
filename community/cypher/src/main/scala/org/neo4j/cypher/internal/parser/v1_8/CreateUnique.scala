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
import expressions.Identifier
import org.neo4j.cypher.internal.mutation.UniqueLink
import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.mutation.CreateUniqueAction
import org.neo4j.cypher.internal.mutation.NamedExpectation
import org.neo4j.cypher.internal.commands.True


trait CreateUnique extends Base with ParserPattern {
  case class PathAndRelateLink(path:Option[NamedPath], links:Seq[UniqueLink])

  def relate: Parser[(Seq[StartItem], Seq[NamedPath])] = ignoreCase("create unique") ~> usePattern(translate) ^^ (patterns => {
    val (links, path)= reduce(patterns.map {
      case PathAndRelateLink(p, l) => (l, p.toSeq)
    })

    (Seq(CreateUniqueStartItem(CreateUniqueAction(links:_*))), path)
  })

  private def translate(abstractPattern: AbstractPattern): Maybe[PathAndRelateLink] = abstractPattern match {
    case ParsedNamedPath(name, patterns) =>
      val namedPathPatterns = patterns.map(matchTranslator).reduce(_ ++ _)
      val startItems = patterns.map(translate).reduce(_ ++ _)

      startItems match {
        case No(msg) => No(msg)
        case Yes(links) => namedPathPatterns.seqMap(p => {
          val namedPath = NamedPath(name, p.map(_.asInstanceOf[Pattern]): _*)
          Seq(PathAndRelateLink(Some(namedPath), links.flatMap(_.links)))
        })
      }

    case ParsedRelation(name, props, ParsedEntity(Identifier(startName), startProps, True()),
    ParsedEntity(Identifier(endName), endProps, True()), typ, dir, map, True()) if typ.size == 1 =>
      val link = UniqueLink(
        start = NamedExpectation(startName, startProps),
        end = NamedExpectation(endName, endProps),
        rel = NamedExpectation(name, props),
        relType = typ.head,
        dir = dir
      )

      Yes(Seq(PathAndRelateLink(None, Seq(link))))
    case _ => No(Seq())
  }

  def matchTranslator(abstractPattern: AbstractPattern): Maybe[Any]
}
