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

import org.neo4j.cypher.internal.mutation._
import org.neo4j.cypher.internal.commands.{True, Entity, Property}

trait Updates extends Base with Expressions with StartClause {
  def updates: Parser[Seq[UpdateAction]] = rep(delete | set | foreach | relate) ^^ (cmds => cmds.flatten)

  def foreach: Parser[Seq[UpdateAction]] = ignoreCase("foreach") ~> "(" ~> identity ~ ignoreCase("in") ~ expression ~ ":" ~ opt(createStart) ~ opt(updates) <~ ")" ^^ {
    case id ~ in ~ iterable ~ ":" ~ creates ~ innerUpdates => {
      val createCmds = creates.toSeq.map(_.startItems.map(_.asInstanceOf[UpdateAction])).flatten
      val updateCmds = innerUpdates.toSeq.flatten
      List(ForeachAction(iterable, id, createCmds ++ updateCmds))
    }
  }

  def delete: Parser[Seq[UpdateAction]] = ignoreCase("delete") ~> commaList(expression) ^^ {
    case expressions => expressions.map {
      case Property(entity, property) => DeletePropertyAction(Entity(entity), property)
      case x => DeleteEntityAction(x)
    }
  }

  private def translate(abstractPattern: AbstractPattern): Maybe[RelateLink] = abstractPattern match {
    case ParsedRelation(name, props, ParsedEntity(Entity(startName), startProps, True()), ParsedEntity(Entity(endName), endProps, True()), typ, dir, map, True()) if typ.size == 1 => Yes(RelateLink(
      start = NamedExpectation(startName, startProps),
      end = NamedExpectation(endName, endProps),
      rel = NamedExpectation(name, props),
      relType = typ.head,
      dir = dir
    ))
    case _ => No("")
  }

  def set: Parser[Seq[UpdateAction]] = ignoreCase("set") ~> commaList(propertySet)

  def relate: Parser[Seq[UpdateAction]] = ignoreCase("relate") ~> usePattern(translate) ^^ (links => Seq(RelateAction(links: _*)))

  def propertySet = property ~ "=" ~ expression ^^ {
    case p ~ "=" ~ e => PropertySetAction(p.asInstanceOf[Property], e)
  }
}