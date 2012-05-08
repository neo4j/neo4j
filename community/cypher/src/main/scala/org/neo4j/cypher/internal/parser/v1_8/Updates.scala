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
import org.neo4j.graphdb.Direction
import org.neo4j.cypher.internal.commands.{Expression, Entity, Property}

trait Updates extends Base with Expressions with StartClause {
  def updates: Parser[Seq[UpdateAction]] = rep(delete | set | foreach | relate) ^^ (cmds => cmds.flatten)

  def foreach: Parser[Seq[UpdateAction]] = ignoreCase("foreach") ~> "(" ~> identity ~ ignoreCase("in") ~ expression ~ ":" ~ opt(createStart) ~ opt(updates) <~ ")" ^^ {
    case id ~ in ~ iterable ~ ":" ~ creates ~ innerUpdates => {
      val createCmds = creates.toSeq.map(_.startItems.map(_.asInstanceOf[UpdateAction])).flatten
      val updateCmds = innerUpdates.toSeq.flatten
      List(ForeachAction(iterable, id, createCmds ++ updateCmds))
    }
  }

  def delete: Parser[Seq[UpdateAction]] = ignoreCase("delete") ~> comaList(expression) ^^ {
    case expressions => expressions.map {
      case Property(entity, property) => DeletePropertyAction(Entity(entity), property)
      case x => DeleteEntityAction(x)
    }
  }

  def set: Parser[Seq[UpdateAction]] = ignoreCase("set") ~> comaList(propertySet)

  def relate: Parser[Seq[UpdateAction]] = ignoreCase("relate") ~> node ~ rep1(tail) ^^ {
    case head ~ tails => {
      var start = head
      val links = tails.map {
        case l ~ "-[" ~ rel ~ ":" ~ typ ~ properties ~ "]-" ~ r ~ end => {
          val t = RelateLink(start, end, (namer.name(rel), properties), typ, direction(l, r))
          start = end
          t
        }
      }

      Seq(RelateAction(links:_*))
    }
  }

  private def props = opt(properties) ^^ {
    case None => Map[String, Expression]()
    case Some(x) => x
  }

  private def node: Parser[(String, Map[String, Expression])] =
    identity ^^ (name => (name, Map[String, Expression]())) |
      parens(opt(identity) ~ props) ^^ { case id ~ props => (namer.name(id), props) }


  private def tail = opt("<") ~ "-[" ~ opt(identity) ~ ":" ~ identity ~ props ~ "]-" ~ opt(">") ~ node

  private def direction(l: Option[String], r: Option[String]): Direction = (l, r) match {
    case (None, Some(_)) => Direction.OUTGOING
    case (Some(_), None) => Direction.INCOMING
    case _ => Direction.BOTH

  }

  def propertySet = property ~ "=" ~ expression ^^ {
    case p ~ "=" ~ e => PropertySetAction(p.asInstanceOf[Property], e)
  }
}