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

import org.neo4j.cypher.internal.commands.{Foreach, SetProperty, DeleteEntityCommand, UpdateCommand}


trait Updates extends Base with Expressions {
  def updates:Parser[Seq[UpdateCommand]] = (deleteThenSet | setThenDelete) ~ opt(foreach) ^^ {
    case deleteAndSet ~ foreach => deleteAndSet ++ foreach.toSeq
  }

  def foreach:Parser[UpdateCommand] = ignoreCase("foreach") ~> "(" ~> identity ~ ignoreCase("in") ~ expression ~ ":" ~ updates <~ ")" ^^ {
    case id ~ in ~ iterable ~ ":" ~ innerUpdates => Foreach(iterable, id, innerUpdates)
  }

  def deleteThenSet:Parser[Seq[UpdateCommand]] = opt(delete) ~ opt(set) ^^ {
    case x ~ y => x.toSeq.flatten ++ y.toSeq.flatten
  }

  def setThenDelete:Parser[Seq[UpdateCommand]] = opt(delete) ~ opt(set) ^^ {
    case x ~ y => x.toSeq.flatten ++ y.toSeq.flatten
  }

  def delete: Parser[List[UpdateCommand]] = ignoreCase("delete") ~> comaList(expression) ^^
    (expressions => expressions.map(DeleteEntityCommand(_)))

  def set: Parser[List[UpdateCommand]] = ignoreCase("set") ~> comaList(propertySet)

  def propertySet = property ~ "=" ~ expression ^^ {
    case p ~ "=" ~ e => SetProperty(p, e)
  }
}