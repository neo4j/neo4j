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

import org.neo4j.cypher.internal.compiler.v1_9.mutation._
import org.neo4j.cypher.internal.compiler.v1_9.commands._
import expressions.Property
import org.neo4j.cypher.SyntaxException

trait Updates extends Base with Expressions with StartClause {
  def updates: Parser[(Seq[UpdateAction], Seq[NamedPath])] = rep(delete | set | foreach) ^^ (cmds => reduce(cmds))

  def foreach: Parser[(Seq[UpdateAction], Seq[NamedPath])] = ignoreCase("foreach") ~> "(" ~> identity ~ ignoreCase("in") ~ expression ~ (":" | "|") ~ opt(createStart) ~ opt(updates) <~ ")" ^^ {
    case id ~ _ ~ collection ~ _ ~ creates ~ innerUpdates => {
      val createCmds = creates.toSeq.map(_._1.map(_.asInstanceOf[UpdatingStartItem].updateAction)).flatten
      val reducedItems: (Seq[UpdateAction], Seq[NamedPath]) = reduce(innerUpdates.toSeq)
      val updateCmds = reducedItems._1
      val namedPaths = reducedItems._2  ++ creates.toSeq.flatMap(_._2)
      if(namedPaths.nonEmpty) throw new SyntaxException("Paths can't be created inside of foreach")
      (Seq(ForeachAction(collection, id, createCmds ++ updateCmds)), Seq())
    }
  }

  def delete: Parser[(Seq[UpdateAction], Seq[NamedPath])] = ignoreCase("delete") ~> commaList(expression) ^^ {
    case expressions => val updateActions: List[UpdateAction with Product] = expressions.map {
      case Property(entity, property) => DeletePropertyAction(entity, property)
      case x => DeleteEntityAction(x)
    }
      (updateActions, Seq())
  }

  def set: Parser[(Seq[UpdateAction], Seq[NamedPath])] =
    setSingleProperty ^^ ((_, Nil)) |
    setToMap ^^ (x => (Seq(x), Nil))

  def setToMap : Parser[UpdateAction] = ignoreCase("set") ~> expression ~ "=" ~ expression ^^ {
    case element ~ "=" ~ map => MapPropertySetAction(element, map)
  }

  def setSingleProperty: Parser[Seq[UpdateAction]] = ignoreCase("set") ~> commaList(propertySet)

  def propertySet = property ~ "=" ~ expressionOrPredicate ^^ {
    case p ~ "=" ~ e => PropertySetAction(p.asInstanceOf[Property], e)
  }
}
