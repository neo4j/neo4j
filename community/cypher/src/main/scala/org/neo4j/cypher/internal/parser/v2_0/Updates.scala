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

import org.neo4j.cypher.internal.mutation._
import org.neo4j.cypher.internal.commands._
import expressions.{Literal, Property, Identifier}
import org.neo4j.cypher.SyntaxException

trait Updates extends Base with Expressions with StartClause {
  def updates: Parser[(Seq[UpdateAction], Seq[NamedPath])] =
    rep(foreach | liftToSeq(label)  | set | delete) ^^ { x => (x.flatten, Seq.empty) }

  private def foreach: Parser[Seq[UpdateAction]] = ignoreCase("foreach") ~> "(" ~> identity ~ ignoreCase("in") ~ expression ~ ":" ~ opt(createStart) ~ opt(updates) <~ ")" ^^ {
    case id ~ in ~ collection ~ ":" ~ creates ~ innerUpdates =>
      val createCmds = creates.toSeq.map(_._1.map(_.asInstanceOf[UpdatingStartItem].updateAction)).flatten
      val reducedItems: (Seq[UpdateAction], Seq[NamedPath]) = reduce(innerUpdates.toSeq)
      val updateCmds = reducedItems._1
      val namedPaths = reducedItems._2  ++ creates.toSeq.flatMap(_._2)
      if(namedPaths.nonEmpty) throw new SyntaxException("Paths can't be created inside of foreach")
      Seq(ForeachAction(collection, id, createCmds ++ updateCmds))
  }

  private def label: Parser[UpdateAction] = ignoreCase("label") ~> identity ~ labelOp ~ expression  ^^ {
    case entity ~ op ~ labelSetExpr =>
      LabelAction(Identifier(entity), op, labelSetExpr)
  }

  private def delete: Parser[Seq[UpdateAction]] = ignoreCase("delete") ~> commaList(expression) ^^ {
    case expressions => val updateActions: List[UpdateAction with Product] = expressions.map {
      case Property(entity, property) => DeletePropertyAction(entity, property)
      case x => DeleteEntityAction(x)
    }

    updateActions
  }

  private def set: Parser[Seq[UpdateAction]] = {
    def setToMap : Parser[UpdateAction] = ignoreCase("set") ~> expression ~ "=" ~ expression ^^ {
      case element ~ "=" ~ map => MapPropertySetAction(element, map)
    }

    def setSingleProperty: Parser[Seq[UpdateAction]] = {
      def propertySet = property ~ "=" ~ expressionOrPredicate ^^ {
        case p ~ "=" ~ e => PropertySetAction(p.asInstanceOf[Property], e)
      }

      ignoreCase("set") ~> commaList(propertySet)
    }

    setSingleProperty | liftToSeq(setToMap)
  }
}