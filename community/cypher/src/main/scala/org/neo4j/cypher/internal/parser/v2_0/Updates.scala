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
import expressions.{Expression, Collection, Property, Identifier}
import org.neo4j.cypher.SyntaxException

trait Updates extends Base with Expressions with StartAndCreateClause {
  def updates: Parser[(Seq[UpdateAction], Seq[NamedPath])] =
    rep(foreach | set | remove | delete) ^^ { x => (x.flatten, Seq.empty) }

  private def foreach: Parser[Seq[UpdateAction]] = FOREACH ~> parens( identity ~ IN ~ expression ~ ":" ~ opt(createStart) ~ opt(updates) ) ^^ {
    case id ~ in ~ collection ~ ":" ~ creates ~ innerUpdates =>
      val createCmds = creates.toSeq.map(_._1.map(_.asInstanceOf[UpdatingStartItem].updateAction)).flatten
      val reducedItems: (Seq[UpdateAction], Seq[NamedPath]) = reduce(innerUpdates.toSeq)
      val updateCmds = reducedItems._1
      val namedPaths = reducedItems._2  ++ creates.toSeq.flatMap(_._2)
      if(namedPaths.nonEmpty) throw new SyntaxException("Paths can't be created inside of foreach")
      Seq(ForeachAction(collection, id, createCmds ++ updateCmds))
  }

  private def set: Parser[Seq[UpdateAction]] = {
    def mapSetter : Parser[UpdateAction] = SET ~> expression ~ "=" ~ expression ^^ {
      case element ~ "=" ~ map => MapPropertySetAction(element, map)
    }

    def singleSetter: Parser[Seq[UpdateAction]] = {
      def labelSet: Parser[UpdateAction] = labelAction(LabelSetOp)

      def propertySet: Parser[UpdateAction] = property ~ "=" ~ exprOrPred ^^ {
        case p ~ "=" ~ e => PropertySetAction(p.asInstanceOf[Property], e)
      }

      SET ~> commaList(propertySet|labelSet)
    }

    singleSetter | liftToSeq(mapSetter)
  }

  private def remove: Parser[Seq[UpdateAction]] =
    // order is important as n:foo doesn't parse as expression nicely
    REMOVE ~> commaList(labelAction(LabelRemoveOp)|mapExpression(propertyRemover)|failure("node labelling or property expected"))

  private def delete: Parser[Seq[UpdateAction]] =
    DELETE ~> commaList(mapExpression(entityRemover))


  private def mapExpression(pf: PartialFunction[Expression, UpdateAction]): Parser[UpdateAction] = expression ^^ pf

  private def propertyRemover: PartialFunction[Expression, UpdateAction] = {
    case Property(entity, property) => DeletePropertyAction(entity, property)
  }

  private def entityRemover: PartialFunction[Expression, UpdateAction] = {
    case x => DeleteEntityAction(x)
  }

  private def labelAction(verb: LabelOp): Parser[UpdateAction] = identity ~ labelShortForm ^^ {
      case entity ~ labels =>
        LabelAction(Identifier(entity), verb, labels.labelVals)
    }
}