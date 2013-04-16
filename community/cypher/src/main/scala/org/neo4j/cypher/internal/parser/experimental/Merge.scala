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
package org.neo4j.cypher.internal.parser.experimental

import org.neo4j.cypher.internal.commands._
import expressions.{Property, Identifier}
import org.neo4j.cypher.internal.mutation.{PropertySetAction, UpdateAction, MergeNodeAction}
import scala.Predef._
import org.neo4j.cypher.internal.commands.NamedPath
import org.neo4j.cypher.internal.commands.MergeNodeStartItem
import org.neo4j.cypher.internal.commands.LabelAction
import org.neo4j.cypher.internal.commands.HasLabel


trait Merge extends Base with Labels with ParserPattern {

  def merge: Parser[(Seq[StartItem], Seq[NamedPath])] = rep1(mergeNode) ~ rep(onCreate | onUpdate) ^^ {
    case nodes ~ actions =>

      // Prepares maps with update and create actions
      var updateActions: Map[String, Seq[UpdateAction]] = Map.empty
      var createActions: Map[String, Seq[UpdateAction]] = nodes.map(action => action.identifier -> action.onCreate).toMap

      // Adds ON CREATE / ON MATCH actions to maps
      actions.foreach {
        case (OnCreate, id, theseActions) => createActions = add(theseActions, createActions, id)
        case (OnMatch, id, theseActions)  => updateActions = add(theseActions, updateActions, id)
      }

      // Creates StartItems with the action maps
      val startItems = nodes.map {
        case nodeStartAction => MergeNodeStartItem(nodeStartAction.copy(
          onCreate = createActions(nodeStartAction.identifier),
          onMatch = updateActions.getOrElse(nodeStartAction.identifier, Seq.empty)))
      }

      (startItems, Seq.empty)
  }

  private def add(actions: Seq[UpdateAction], map: Map[String, Seq[UpdateAction]], id: String): Map[String, Seq[UpdateAction]] =
    map + (map.get(id) match {
      case Some(existingActions) => (id -> (actions ++ existingActions))
      case None                  => (id -> actions)
    })

  private def mergeNode: Parser[MergeNodeAction] = MERGE ~> optParens(identity ~ optLabelShortForm ~ opt(curlyMap)) ^^ {
    case id ~ labels ~ propsOption =>
      val props = propsOption.getOrElse(Map.empty)
      val expectations = labels.map(HasLabel(Identifier(id),_)) ++ props.map {
        case (key, value) => Equals(Property(Identifier(id), key), value)
      }
      val onCreate = labels.map(l => LabelAction(Identifier(id), LabelSetOp, Seq(l))) ++ props.map {
        case (key, value) => PropertySetAction(Property(Identifier(id), key), value)
      }
      MergeNodeAction(id, expectations, onCreate, Seq.empty, None)
  }

  private def onCreate: Parser[(OnAction, String, Seq[UpdateAction])] = ON ~> CREATE ~> identity ~ set ^^ {
    case id ~ setActions => (OnCreate, id, setActions)
  }

  private def onUpdate: Parser[(OnAction, String, Seq[UpdateAction])] = ON ~> MATCH ~> identity ~ set ^^ {
    case id ~ setActions => (OnMatch, id, setActions)
  }

  def set: Parser[Seq[UpdateAction]]

  trait OnAction

  case object OnCreate extends OnAction

  case object OnMatch extends OnAction

}