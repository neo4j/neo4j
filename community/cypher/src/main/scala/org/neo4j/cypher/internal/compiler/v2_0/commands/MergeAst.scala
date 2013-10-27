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
package org.neo4j.cypher.internal.compiler.v2_0.commands

import expressions._
import values.KeyToken
import values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_0._
import mutation.{UpdateAction, PropertySetAction, MergeNodeAction}
import org.neo4j.cypher.PatternException
import scala.collection.mutable

case class MergeAst(patterns: Seq[AbstractPattern], onActions: Seq[OnAction]) {
  def nextStep(): Seq[MergeNodeAction] = {

    val actionsMap = new mutable.HashMap[(String, Action), mutable.Set[UpdateAction]] with mutable.MultiMap[(String, Action), UpdateAction]

    for (
      actions <- onActions;
      action <- actions.set) {
      actionsMap.addBinding((actions.identifier, actions.verb), action)
    }

    patterns.map {
      case ParsedEntity(name, _, props, labelTokens, _) =>

        val labelPredicates = labelTokens.map(labelName => HasLabel(Identifier(name), labelName))

        val propertyPredicates = props.map {
          case (propertyKey, expression) => Equals(Property(Identifier(name), PropertyKey(propertyKey)), expression)
        }

        val propertyMap: Map[KeyToken, Expression] = props.collect {
          case (propertyKey, expression) => PropertyKey(propertyKey) -> expression
        }.toMap

        val predicates = labelPredicates ++ propertyPredicates

        val labelActions = labelTokens.map(labelName => LabelAction(Identifier(name), LabelSetOp, Seq(labelName)))
        val propertyActions = props.map {
          case (propertyKey, expression) => {
            if (propertyKey == "*") throw new PatternException("MERGE does not support map parameters")
            PropertySetAction(Property(Identifier(name), PropertyKey(propertyKey)), expression)
          }
        }

        val actionsFromOnCreateClause = actionsMap.get((name, On.Create)).getOrElse(Set.empty)
        val actionsFromOnMatchClause = actionsMap.get((name, On.Match)).getOrElse(Set.empty)

        val onCreate: Seq[UpdateAction] = labelActions ++ propertyActions ++ actionsFromOnCreateClause

        MergeNodeAction(name, propertyMap, labelTokens, predicates, onCreate, actionsFromOnMatchClause.toSeq, None)

      case _ =>
        throw new PatternException("MERGE only supports single node patterns")
    }
  }
}


