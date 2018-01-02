/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.{Equals, HasLabel}
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compiler.v2_3.mutation.{SetAction, MergeNodeAction, MergePatternAction, PropertySetAction, UpdateAction}
import org.neo4j.cypher.internal.frontend.v2_3.PatternException

case class MergeAst(patterns: Seq[AbstractPattern],
                    onActions: Seq[OnAction],
                    matches: Seq[Pattern],
                    create: Seq[UpdateAction]) {

  def nextStep(): Seq[UpdateAction] = singleNodeActions ++ getPatternMerges

  def singleNodeActions: Seq[MergeNodeAction] = patterns.collect {
    case entity: ParsedEntity                          => getSingleNodeAction(entity)
    case ParsedNamedPath(_, Seq(entity: ParsedEntity)) => getSingleNodeAction(entity)
  }

  private def getSingleNodeAction(entity:ParsedEntity) = {
    val ParsedEntity(name, _, props, labelTokens) = entity

    val labelPredicates = labelTokens.map(labelName => HasLabel(Identifier(name), labelName))

    val propertyPredicates = props.map {
      case (propertyKey, expression) => Equals(Property(Identifier(name), PropertyKey(propertyKey)), expression)
    }

    val propertyMap: Map[KeyToken, Expression] = props.collect {
      case (propertyKey, expression) => PropertyKey(propertyKey) -> expression
    }.toMap

    val labelActions = labelTokens.map(labelName => LabelAction(Identifier(name), LabelSetOp, Seq(labelName)))
    val propertyActions = props.map {
      case (propertyKey, expression) => {
        if (propertyKey == "*") throw new PatternException("MERGE does not support map parameters")
        PropertySetAction(Property(Identifier(name), PropertyKey(propertyKey)), expression)
      }
    }

    val onCreate: Seq[UpdateAction] = labelActions ++ propertyActions ++ getActionsFor(On.Create)
    val predicates = labelPredicates ++ propertyPredicates

    MergeNodeAction(name, propertyMap, labelTokens, predicates, onCreate, getActionsFor(On.Match), None)

  }

  private def getPatternMerges =
    if (!matches.exists(_.isInstanceOf[RelatedTo]))
      None
    else
      Some(MergePatternAction(matches, create, getActionsFor(On.Create), getActionsFor(On.Match)))

  private def getActionsFor(action:Action): Seq[SetAction] = onActions.collect {
    case p if p.verb == action => p.set
  }.flatten

}


