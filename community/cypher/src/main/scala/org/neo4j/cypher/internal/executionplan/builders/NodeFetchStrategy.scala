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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.commands.expressions.{Literal, IdFunction, Property, Identifier}
import org.neo4j.cypher.internal.commands.AllNodes
import org.neo4j.cypher.internal.commands.SchemaIndex
import org.neo4j.cypher.internal.commands.NodeByLabel
import org.neo4j.cypher.internal.commands.HasLabel

object NodeFetchStrategy {
  type LabelName = String
  type IdentifierName = String
  type PropertyKey = String


  val Single = 0
  val IndexEquality = 1
  val IndexRange = 2
  val IndexScan = 3
  val LabelScan = 4
  val Global = 5

  def findStartStrategy(node: String, where: Seq[QueryToken[Predicate]], ctx: PlanContext):
  StartItemWithRating = {
    val labels: Seq[LabelName] = findLabelsForNode(node, where)
    val propertyPredicates: Seq[PropertyKey] = findEqualityPredicatesOnProperty(node, where)
    val idPredicates: Seq[Long] = findEqualityPredicatesUsingNodeId(node, where)

    val indexSeeks = for (
      label <- labels;
      property <- propertyPredicates
      if (ctx.getIndexRuleId(label, property).nonEmpty)
    ) yield SchemaIndex(node, label, property, None)

    if (idPredicates.nonEmpty) {
      StartItemWithRating(NodeById(node, idPredicates: _*), Single)
    } else if (indexSeeks.nonEmpty) {
      // TODO: Once we have index statistics, we can pick the best one
      StartItemWithRating(indexSeeks.head, IndexEquality)
    } else if (labels.nonEmpty) {
      // TODO: Once we have label statistics, we can pick the best one
      StartItemWithRating(NodeByLabel(node, labels.head), LabelScan)
    } else {
      StartItemWithRating(AllNodes(node), Global)
    }
  }

  private def findLabelsForNode(node: String, where: Seq[QueryToken[Predicate]]): Seq[LabelName] =
    where.collect {
      case Unsolved(HasLabel(Identifier(identifier), labelNames)) if identifier == node => labelNames
    }.
      flatten.
      map(_.name)

  private def findEqualityPredicatesOnProperty(identifier: IdentifierName, where: Seq[QueryToken[Predicate]]): Seq[PropertyKey] =
    where.collect {
      case Unsolved(Equals(Property(Identifier(id), propertyName), expression)) if id == identifier => propertyName
      case Unsolved(Equals(expression, Property(Identifier(id), propertyName))) if id == identifier => propertyName
    }

  private def findEqualityPredicatesUsingNodeId(identifier: IdentifierName, where: Seq[QueryToken[Predicate]]): Seq[Long] =
    where.collect {
      case Unsolved(Equals(IdFunction(Identifier(id)), Literal(idValue)))
        if id == identifier && idValue.isInstanceOf[Number] => idValue.asInstanceOf[Number].longValue()
      case Unsolved(Equals(Literal(idValue), IdFunction(Identifier(id))))
        if id == identifier && idValue.isInstanceOf[Number] => idValue.asInstanceOf[Number].longValue()
    }
}

case class StartItemWithRating(s: StartItem, rating: Integer)