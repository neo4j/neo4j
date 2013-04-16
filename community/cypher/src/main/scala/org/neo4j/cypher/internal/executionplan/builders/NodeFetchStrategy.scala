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

//TODO: Make perty?
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
    val labelPredicates: Seq[SolvedPredicate[LabelName]] = findLabelsForNode(node, where)
    val propertyPredicates: Seq[SolvedPredicate[PropertyKey]] = findEqualityPredicatesOnProperty(node, where)
    val idPredicates: Seq[SolvedPredicate[Long]] = findEqualityPredicatesUsingNodeId(node, where)

    val indexSeeks: Seq[(SchemaIndex, Predicate, Predicate)] = for (
      labelPredicate <- labelPredicates;
      propertyPredicate <- propertyPredicates
      if (ctx.getIndexRuleId(labelPredicate.solution, propertyPredicate.solution).nonEmpty)
    ) yield (SchemaIndex(node, labelPredicate.solution, propertyPredicate.solution, None), labelPredicate.predicate, propertyPredicate.predicate)

    if (idPredicates.nonEmpty) {
      val ids: Seq[Long] = idPredicates.map(_.solution)
      val predicates: Seq[Predicate] = idPredicates.map(_.predicate)
      StartItemWithRating(NodeById(node, ids: _*), Single, predicates)
    } else if (indexSeeks.nonEmpty) {
      // TODO: Once we have index statistics, we can pick the best one
      val chosenSeek = indexSeeks.head
      StartItemWithRating(chosenSeek._1, IndexEquality, Seq(chosenSeek._2, chosenSeek._3))
    } else if (labelPredicates.nonEmpty) {
      // TODO: Once we have label statistics, we can pick the best one
      StartItemWithRating(NodeByLabel(node, labelPredicates.head.solution), LabelScan, Seq(labelPredicates.head.predicate))
    } else {
      StartItemWithRating(AllNodes(node), Global, null)
    }
  }

  private def findLabelsForNode(node: String, where: Seq[QueryToken[Predicate]]): Seq[SolvedPredicate[LabelName]] =
    where.collect {
      case Unsolved(predicate@HasLabel(Identifier(identifier), label)) if identifier == node => SolvedPredicate(label.name, predicate)
    }

  private def findEqualityPredicatesOnProperty(identifier: IdentifierName, where: Seq[QueryToken[Predicate]]): Seq[SolvedPredicate[PropertyKey]] =
    where.collect {
      case Unsolved(predicate@Equals(Property(Identifier(id), propertyName), expression)) if id == identifier => SolvedPredicate(propertyName, predicate)
      case Unsolved(predicate@Equals(expression, Property(Identifier(id), propertyName))) if id == identifier => SolvedPredicate(propertyName, predicate)
    }

  private def findEqualityPredicatesUsingNodeId(identifier: IdentifierName, where: Seq[QueryToken[Predicate]]): Seq[SolvedPredicate[Long]] =
    where.collect {
      case Unsolved(predicate@Equals(IdFunction(Identifier(id)), Literal(idValue)))
        if id == identifier && idValue.isInstanceOf[Number] => SolvedPredicate(idValue.asInstanceOf[Number].longValue(), predicate)
      case Unsolved(predicate@Equals(Literal(idValue), IdFunction(Identifier(id))))
        if id == identifier && idValue.isInstanceOf[Number] => SolvedPredicate(idValue.asInstanceOf[Number].longValue(), predicate)
    }
}

case class StartItemWithRating(s: StartItem, rating: Integer, solvedPredicates:Seq[Predicate])
case class SolvedPredicate[T](solution:T, predicate:Predicate)