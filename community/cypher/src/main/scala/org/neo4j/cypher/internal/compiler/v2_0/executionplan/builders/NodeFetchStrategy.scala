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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.commands._
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions._
import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.commands.values.KeyToken

/*
This rather simple class finds a starting strategy for a given single node and a list of predicates required
to be true for that node

@see NodeStrategy
 */
object NodeFetchStrategy {

  val nodeStrategies: Seq[NodeStrategy] = Seq(NodeByIdStrategy, IndexSeekStrategy, LabelScanStrategy, GlobalStrategy)

  def findStartStrategy(node: String, boundIdentifiers: Set[String], where: Seq[Predicate], ctx: PlanContext): RatedStartItem = {
    val ratedItems = nodeStrategies.flatMap(_.findRatedStartItems(node, boundIdentifiers, where, ctx))
    ratedItems.sortBy(_.rating).head
  }

  def findUniqueIndexes(props: Map[KeyToken, Expression], labels: Seq[KeyToken], ctx: PlanContext): Seq[(KeyToken, KeyToken)] = {
    val indexes = labels.flatMap { (label: KeyToken) => findUniqueIndexesForLabel( label, props.keys, ctx ) }
    implicit val ordering = KeyToken.Ordering
    indexes.sorted
  }

  def findUniqueIndexesForLabel(label: KeyToken, keys: Iterable[KeyToken], ctx: PlanContext): Seq[(KeyToken, KeyToken)] =
    keys.flatMap { (key: KeyToken) =>
      ctx.getUniquenessConstraint(label.name, key.name).map { _ => (label, key) }
    }.toSeq

  val Single = 0
  val IndexEquality = 1
  val IndexRange = 2
  val IndexScan = 3
  val LabelScan = 4
  val Global = 5
}

import NodeFetchStrategy.Single
import NodeFetchStrategy.Global
import NodeFetchStrategy.IndexEquality
import NodeFetchStrategy.LabelScan

/*
Bundles a possible start item with a rating (where lower implies better) and a list of predicates that
are implicitly solved when using the start item
 */
case class RatedStartItem(s: StartItem, rating: Int, solvedPredicates: Seq[Predicate])

/*
Finders produce StartItemWithRatings for a node and a set of required predicates over that node
 */
trait NodeStrategy {

  type LabelName = String
  type IdentifierName = String
  type PropertyKey = String

  def findRatedStartItems(node: String, boundIdentifiers: Set[String], where: Seq[Predicate], ctx: PlanContext): Seq[RatedStartItem]

  protected def findLabelsForNode(node: String, where: Seq[Predicate]): Seq[SolvedPredicate[LabelName]] =
    where.collect {
      case predicate @ HasLabel(Identifier(identifier), label) if identifier == node => SolvedPredicate(label.name, predicate)
    }

  case class SolvedPredicate[+T](solution: T, predicate: Predicate)
}

object NodeByIdStrategy extends NodeStrategy {

  def findRatedStartItems(node: String, boundIdentifiers: Set[String], where: Seq[Predicate], ctx: PlanContext): Seq[RatedStartItem] = {
    val solvedPredicates: Seq[SolvedPredicate[Expression]] = findEqualityPredicatesForBoundIdentifiers(node, boundIdentifiers, where)
    val solutions: Seq[Expression] = solvedPredicates.map(_.solution)
    val predicates: Seq[Predicate] = solvedPredicates.map(_.predicate)

    solutions match {
      case Seq()        => Seq()
      case head :: tail => Seq(RatedStartItem(NodeByIdOrEmpty(node, head), Single, predicates))
    }
  }

  private def findEqualityPredicatesForBoundIdentifiers(identifier: IdentifierName, boundIdentifiers: Set[String], where: Seq[Predicate]): Seq[SolvedPredicate[Expression]] = {
    def computable(expression: Expression): Boolean = ! expression.exists {
      case Identifier(name) => !boundIdentifiers(name)
      case _                => false
    }
    
    where.collect {
      case predicate @ Equals(IdFunction(Identifier(id)), Literal(idValue)) if id == identifier && idValue.isInstanceOf[Number] => SolvedPredicate(Literal(idValue.asInstanceOf[Number].longValue()), predicate)
      case predicate @ Equals(Literal(idValue), IdFunction(Identifier(id))) if id == identifier && idValue.isInstanceOf[Number] => SolvedPredicate(Literal(idValue.asInstanceOf[Number].longValue()), predicate)
        
      case predicate @ Equals(IdFunction(Identifier(id)), expression) if id == identifier && computable(expression) => SolvedPredicate(expression, predicate)
      case predicate @ Equals(expression, IdFunction(Identifier(id))) if id == identifier && computable(expression) => SolvedPredicate(expression, predicate)

      case predicate @ AnyInCollection(collectionExpression, _, Equals(IdFunction(Identifier(id)), _)) if id == identifier && computable(collectionExpression) => SolvedPredicate(collectionExpression, predicate)
    }
  }
}

object IndexSeekStrategy extends NodeStrategy {

  def findRatedStartItems(node: String, boundIdentifiers: Set[String], where: Seq[Predicate], ctx: PlanContext): Seq[RatedStartItem] = {
    val labelPredicates: Seq[SolvedPredicate[LabelName]] = findLabelsForNode(node, where)
    val propertyPredicates: Seq[SolvedPredicate[PropertyKey]] = findEqualityPredicatesOnProperty(node, where)

    for (
      labelPredicate <- labelPredicates;
      propertyPredicate <- propertyPredicates if ctx.getIndexRule(labelPredicate.solution, propertyPredicate.solution).nonEmpty
    ) yield {
      val schemaIndex = SchemaIndex(node, labelPredicate.solution, propertyPredicate.solution, AnyIndex, None)
      val optConstraint = ctx.getUniquenessConstraint(labelPredicate.solution, propertyPredicate.solution)
      val rating = if (optConstraint.isDefined) Single else IndexEquality
      val predicates = Seq(labelPredicate.predicate, propertyPredicate.predicate)
      RatedStartItem(schemaIndex, rating, predicates)
    }
  }

  private def findEqualityPredicatesOnProperty(identifier: IdentifierName, where: Seq[Predicate]): Seq[SolvedPredicate[PropertyKey]] =
    where.collect {
      case predicate @ Equals(Property(Identifier(id), propertyKey), expression) if id == identifier => SolvedPredicate(propertyKey.name, predicate)
      case predicate @ Equals(expression, Property(Identifier(id), propertyKey)) if id == identifier => SolvedPredicate(propertyKey.name, predicate)
      case predicate @ Equals(nullable @ Nullable(Property(Identifier(id), propertyKey)), expression) if nullable.default == Some(false) && id == identifier => SolvedPredicate(propertyKey.name, predicate)
      case predicate @ Equals(expression, nullable @ Nullable(Property(Identifier(id), propertyKey))) if nullable.default == Some(false) && id == identifier => SolvedPredicate(propertyKey.name, predicate)
    }
}

object GlobalStrategy extends NodeStrategy {
  def findRatedStartItems(node: String, boundIdentifiers: Set[String], where: Seq[Predicate], ctx: PlanContext): Seq[RatedStartItem] =
    Seq(RatedStartItem(AllNodes(node), Global, Seq.empty))
}

object LabelScanStrategy extends NodeStrategy {
  def findRatedStartItems(node: String, boundIdentifiers: Set[String], where: Seq[Predicate], ctx: PlanContext): Seq[RatedStartItem] = {
    val labelPredicates: Seq[SolvedPredicate[LabelName]] = findLabelsForNode(node, where)

    labelPredicates.map {
      case SolvedPredicate(labelName, predicate) =>
        RatedStartItem(NodeByLabel(node, labelName), LabelScan, Seq(predicate))
    }
  }
}
