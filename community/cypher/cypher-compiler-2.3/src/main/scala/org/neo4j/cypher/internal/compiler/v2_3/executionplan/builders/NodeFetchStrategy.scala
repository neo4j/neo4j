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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.commands._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions._
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.KeyToken
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable

/*
This rather simple class finds a starting strategy for a given single node and a list of predicates required
to be true for that node

@see NodeStrategy
 */
object NodeFetchStrategy {

  val nodeStrategies: Seq[NodeStrategy] = Seq(NodeByIdStrategy, IndexSeekStrategy, LabelScanStrategy, GlobalStrategy)

  def findStartStrategy(node: String, where: Seq[Predicate], ctx: PlanContext, symbols: SymbolTable): RatedStartItem = {
    val ratedItems = nodeStrategies.flatMap(_.findRatedStartItems(node, where, ctx, symbols))
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

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders.NodeFetchStrategy.{Global, IndexEquality, LabelScan, Single}

/*
Bundles a possible start item with a rating (where lower implies better) and a list of predicates that
are implicitly solved when using the start item
 */
case class RatedStartItem(s: StartItem, rating: Int, solvedPredicates: Seq[Predicate], newUnsolvedPredicates: Seq[Predicate] = Seq.empty)

/*
Finders produce StartItemWithRatings for a node and a set of required predicates over that node
 */
trait NodeStrategy {

  type LabelName = String
  type IdentifierName = String
  type PropertyKey = String

  def findRatedStartItems(node: String, where: Seq[Predicate], ctx: PlanContext, symbols: SymbolTable): Seq[RatedStartItem]

  protected def findLabelsForNode(node: String, where: Seq[Predicate]): Seq[SolvedPredicate[LabelName]] =
    where.collect {
      case predicate @ HasLabel(Identifier(identifier), label) if identifier == node => SolvedPredicate(label.name, predicate)
    }

  case class SolvedPredicate[+T](solution: T, predicate: Predicate, newUnsolvedPredicate: Option[Predicate] = None)
}

object NodeByIdStrategy extends NodeStrategy {

  def findRatedStartItems(node: String, where: Seq[Predicate], ctx: PlanContext, symbols: SymbolTable): Seq[RatedStartItem] = {
    val solvedPredicates: Seq[SolvedPredicate[Expression]] = findEqualityPredicatesForBoundIdentifiers(node, symbols, where)
    val solutions: Seq[Expression] = solvedPredicates.map(_.solution)
    val predicates: Seq[Predicate] = solvedPredicates.map(_.predicate)

    solutions match {
      case Seq()        => Seq()
      case head :: tail => Seq(RatedStartItem(NodeByIdOrEmpty(node, head), Single, predicates))
    }
  }

  private def findEqualityPredicatesForBoundIdentifiers(identifier: IdentifierName, symbols: SymbolTable, where: Seq[Predicate]): Seq[SolvedPredicate[Expression]] = {
    def computable(expression: Expression): Boolean = expression.symbolDependenciesMet(symbols)

    where.collect {
      case predicate @ Equals(IdFunction(Identifier(id)), Literal(idValue: Number)) if id == identifier => SolvedPredicate(Literal(idValue.longValue()), predicate)
      case predicate @ Equals(Literal(idValue: Number), IdFunction(Identifier(id))) if id == identifier => SolvedPredicate(Literal(idValue.longValue()), predicate)

      case predicate @ Equals(IdFunction(Identifier(id)), expression) if id == identifier && computable(expression) => SolvedPredicate(expression, predicate)
      case predicate @ Equals(expression, IdFunction(Identifier(id))) if id == identifier && computable(expression) => SolvedPredicate(expression, predicate)

      case predicate @ AnyInCollection(collectionExpression, _, Equals(IdFunction(Identifier(id)), _)) if id == identifier && computable(collectionExpression) => SolvedPredicate(collectionExpression, predicate)
    }
  }
}

object IndexSeekStrategy extends NodeStrategy {

  def findRatedStartItems(node: String, where: Seq[Predicate], ctx: PlanContext, symbols: SymbolTable): Seq[RatedStartItem] = {
    val labelPredicates: Seq[SolvedPredicate[LabelName]] = findLabelsForNode(node, where)
    val equalityPredicates: Seq[SolvedPredicate[PropertyKey]] = findEqualityPredicatesOnProperty(node, where, symbols)
    val seekByPrefixPredicates: Seq[SolvedPredicate[PropertyKey]] = findIndexSeekByPrefixPredicatesOnProperty(node, where, symbols)
    val seekByRangePredicates: Seq[SolvedPredicate[PropertyKey]] = findIndexSeekByRangePredicatesOnProperty(node, where, symbols)

    val result = for (
      labelPredicate <- labelPredicates
    ) yield {
        val equalityItems: Seq[RatedStartItem] =
          for (equalityPredicate <- equalityPredicates if ctx.getIndexRule(labelPredicate.solution, equalityPredicate.solution).nonEmpty)
            yield {
              val optConstraint = ctx.getUniquenessConstraint(labelPredicate.solution, equalityPredicate.solution)
              val rating = if (optConstraint.isDefined) Single else IndexEquality
              val indexType = if (optConstraint.isDefined) UniqueIndex else AnyIndex
              val schemaIndex = SchemaIndex(node, labelPredicate.solution, equalityPredicate.solution, indexType, None)
              RatedStartItem(schemaIndex, rating, solvedPredicates = Seq.empty,
                             newUnsolvedPredicates = equalityPredicate.newUnsolvedPredicate.toSeq)
            }

        val seekByPrefixItems: Seq[RatedStartItem] =
          for (seekByPrefixPredicate <- seekByPrefixPredicates if ctx.getIndexRule(labelPredicate.solution, seekByPrefixPredicate.solution).nonEmpty)
            yield {
              val schemaIndex = SchemaIndex(node, labelPredicate.solution, seekByPrefixPredicate.solution, AnyIndex, None)
              RatedStartItem(schemaIndex, NodeFetchStrategy.IndexRange, solvedPredicates = Seq.empty,
                             newUnsolvedPredicates = seekByPrefixPredicate.newUnsolvedPredicate.toSeq)
            }

        val seekByRangeItems: Seq[RatedStartItem] =
          for (seekByRangePredicate <- seekByRangePredicates if ctx.getIndexRule(labelPredicate.solution, seekByRangePredicate.solution).nonEmpty)
            yield {
              val schemaIndex = SchemaIndex(node, labelPredicate.solution, seekByRangePredicate.solution, AnyIndex, None)
              RatedStartItem(schemaIndex, NodeFetchStrategy.IndexRange, solvedPredicates = Seq.empty)
            }

        equalityItems ++ seekByPrefixItems ++ seekByRangeItems
      }

    result.flatten
  }

  private def findEqualityPredicatesOnProperty(identifier: IdentifierName, where: Seq[Predicate], symbols: SymbolTable): Seq[SolvedPredicate[PropertyKey]] = {
    where.collect {
      case predicate @ Equals(Property(Identifier(id), propertyKey), expression)
        if id == identifier && expression.symbolDependenciesMet(symbols) => SolvedPredicate(propertyKey.name, predicate)

      case predicate @ Equals(expression, Property(Identifier(id), propertyKey))
        if id == identifier && expression.symbolDependenciesMet(symbols) => SolvedPredicate(propertyKey.name, predicate)

      case predicate @ AnyInCollection(expression, _, Equals(Property(Identifier(id), propertyKey),Identifier(_)))
        if id == identifier && expression.symbolDependenciesMet(symbols) => SolvedPredicate(propertyKey.name, predicate)
    }
  }

  private def findIndexSeekByPrefixPredicatesOnProperty(identifier: IdentifierName, where: Seq[Predicate], initialSymbols: SymbolTable): Seq[SolvedPredicate[PropertyKey]] = {
    where.collect {
      case literalPredicate@StartsWith(p@Property(Identifier(id), prop), _) if id == identifier =>
        SolvedPredicate(prop.name, literalPredicate)
    }
  }

  private def findIndexSeekByRangePredicatesOnProperty(identifier: IdentifierName, where: Seq[Predicate], symbols: SymbolTable): Seq[SolvedPredicate[PropertyKey]] =
    where.collect {
      case predicate@AndedPropertyComparablePredicates(Identifier(id), prop@Property(_, key), comparables)
        if id == identifier && comparables.forall(_.other(prop).symbolDependenciesMet(symbols)) =>
        SolvedPredicate(key.name, predicate)
    }
}

object GlobalStrategy extends NodeStrategy {
  def findRatedStartItems(node: String, where: Seq[Predicate], ctx: PlanContext, symbols: SymbolTable): Seq[RatedStartItem] =
    Seq(RatedStartItem(AllNodes(node), Global, Seq.empty))
}

object LabelScanStrategy extends NodeStrategy {
  def findRatedStartItems(node: String, where: Seq[Predicate], ctx: PlanContext, symbols: SymbolTable): Seq[RatedStartItem] = {
    val labelPredicates: Seq[SolvedPredicate[LabelName]] = findLabelsForNode(node, where)

    labelPredicates.map {
      case SolvedPredicate(labelName, predicate, newUnsolvedPredicate) =>
        RatedStartItem(NodeByLabel(node, labelName), LabelScan, Seq(predicate), newUnsolvedPredicate.toSeq)
    }
  }
}