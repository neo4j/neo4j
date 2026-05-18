/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast.semantics.scoping

import org.neo4j.cypher.internal.ast.ExplicitGroupingElements
import org.neo4j.cypher.internal.ast.GroupingAll
import org.neo4j.cypher.internal.ast.GroupingElements
import org.neo4j.cypher.internal.ast.GroupingNone
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.helpers.LazyVal

import scala.collection.immutable.ListSet

/**
 * ProjectionSpecification
 *
 * ProjectionItem
 *    - Expression
 *    - Option[LogicalVariable]
 *
 *    Grouping Keys
 *       - Grouping expression
 *       - IsRecognizable
 *
 *    RecognizingItem
 *
 *       NonAggregatingItem
 *
 *       AggregatingItem
 */

sealed trait ProjectionItem {
  def expression: Expression
  def alias: Option[LogicalVariable]

  def scopeSymbol: LogicalVariable = alias.getOrElse(referenceableVariable)

  def aliasString: String = alias.map(a => s"its alias `${a.name}`").getOrElse("an alias")

  // Recognizable expressions
  private val isRecognizableLazy: LazyVal[Boolean] = LazyVal {
    expression match {
      case _: Literal | _: Parameter | _: LogicalVariable | Property(_: LogicalVariable, _) => true
      case _                                                                                => false
    }
  }
  def isRecognizable: Boolean = isRecognizableLazy.value

  private def stringifiedName(expression: Expression): String =
    ProjectionItem.SyntheticNamePrefix +
      ProjectionItem.expressionStringifier(expression) +
      ProjectionItem.SyntheticNameSuffix

  // Synthetic variable used to refer to a recognizable expression by name when the user did not supply an alias.
  private val referenceableVariableLazy: LazyVal[LogicalVariable] =
    LazyVal(Variable(stringifiedName(expression))(expression.position, isIsolated = false))
  def referenceableVariable: LogicalVariable = referenceableVariableLazy.value

  def isSubclauseRecognizable(expr: Expression): Boolean = expr match {
    case lv: LogicalVariable                 => alias.contains(lv) || lv == expression || lv == referenceableVariable
    case p @ Property(_: LogicalVariable, _) => p == expression
    case _                                   => false
  }
}

object ProjectionItem {

  private[scoping] val SyntheticNamePrefix: String = "`  "
  private[scoping] val SyntheticNameSuffix: String = "`"

  private[scoping] val expressionStringifier: ExpressionStringifier = ExpressionStringifier()

  def unapply(pi: ProjectionItem): Option[(Expression, Option[LogicalVariable])] = Some((pi.expression, pi.alias))
}

sealed trait RecognizingItem extends ProjectionItem {
  def aggregatingRecognizableExpression: Option[Expression]
  def subclauseRecognizableExpression: Option[Expression]
  def subclauseRecognizableSymbols: Set[Expression]
}

case class GroupingKey(expression: Expression, alias: Option[LogicalVariable], explicit: Boolean)
    extends ProjectionItem {

  private def matchesUnderlyingExpression(expr: Expression): Boolean = expr match {
    case lv: LogicalVariable                 => lv == expression
    case p @ Property(_: LogicalVariable, _) => p == expression
    case _                                   => false
  }

  def isNonAggregatingRecognizable(expr: Expression): Boolean = matchesUnderlyingExpression(expr)

  def isAggregationRecognizable(expr: Expression): Boolean = matchesUnderlyingExpression(expr)
}

case class NonAggregatingItem(override val expression: Expression, override val alias: Option[LogicalVariable])
    extends RecognizingItem {

  override val aggregatingRecognizableExpression: Option[Expression] = None

  override val subclauseRecognizableExpression: Option[Expression] = Some(expression)

  override def subclauseRecognizableSymbols: Set[Expression] = (alias ++ subclauseRecognizableExpression).toSet
}

case class AggregatingItem(override val expression: Expression, override val alias: Option[LogicalVariable])
    extends RecognizingItem {

  override val aggregatingRecognizableExpression: Option[Expression] = None

  override val subclauseRecognizableExpression: Option[Expression] = Some(expression)

  override def subclauseRecognizableSymbols: Set[Expression] = (alias ++ subclauseRecognizableExpression).toSet
}

/**
 *  The three item collections are constructed as `ListSet` (insertion-ordered) so that
 *  recognition lookups via `find` are deterministic.
 */
case class ProjectionSpecification(
  groupingKeys: Set[GroupingKey],
  nonAggregatingItems: Set[NonAggregatingItem],
  aggregatingItems: Set[AggregatingItem],
  distinct: Boolean,
  hasGroupBy: Boolean
) {
  val items: Set[ProjectionItem] = nonAggregatingItems ++ aggregatingItems
  val allItems: Set[ProjectionItem] = nonAggregatingItems ++ aggregatingItems ++ groupingKeys
  val aliases: Set[LogicalVariable] = nonAggregatingItems.flatMap(_.alias) ++ aggregatingItems.flatMap(_.alias)

  def isAggregating: Boolean = aggregatingItems.nonEmpty || distinct || hasGroupBy
  def isEmpty: Boolean = nonAggregatingItems.isEmpty && aggregatingItems.isEmpty
  def size: Int = nonAggregatingItems.size + aggregatingItems.size

  /**
   * Drop any grouping key whose underlying expression is a Variable / Property(Variable, _) whose name is in
   * `shadowedNames`. Used when an inner scope-binding construct (list-comp, iter-pred, pattern-comp, reduce)
   * shadows a name that would otherwise be recognised as a grouping key from an outer projection.
   */
  def shadowGroupingKeys(shadowedNames: Set[String]): ProjectionSpecification = {
    val filteredKeys = groupingKeys.filterNot { gk =>
      gk.expression match {
        case v: LogicalVariable              => shadowedNames.contains(v.name)
        case Property(v: LogicalVariable, _) => shadowedNames.contains(v.name)
        case _                               => false
      }
    }
    copy(groupingKeys = filteredKeys)
  }

  /**
   * Symbols visible to subclause expressions (ORDER BY / WHERE / SKIP / LIMIT). Per CIP-236 Rule 7,
   * grouping-key aliases are visible in subclauses even when they are not return items, so
   * `groupingKeys.flatMap(_.alias)` is included alongside the projection-item scope symbols.
   */
  val subclauseScopeSymbols: Set[LogicalVariable] =
    nonAggregatingItems.map(_.scopeSymbol) ++
      aggregatingItems.map(_.scopeSymbol) ++
      groupingKeys.flatMap(_.alias)

  /**
   * Combine [[visible]] with this projection's [[subclauseScopeSymbols]] under shadowing semantics:
   * symbols in `visible` whose name matches a `subclauseScopeSymbol` are dropped, then unioned with
   * the projection symbols. Used by [[ProjectionExpressionContext.projectionChildContext]] to build
   * the visible-symbol set for ORDER BY / WHERE / SKIP / LIMIT subclauses.
   */
  def shadowSubclauseSymbols(visible: Set[LogicalVariable]): Set[LogicalVariable] = {
    val preferredNames = subclauseScopeSymbols.iterator.map(_.name).toSet
    visible.filterNot(v => preferredNames.contains(v.name)) union subclauseScopeSymbols
  }

  private val groupingKeyByExpressionLazy: LazyVal[Map[Expression, GroupingKey]] =
    LazyVal(firstWinsMap(groupingKeys.iterator.map(gk => gk.expression -> gk)))
  private def groupingKeyByExpression: Map[Expression, GroupingKey] = groupingKeyByExpressionLazy.value

  private val groupingKeyByAliasLazy: LazyVal[Map[LogicalVariable, GroupingKey]] =
    LazyVal(firstWinsMap(groupingKeys.iterator.flatMap(gk => gk.alias.iterator.map(_ -> gk))))
  private def groupingKeyByAlias: Map[LogicalVariable, GroupingKey] = groupingKeyByAliasLazy.value

  private val allItemByExpressionLazy: LazyVal[Map[Expression, ProjectionItem]] =
    LazyVal(firstWinsMap(allItems.iterator.map(i => i.expression -> i)))
  private def allItemByExpression: Map[Expression, ProjectionItem] = allItemByExpressionLazy.value

  private def firstWinsMap[K, V](pairs: Iterator[(K, V)]): Map[K, V] =
    pairs.foldLeft(Map.empty[K, V]) { case (m, (k, v)) => if (m.contains(k)) m else m.updated(k, v) }

  def isNonAggregatingRecognizable(expr: Expression): Boolean = expr match {
    case _: LogicalVariable | Property(_: LogicalVariable, _) => groupingKeyByExpression.contains(expr)
    case _                                                    => false
  }

  def isSubclauseRecognizable(expr: Expression): Boolean =
    isAggregating &&
      (groupingKeys.exists(_.isSubclauseRecognizable(expr)) || items.exists(_.isSubclauseRecognizable(expr)))

  def isAggregationRecognizable(expr: Expression): Boolean = expr match {
    case _: LogicalVariable | Property(_: LogicalVariable, _) => groupingKeyByExpression.contains(expr)
    case _                                                    => false
  }

  def isAlias(that: Expression): Boolean = that match {
    case lv: LogicalVariable => aliases.contains(lv)
    case _                   => false
  }

  def hasExplicitKeys: Boolean = groupingKeys.exists(_.explicit) || groupingKeys.isEmpty

  def getGroupingKeyExpression(expr: Expression): Option[Expression] = expr match {
    case lv: LogicalVariable => groupingKeyByAlias.get(lv).map(_.expression)
    case _                   => None
  }

  private def containsDeclaration(that: Expression): Boolean = {
    val subExpressions = that.subExpressions.toSet
    aliases.exists(subExpressions)
  }

  def recognizeInNonAggregatingItem(that: Expression, isSubExpression: Boolean): Option[ProjectionItem] =
    if (!hasGroupBy) None
    else groupingKeyByExpression.get(that).filter(gk => !isSubExpression || gk.isRecognizable)

  def recognizeInAggregation(that: Expression): Option[ProjectionItem] =
    groupingKeyByExpression.get(that).filter(_.isRecognizable)

  // Recognizes expressions according to recognition rules defined in CIP-248
  def recognizeInSubclause(that: Expression, isSubExpression: Boolean): Option[ProjectionItem] =
    if (hasGroupBy && containsDeclaration(that)) None
    else allItemByExpression.get(that).filter(item => !isSubExpression || !hasGroupBy || item.isRecognizable)

}

object ProjectionSpecification {

  def nonAggregating(projections: Seq[(Expression, Option[LogicalVariable])]): ProjectionSpecification = {

    val nonAggregatingItems = projections.map { case (e, a) => NonAggregatingItem(e, a) }.to(ListSet)

    ProjectionSpecification(Set.empty, nonAggregatingItems, Set.empty, distinct = false, hasGroupBy = false)
  }

  def implicitKeys(
    projections: Seq[(Expression, Option[LogicalVariable])],
    distinct: Boolean
  ): ProjectionSpecification = {

    val (aggregating, nonAggregating) = projections.distinct.partition(_._1.containsAggregate)
    val nonAggregatingItems = nonAggregating.map { case (e, a) => NonAggregatingItem(e, a) }.to(ListSet)
    val aggregatingItems = aggregating.map { case (e, a) => AggregatingItem(e, a) }.to(ListSet)
    val groupingKeys =
      nonAggregating.map { case (e, a) => GroupingKey(e, a, explicit = false) }.to(ListSet)

    ProjectionSpecification(groupingKeys, nonAggregatingItems, aggregatingItems, distinct, hasGroupBy = false)
  }

  def explicitKeys(
    groupingElements: GroupingElements,
    projections: Seq[(Expression, Option[LogicalVariable])],
    distinct: Boolean
  ): ProjectionSpecification = {
    val (aggregating, nonAggregating) = projections.distinct.partition(_._1.containsAggregate)
    val nonAggregatingItems = nonAggregating.map { case (e, a) => NonAggregatingItem(e, a) }.to(ListSet)
    val aggregatingItems = aggregating.map { case (e, a) => AggregatingItem(e, a) }.to(ListSet)

    def getGroupingKey(exprOrAlias: Expression): GroupingKey = exprOrAlias match {
      case alias: LogicalVariable =>
        projections
          .find(_._2.contains(alias)).map { case (expr, _) => GroupingKey(expr, Some(alias), explicit = true) }
          .getOrElse(GroupingKey(alias, Some(alias), explicit = true))
      case expr: Expression =>
        projections
          .find(_._1 == expr).map { case (expr, alias) => GroupingKey(expr, alias, explicit = true) }
          .getOrElse(GroupingKey(expr, None, explicit = true))
    }

    val groupingKeys: ListSet[GroupingKey] = groupingElements match {
      case ExplicitGroupingElements(elements) => elements.map(getGroupingKey).to(ListSet)
      case GroupingAll() =>
        nonAggregating.map { case (e, a) => GroupingKey(e, a, explicit = false) }.to(ListSet)
      case GroupingNone() => ListSet.empty[GroupingKey]
    }

    ProjectionSpecification(groupingKeys, nonAggregatingItems, aggregatingItems, distinct, hasGroupBy = true)
  }

}
