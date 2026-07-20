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

import org.neo4j.cypher.internal.ast.AliasedReturnItem
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
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
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

  def isPassthrough: Boolean = alias.contains(expression)

  def scopeSymbol: LogicalVariable = alias.getOrElse(referenceableVariable)

  def aliasString: String = alias.map(a => s"its alias `${a.name}`").getOrElse("an alias")

  /**
   * Whether this item's expression is a constant or a plain reference: a literal, parameter, variable,
   * or property access on a variable, per CIP-248.
   *
   * Note: this is more permissive than [[GroupingKey.isRecognized]], which additionally excludes
   * constants (literals and parameters).
   */
  private val isConstantOrReferenceLazy: LazyVal[Boolean] = LazyVal {
    expression match {
      case _: Literal | _: Parameter | _: LogicalVariable | Property(_: LogicalVariable, _) => true
      case _                                                                                => false
    }
  }
  def isConstantOrReference: Boolean = isConstantOrReferenceLazy.value

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

  def asReturnItem(anonVarGen: AnonymousVariableNameGenerator): AliasedReturnItem = {
    val itemAlias = alias.getOrElse(Variable(anonVarGen.nextName, expression.position))
    AliasedReturnItem(expression, itemAlias)(expression.position)
  }

  def asReturnItem: AliasedReturnItem = {
    expression match {
      case lv: LogicalVariable => AliasedReturnItem(lv.copyId, alias.get.copyId)(expression.position)
      case _                   => AliasedReturnItem(expression, alias.get.copyId)(expression.position)
    }
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

  /**
   * Recognized grouping keys are either variables or property access on variables, as defined in CIP-248.
   *
   * Note: this is stricter than [[ProjectionItem.isConstantOrReference]], which additionally permits
   * constants (literals and parameters).
   */
  def isRecognized: Boolean = expression match {
    case _: LogicalVariable              => true
    case Property(_: LogicalVariable, _) => true
    case _                               => false
  }

  def introduceAlias(anonVarGen: AnonymousVariableNameGenerator): GroupingKey =
    copy(alias = Some(alias.getOrElse(Variable(anonVarGen.nextName, expression.position))))

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

case class ProjectionSpecification(
  groupingKeys: Set[GroupingKey],
  nonAggregatingItems: Set[NonAggregatingItem],
  aggregatingItems: Set[AggregatingItem],
  distinct: Boolean,
  hasGroupBy: Boolean
) {
  val items: Set[RecognizingItem] = nonAggregatingItems ++ aggregatingItems
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

  private val allItemsByExpressionLazy: LazyVal[Map[Expression, ProjectionItem]] =
    LazyVal(firstWinsMap(allItems.iterator.map(i => i.expression -> i)))
  private def allItemsByExpression: Map[Expression, ProjectionItem] = allItemsByExpressionLazy.value

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

  def getUnderlyingExpression(expr: Expression): Option[Expression] = expr match {
    case lv: LogicalVariable => groupingKeyByAlias.get(lv).map(_.expression)
    case _                   => None
  }

  private def containsDeclaration(that: Expression): Boolean = {
    val subExpressions = that.subExpressions.toSet
    aliases.exists(subExpressions)
  }

  def recognizeInNonAggregatingItem(that: Expression, isSubExpression: Boolean): Option[ProjectionItem] =
    if (!hasGroupBy) None
    else groupingKeyByExpression.get(that).filter(gk => !isSubExpression || gk.isConstantOrReference)

  def recognizeInAggregation(that: Expression): Option[ProjectionItem] =
    groupingKeyByExpression.get(that).filter(_.isConstantOrReference)

  // Recognizes expressions according to recognition rules defined in CIP-248
  def recognizeInSubclause(that: Expression, isSubExpression: Boolean): Option[ProjectionItem] =
    if (hasGroupBy && containsDeclaration(that)) None
    else allItemsByExpression.get(that).filter(item => !isSubExpression || !hasGroupBy || item.isConstantOrReference)

  // An Inset key is a grouping key that is not present in the projection items.
  def hasInsetKeys: Boolean =
    hasGroupBy && groupingKeys.exists(gk =>
      !nonAggregatingItems.exists(pi => pi.expression == gk.expression || pi.alias == gk.alias)
    )

  private val nonPassthroughAliases = (items ++ groupingKeys).filterNot(_.isPassthrough)

  private val nonPassthroughAliasSetLazy: LazyVal[Set[LogicalVariable]] =
    LazyVal(nonPassthroughAliases.iterator.flatMap(_.alias).toSet)

  def isNonPassthroughAlias(that: Expression): Boolean =
    that match {
      case lv: LogicalVariable => nonPassthroughAliasSetLazy.value.contains(lv)
      case _                   => false
    }

  def containsNonPassthroughAlias(variables: Set[LogicalVariable]): Boolean =
    variables.exists(isNonPassthroughAlias)

  /**
   *  Expression substitution works differently in Cypher 25 with and without the GROUP BY clause.
   *
   *  1. In Cypher 25 with GROUP BY and in subexpressions in both cases only
   *     expressions that do not include a reference to a variable with the
   *     same name as a variable declared in the clause are substituted.
   *
   *  2. In Cypher 25 without GROUP BY a full expression is always substituted.
   *     This behavior is deprecated and will be replaced by the same behavior
   *     as the GROUP BY case.
   */
  private def substituteExpression(that: Expression, scopeState: ScopeState): Option[Expression] =
    that match {
      case lv: LogicalVariable if isNonPassthroughAlias(lv) => Some(lv)
      case _ =>
        val matched = allItems.find(_.expression == that)
        val matchedRefs = matched.map(item => scopeState.getReferenced(item.expression, Set.empty))
        matched match {
          case Some(_) if containsNonPassthroughAlias(matchedRefs.get) => Some(that)
          case Some(item) => Some(item.alias.get.copyId.withPosition(that.position))
          case None       => None
        }
    }

  def substituteSubExpression(that: Expression, scopeState: ScopeState): Expression =
    substituteExpression(that, scopeState).getOrElse(that)

  /**
   * Full-expression substitution for the GROUP BY case: a subclause expression equal to a grouping key
   * is rewritten to that key's alias, except when ambiguous — it references a variable shadowed by a
   * non-passthrough projection alias of the same name, in which case the alias wins and the expression
   * is kept as written.
   */
  private def substituteFullExpressionWithGroupBy(that: Expression, scopeState: ScopeState): Option[Expression] =
    that match {
      case lv: LogicalVariable if isNonPassthroughAlias(lv) => Some(lv)
      case _ =>
        allItemsByExpression.get(that) match {
          case Some(item) if shadowedByOtherAlias(item, that, scopeState) => Some(that)
          case Some(item) => item.alias.map(_.withPosition(that.position))
          case None       => None
        }
    }

  private def shadowedByOtherAlias(item: ProjectionItem, that: Expression, scopeState: ScopeState): Boolean =
    scopeState.referenceTargets(that).exists(t => !item.alias.contains(t) && isNonPassthroughAlias(t))

  def substituteFullExpression(
    that: Expression,
    useLegacySubstitution: Boolean,
    scopeState: ScopeState
  ): Option[Expression] =
    if (useLegacySubstitution) that match {
      case lv: LogicalVariable if isNonPassthroughAlias(lv) => None
      case _ =>
        allItems.find(_.expression == that) match {
          case Some(item) => item.alias.map(_.copyId)
          case None       => None
        }
    }
    else substituteFullExpressionWithGroupBy(that, scopeState)

  private val introducedSymbolsLazy: LazyVal[Set[LogicalVariable]] =
    LazyVal(items.iterator.filterNot(_.isPassthrough).flatMap(_.alias).toSet)
  def getIntroducedSymbols: Set[LogicalVariable] = introducedSymbolsLazy.value

  def getShadowingDeclarations(incomingSymbols: Set[LogicalVariable]): Set[LogicalVariable] =
    incomingSymbols intersect getIntroducedSymbols

  def getContainingItem(expr: Expression): Option[ProjectionItem] =
    items.find(_.expression == expr).orElse {
      val subExpressions = expr.subExpressions
      items.find(i => subExpressions.contains(i.expression))
    }
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
