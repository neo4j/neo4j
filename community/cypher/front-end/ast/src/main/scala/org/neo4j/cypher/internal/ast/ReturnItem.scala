/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.ReturnItems.ReturnVariables
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.DeprecatedAmbiguousGroupingNotification
import org.neo4j.cypher.internal.util.InputPosition

/**
 *
 * @param includeExisting       Users must specify return items for the projection, either all variables (*), no variables (-), or explicit expressions.
 *                              Neo4j does not support the no variables case on the surface, but it may appear as the result of expanding the star (*) when no variables are in scope.
 *                              This field is true if the dash (-) was used by a user.
 *
 * @param defaultOrderOnColumns For some clauses the default order of alphabetical columns is inconvenient, primarily show command clauses.
 *                              If this field is set, the given order will be used instead of the alphabetical order.
 */
final case class ReturnItems(
                              includeExisting: Boolean,
                              items: Seq[ReturnItem],
                              defaultOrderOnColumns: Option[List[String]] = None
                            )(val position: InputPosition) extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {

  def withExisting(includeExisting: Boolean): ReturnItems =
    copy(includeExisting = includeExisting)(position)

  def withDefaultOrderOnColumns(defaultOrderOnColumns: List[String]): ReturnItems =
    copy(defaultOrderOnColumns = Some(defaultOrderOnColumns))(position)

  def semanticCheck: SemanticCheck = items.semanticCheck chain ensureProjectedToUniqueIds

  def aliases: Set[LogicalVariable] = items.flatMap(_.alias).toSet

  def mapItems(f: Seq[ReturnItem] => Seq[ReturnItem]): ReturnItems =
    copy(items = f(items))(position)

  def declareVariables(previousScope: Scope): SemanticCheck =
    when (includeExisting) {
      s => success(s.importValuesFromScope(previousScope))
    } chain items.foldSemanticCheck(item => item.alias match {
      case Some(variable) if item.expression == variable =>
        val maybePreviousSymbol = previousScope.symbol(variable.name)
        declareVariable(variable, types(item.expression), maybePreviousSymbol, overriding = true)
      case Some(variable) =>
        declareVariable(variable, types(item.expression), overriding = true)
      case None           => state => SemanticCheckResult(state, Seq.empty)
    })

  private def ensureProjectedToUniqueIds: SemanticCheck = {
    items.groupBy(_.name).foldLeft(success) {
       case (acc, (_, groupedItems)) if groupedItems.size > 1 =>
        acc chain SemanticError("Multiple result columns with the same name are not supported", groupedItems.head.position)
       case (acc, _) =>
         acc
    }
  }

  def returnVariables: ReturnVariables = ReturnVariables(includeExisting, items.flatMap(_.alias))

  def containsAggregate: Boolean = items.exists(_.expression.containsAggregate)

  def isSimple: Boolean = items.exists(_.expression.isSimple)
}

sealed trait ReturnItem extends ASTNode with SemanticCheckable {
  def expression: Expression
  def alias: Option[LogicalVariable]
  def name: String
  def isPassThrough: Boolean = alias.contains(expression)

  def semanticCheck: SemanticCheck = SemanticExpressionCheck.check(Expression.SemanticContext.Results, expression) chain checkForExists

  private def checkForExists: SemanticCheck = {
    val invalid: Option[Expression] = expression.folder.treeFind[Expression] { case _: ExistsSubClause => true }
    invalid.map(exp => SemanticError("The EXISTS subclause is not valid inside a WITH or RETURN clause.", exp.position))
  }

  def stringify(expressionStringifier: ExpressionStringifier): String
}

case class UnaliasedReturnItem(expression: Expression, inputText: String)(val position: InputPosition) extends ReturnItem {
  val alias: Option[LogicalVariable] = expression match {
    case i: LogicalVariable => Some(i.copyId)
    case x: MapProjection => Some(x.name.copyId)
    case _ => None
  }
  val name: String = alias.map(_.name) getOrElse { inputText.trim }

  override def asCanonicalStringVal: String = expression.asCanonicalStringVal

  def stringify(expressionStringifier: ExpressionStringifier): String = expressionStringifier(expression)
}

object AliasedReturnItem {
  def apply(v:LogicalVariable):AliasedReturnItem = AliasedReturnItem(v.copyId, v.copyId)(v.position, isAutoAliased = true)
}

//TODO variable should not be a Variable. A Variable is an expression, and the return item alias isn't
case class AliasedReturnItem(expression: Expression, variable: LogicalVariable)(val position: InputPosition, val isAutoAliased: Boolean) extends ReturnItem {
  val alias: Option[LogicalVariable] = Some(variable)
  val name: String = variable.name

  override def dup(children: Seq[AnyRef]): AliasedReturnItem.this.type =
      this.copy(children.head.asInstanceOf[Expression], children(1).asInstanceOf[LogicalVariable])(position, isAutoAliased).asInstanceOf[this.type]

  override def asCanonicalStringVal: String = s"${expression.asCanonicalStringVal} AS ${variable.asCanonicalStringVal}"

  def stringify(expressionStringifier: ExpressionStringifier): String = s"${expressionStringifier(expression)} AS ${expressionStringifier(variable)}"

}

object ReturnItems {
  private val ExprStringifier = ExpressionStringifier(e => e.asCanonicalStringVal)

  /**
   * This is a subset of the information of [[ReturnItems]].
   * It only tracks the returned variables, but not aliases and other things.
   */
  case class ReturnVariables(
    includeExisting: Boolean,
    explicitVariables: Seq[LogicalVariable]
  )

  object ReturnVariables {
    def empty: ReturnVariables = ReturnVariables(includeExisting = false, Seq.empty)
  }

  def checkAmbiguousGrouping(returnItems: ReturnItems, nameOfClause: String): SemanticCheck = (state: SemanticState) => {
    val stateWithNotifications = {
      val aggregationExpressions = returnItems.items.map(_.expression).collect { case expr if expr.containsAggregate => expr }

      if (AmbiguousAggregation.containsDeprecatedAggrExpr(aggregationExpressions, returnItems.items)) {
        state.addNotification(DeprecatedAmbiguousGroupingNotification(
          returnItems.position,
          getAmbiguousNotificationDetails(returnItems.items, nameOfClause)
        ))
      } else {
        state
      }
    }

    SemanticCheckResult.success(stateWithNotifications)
  }

  /**
   * If possible, creates a notification detail which describes how this query can be rewritten to use an extra `WITH` clause.
   *
   * Example:
   * RETURN n.x + n.y, n.x + n.y + count(*), where n is a variable
   * ->
   * WITH n.x + n.y AS grpExpr0 RETURN grpExpr0, grpExpr0 + count(*)
   *
   * @param allReturnItems all return items, both grouping keys and expressions which contains aggregation(s).
   * @param nameOfClause   "RETURN" OR "WHERE"
   * @return
   */
  private def getAmbiguousNotificationDetails(allReturnItems: Seq[ReturnItem],
                                              nameOfClause: String): Option[String] = {

    val (aggregationItems, allGroupingItems) = allReturnItems.partition(_.expression.containsAggregate)
    val deprecatedGroupingKeys = AmbiguousAggregation.deprecatedGroupingKeysUsedInAggrExpr(aggregationItems.map(_.expression), allGroupingItems)
    if (deprecatedGroupingKeys.nonEmpty) {
      val (withItems, returnItems) = AmbiguousAggregation.getAliasedWithAndReturnItems(allReturnItems)
      val aggregationExpressions = returnItems.map(_.expression).collect { case expr if expr.containsAggregate => expr }

      if (!AmbiguousAggregation.containsDeprecatedAggrExpr(aggregationExpressions, returnItems)) {
        val singleDeprecatedGK = deprecatedGroupingKeys.size == 1
        Some(s"The grouping key${if (singleDeprecatedGK) "" else "s"} " +
          s"${deprecatedGroupingKeys.map(gk => ExprStringifier(gk.expression)).mkString("`", "`, `", "`")} " +
          s"${if (singleDeprecatedGK) "is" else "are"} deprecated. Could be rewritten using a `WITH`: " +
          s"`WITH ${withItems.map(_.stringify(ExprStringifier)).mkString(", ")}" +
          s" $nameOfClause ${returnItems.map(_.stringify(ExprStringifier)).mkString(", ")}`")
      } else {
        None
      }
    } else {
      None
    }
  }
}