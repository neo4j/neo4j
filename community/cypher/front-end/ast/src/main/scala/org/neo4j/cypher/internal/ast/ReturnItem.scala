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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.ReturnItems.ReturnVariables
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.util.ASTNode
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
    when(includeExisting) {
      (s: SemanticState) => success(s.importValuesFromScope(previousScope))
    } chain items.foldSemanticCheck(item =>
      item.alias match {
        case Some(variable) if item.expression == variable =>
          val maybePreviousSymbol = previousScope.symbol(variable.name)
          declareVariable(variable, types(item.expression), maybePreviousSymbol, overriding = true)
        case Some(variable) =>
          declareVariable(variable, types(item.expression), overriding = true)
        case None => (state: SemanticState) => SemanticCheckResult(state, Seq.empty)
      }
    )

  private def ensureProjectedToUniqueIds: SemanticCheck = {
    items.groupBy(_.name).foldSemanticCheck {
      case (_, groupedItems) if groupedItems.size > 1 =>
        SemanticError("Multiple result columns with the same name are not supported", groupedItems.head.position)
      case _ =>
        SemanticCheck.success
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

  def semanticCheck: SemanticCheck =
    SemanticExpressionCheck.check(Expression.SemanticContext.Results, expression)

  def stringify(expressionStringifier: ExpressionStringifier): String
}

case class UnaliasedReturnItem(expression: Expression, inputText: String)(val position: InputPosition)
    extends ReturnItem {

  val alias: Option[LogicalVariable] = expression match {
    case i: LogicalVariable => Some(i.copyId)
    case x: MapProjection   => Some(x.name.copyId)
    case _                  => None
  }
  val name: String = alias.map(_.name) getOrElse { inputText.trim }

  override def asCanonicalStringVal: String = expression.asCanonicalStringVal

  def stringify(expressionStringifier: ExpressionStringifier): String = expressionStringifier(expression)
}

object AliasedReturnItem {

  def apply(v: LogicalVariable): AliasedReturnItem =
    AliasedReturnItem(v.copyId, v.copyId)(v.position)
}

case class AliasedReturnItem(expression: Expression, variable: LogicalVariable)(val position: InputPosition)
    extends ReturnItem {
  val alias: Option[LogicalVariable] = Some(variable)
  val name: String = variable.name

  override def dup(children: Seq[AnyRef]): AliasedReturnItem.this.type =
    this.copy(
      children.head.asInstanceOf[Expression],
      children(1).asInstanceOf[LogicalVariable]
    )(position).asInstanceOf[this.type]

  override def asCanonicalStringVal: String = s"${expression.asCanonicalStringVal} AS ${variable.asCanonicalStringVal}"

  def stringify(expressionStringifier: ExpressionStringifier): String =
    s"${expressionStringifier(expression)} AS ${expressionStringifier(variable)}"

}

object ReturnItems {

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

  def implicitGroupingExpressionInAggregationColumnErrorMessage(variables: Seq[String]): String =
    "Aggregation column contains implicit grouping expressions. " +
      "For example, in 'RETURN n.a, n.a + n.b + count(*)' the aggregation expression 'n.a + n.b + count(*)' includes the implicit grouping key 'n.b'. " +
      "It may be possible to rewrite the query by extracting these grouping/aggregation expressions into a preceding WITH clause. " +
      s"Illegal expression(s): ${variables.mkString(", ")}"

  def checkAmbiguousGrouping(returnItems: ReturnItems): Option[SemanticError] = {
    val returnItemExprs = returnItems.items.map(_.expression).toSet
    // FullSubqueryExpressions can contain aggregates, but they also contain a whole query so that isn't relevant for this check.
    val aggregationExpressions = returnItemExprs.collect {
      case expr if expr.containsAggregate && !expr.isInstanceOf[FullSubqueryExpression] => expr
    }
    val newGroupingVariables = returnItemExprs.collect { case expr: LogicalVariable => expr }
    val newPropertiesUsedForGrouping = returnItemExprs.collect { case v @ LogicalProperty(LogicalVariable(_), _) => v }

    val ambiguousAggregationExpressions = aggregationExpressions
      .flatMap(aggItem =>
        AmbiguousAggregation.ambiguousExpressions(aggItem, newGroupingVariables, newPropertiesUsedForGrouping)
      )

    if (ambiguousAggregationExpressions.nonEmpty) {
      val errorMsg = implicitGroupingExpressionInAggregationColumnErrorMessage(
        ambiguousAggregationExpressions.map(_.asCanonicalStringVal).toSeq
      )
      Some(SemanticError(
        errorMsg,
        ambiguousAggregationExpressions.head.position
      ))
    } else {
      None
    }
  }
}
