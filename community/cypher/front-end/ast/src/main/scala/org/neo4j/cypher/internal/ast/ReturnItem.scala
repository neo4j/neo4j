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
import org.neo4j.cypher.internal.ast.semantics._
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.LazyVal

/**
 *
 * @param projectionType The projection type describes whether existing variables are included and if whether they can be overridden.
 *
 * @param defaultOrderOnColumns For some clauses the default order of alphabetical columns is inconvenient, primarily show command clauses.
 *                              If this field is set, the given order will be used instead of the alphabetical order.
 */
final case class ReturnItems(
  projectionType: ProjectionType,
  items: Seq[ReturnItem],
  defaultOrderOnColumns: Option[List[String]] = None
)(val position: InputPosition) extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {

  def withDefaultOrderOnColumns(defaultOrderOnColumns: List[String]): ReturnItems =
    copy(defaultOrderOnColumns = Some(defaultOrderOnColumns))(position)

  def semanticCheck: SemanticCheck = {
    SemanticCheck.when(projectionType == StrictlyAdditiveProjection) {
      SemanticCheck.fromFunction((state: SemanticState) => {
        items.collectFirst {
          case AliasedReturnItem(_, variable) if state.currentScope.symbolNames contains variable.name =>
            SemanticCheckResult.error(state, SemanticError.variableAlreadyDeclared(variable.name, variable.position))
        }.getOrElse(SemanticCheckResult.success(state))
      })
    } chain items.semanticCheck
  }

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
          declareVariable(variable, types(item.expression), None, overriding = true)
        case None => (state: SemanticState) => SemanticCheckResult(state, Seq.empty)
      }
    )

  def returnVariables: ReturnVariables = ReturnVariables(includeExisting, items.flatMap(_.alias))

  def containsAggregate: Boolean = items.exists(_.expression.containsAggregate)

  def directlyContainsAggregate: Boolean = items.exists(_.directlyContainsAggregate)

  def isSimple: Boolean = items.forall(_.expression.isSimple)

  /*
   * Users must specify return items for the projection, either all variables (*), no variables (-), or explicit expressions.
   * Neo4j does not support the no variables case on the surface, but it may appear as the result of expanding the star (*) when no variables are in scope.
   * This field is true if the dash (-) was used by a user.
   */
  val includeExisting: Boolean = projectionType == AdditiveProjection || projectionType == StrictlyAdditiveProjection
}

sealed trait ReturnItem extends ASTNode with SemanticCheckable {
  def expression: Expression
  def alias: Option[LogicalVariable]
  def name: String
  def isPassThrough: Boolean = alias.contains(expression)

  def semanticCheck: SemanticCheck =
    SemanticExpressionCheck.check(Expression.SemanticContext.Results, expression)

  def stringify(expressionStringifier: ExpressionStringifier): String

  def withName(name: LogicalVariable)(position: InputPosition): ReturnItem

  def directlyContainsAggregate: Boolean = {
    expression.folder.treeFold(false) {
      case IsAggregate(_)            => _ => SkipChildren(true)
      case _: FullSubqueryExpression => _ => SkipChildren(false)
      case _                         => x => TraverseChildren(x)
    }
  }

}

sealed trait ProjectionType

// Do not include existing variables (unless explicitly listed in the return items)
// WITH x, YIELD x, RETURN x, SKIP ..., LIMIT ..., ORDER BY ..., FILTER ...
case object FreeProjection extends ProjectionType

// Include existing variables, allow override
// WITH *, YIELD *, RETURN *
case object AdditiveProjection extends ProjectionType

// Including existing variables, fail on override
// LET ...
case object StrictlyAdditiveProjection extends ProjectionType

case class UnaliasedReturnItem(expression: Expression, inputText: String)(val position: InputPosition)
    extends ReturnItem {

  val alias: Option[LogicalVariable] = expression match {
    case i: LogicalVariable => Some(i.copyId)
    case x: MapProjection   => Some(x.name.copyId)
    case _                  => None
  }
  val name: String = alias.map(_.name) getOrElse { inputText.trim }
  private val groupingNameLazy: LazyVal[String] = LazyVal(ExpressionStringifier()(expression))
  def groupingName: String = groupingNameLazy.value

  override def asCanonicalStringVal: String = expression.asCanonicalStringVal

  def stringify(expressionStringifier: ExpressionStringifier): String = expressionStringifier(expression)

  override def withName(name: LogicalVariable)(position: InputPosition): ReturnItem =
    AliasedReturnItem(expression, name)(position)

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

  override def withName(name: LogicalVariable)(position: InputPosition): ReturnItem =
    AliasedReturnItem(expression, name)(position)
}

object ReturnItems {

  /**
   * This is a subset of the information of [[ReturnItems]].
   * It only tracks the returned variables, but not aliases and other things.
   */
  case class ReturnVariables(
    includeExisting: Boolean,
    explicitVariables: Seq[LogicalVariable]
  ) {

    def merge(other: ReturnVariables): ReturnVariables = {
      ReturnVariables(includeExisting || other.includeExisting, (explicitVariables ++ other.explicitVariables).distinct)
    }
  }

  object ReturnVariables {
    def empty: ReturnVariables = ReturnVariables(includeExisting = false, Seq.empty)
  }
}
