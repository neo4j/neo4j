/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticCheckResult.success
import org.neo4j.cypher.internal.frontend.v3_4.semantics._
import org.neo4j.cypher.internal.util.v3_4.{ASTNode, InputPosition, InternalException}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression, LogicalVariable, MapProjection}

sealed trait ReturnItemsDef extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {
  /**
    * Users must specify return items for the projection, either all variables (*), no variables (-), or explicit expressions.
    * Neo4j does not support the no variables case on the surface, but it may appear as the result of expanding the star (*) when no variables are in scope.
    * This field is true if the dash (-) was used by a user.
    */
  def includeExisting: Boolean
  def declareVariables(previousScope: Scope): SemanticCheck
  def containsAggregate: Boolean
  def withExisting(includeExisting: Boolean): ReturnItemsDef
  def items: Seq[ReturnItem]

  def isStarOnly: Boolean = includeExisting && items.isEmpty
}

final case class DiscardCardinality()(val position: InputPosition) extends ReturnItemsDef {
  override def includeExisting: Boolean = false
  override def semanticCheck: SemanticCheck = _success
  override def items: Seq[ReturnItem] = Seq.empty
  override def declareVariables(previousScope: Scope): (SemanticState) => SemanticCheckResult = _success
  override def containsAggregate = false
  override def withExisting(includeExisting: Boolean): DiscardCardinality = this
  private def _success(s: SemanticState) = success(s)
}

final case class ReturnItems(
                              includeExisting: Boolean,
                              items: Seq[ReturnItem]
                            )(val position: InputPosition) extends ReturnItemsDef with SemanticAnalysisTooling {

  override def withExisting(includeExisting: Boolean): ReturnItemsDef =
    copy(includeExisting = includeExisting)(position)

  override def semanticCheck: SemanticCheck = items.semanticCheck chain ensureProjectedToUniqueIds

  def aliases: Set[LogicalVariable] = items.flatMap(_.alias).toSet

  def passedThrough: Set[LogicalVariable] = items.collect {
    case item => item.alias.collect { case ident if ident == item.expression => ident }
  }.flatten.toSet

  def mapItems(f: Seq[ReturnItem] => Seq[ReturnItem]): ReturnItems =
    copy(items = f(items))(position)

  override def declareVariables(previousScope: Scope): SemanticCheck =
    when (includeExisting) {
      s => success(s.importValuesFromScope(previousScope))
    } chain items.foldSemanticCheck(item => item.alias match {
      case Some(variable) if item.expression == variable =>
        val positions = previousScope.symbol(variable.name).fold(Set.empty[InputPosition])(_.positions)
        declareVariable(variable, types(item.expression), positions)
      case Some(variable) => declareVariable(variable, types(item.expression))
      case None           => (state) => SemanticCheckResult(state, Seq.empty)
    })

  private def ensureProjectedToUniqueIds: SemanticCheck = {
    items.groupBy(_.name).foldLeft(success) {
       case (acc, (k, items)) if items.size > 1 =>
        acc chain SemanticError("Multiple result columns with the same name are not supported", items.head.position)
       case (acc, _) =>
         acc
    }
  }

  override def containsAggregate = items.exists(_.expression.containsAggregate)
}

sealed trait ReturnItem extends ASTNode with SemanticCheckable {
  def expression: Expression
  def alias: Option[LogicalVariable]
  def name: String
  def makeSureIsNotUnaliased(state: SemanticState): SemanticCheckResult

  def semanticCheck = SemanticExpressionCheck.check(Expression.SemanticContext.Results, expression)
}

case class UnaliasedReturnItem(expression: Expression, inputText: String)(val position: InputPosition) extends ReturnItem {
  val alias = expression match {
    case i: LogicalVariable => Some(i.bumpId)
    case x: MapProjection => Some(x.name.bumpId)
    case _ => None
  }
  val name = alias.map(_.name) getOrElse { inputText.trim }

  def makeSureIsNotUnaliased(state: SemanticState): SemanticCheckResult =
    throw new InternalException("Should have been aliased before this step")
}

object AliasedReturnItem {
  def apply(v:LogicalVariable):AliasedReturnItem = AliasedReturnItem(v.copyId, v.copyId)(v.position)
}

//TODO variable should not be a Variable. A Variable is an expression, and the return item alias isn't
case class AliasedReturnItem(expression: Expression, variable: LogicalVariable)(val position: InputPosition) extends ReturnItem {
  val alias = Some(variable)
  val name = variable.name

  def makeSureIsNotUnaliased(state: SemanticState): SemanticCheckResult = success(state)
}
