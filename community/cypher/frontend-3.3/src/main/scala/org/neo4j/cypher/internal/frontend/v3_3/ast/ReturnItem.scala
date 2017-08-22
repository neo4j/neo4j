/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3._

case class ReturnItems(includeExisting: Boolean, items: Seq[ReturnItem])(val position: InputPosition)
    extends ASTNode
    with ASTPhrase
    with SemanticCheckable
    with SemanticChecking {
  def semanticCheck =
    items.semanticCheck chain
      ensureProjectedToUniqueIds

  def aliases: Set[Variable] = items.flatMap(_.alias).toSet

  def passedThrough: Set[Variable] =
    items
      .collect {
        case item => item.alias.collect { case ident if ident == item.expression => ident }
      }
      .flatten
      .toSet

  def mapItems(f: Seq[ReturnItem] => Seq[ReturnItem]) = copy(items = f(items))(position)

  def declareVariables(previousScope: Scope) =
    when(includeExisting) { s =>
      SemanticCheckResult.success(s.importScope(previousScope))
    } chain items.foldSemanticCheck(item =>
      item.alias match {
        case Some(variable) if item.expression == variable =>
          val positions = previousScope.symbol(variable.name).fold(Set.empty[InputPosition])(_.positions)
          variable.declare(item.expression.types, positions)
        case Some(variable) => variable.declare(item.expression.types)
        case None =>
          (state) =>
            SemanticCheckResult(state, Seq.empty)
    })

  private def ensureProjectedToUniqueIds: SemanticCheck = {
    items.groupBy(_.name).foldLeft(SemanticCheckResult.success) {
      case (acc, (k, items)) if items.size > 1 =>
        acc chain SemanticError("Multiple result columns with the same name are not supported", items.head.position)
      case (acc, _) =>
        acc
    }
  }

  def containsAggregate = items.exists(_.expression.containsAggregate)
}

sealed trait ReturnItem extends ASTNode with ASTPhrase with SemanticCheckable {
  def expression: Expression
  def alias: Option[Variable]
  def name: String
  def makeSureIsNotUnaliased(state: SemanticState): SemanticCheckResult

  def semanticCheck = expression.semanticCheck(Expression.SemanticContext.Results)
}

case class UnaliasedReturnItem(expression: Expression, inputText: String)(val position: InputPosition)
    extends ReturnItem {
  val alias = expression match {
    case i: Variable      => Some(i.bumpId)
    case x: MapProjection => Some(x.name.bumpId)
    case _                => None
  }
  val name = alias.map(_.name) getOrElse { inputText.trim }

  def makeSureIsNotUnaliased(state: SemanticState): SemanticCheckResult =
    throw new InternalException("Should have been aliased before this step")
}

//TODO variable should not be a Variable. A Variable is an expression, and the return item alias isn't
case class AliasedReturnItem(expression: Expression, variable: Variable)(val position: InputPosition)
    extends ReturnItem {
  val alias = Some(variable)
  val name = variable.name

  def makeSureIsNotUnaliased(state: SemanticState): SemanticCheckResult = SemanticCheckResult.success(state)
}
