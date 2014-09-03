/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.compiler.v2_2._

sealed trait ReturnItems extends ASTNode with ASTPhrase with SemanticCheckable {
  def declareIdentifiers(currentState: SemanticState): SemanticCheck
  def containsAggregate: Boolean
}

case class ListedReturnItems(items: Seq[ReturnItem])(val position: InputPosition) extends ReturnItems {
  def semanticCheck = items.semanticCheck chain
    ensureProjectedToUniqueIds

  def declareIdentifiers(currentState: SemanticState) =
    items.foldSemanticCheck(item =>
      Identifier(item.name)(null).declare(item.expression.types(currentState))
    )

  private def ensureProjectedToUniqueIds: SemanticCheck = {
    items.groupBy(_.name).foldLeft(SemanticCheckResult.success) {
       case (acc, (k, items)) if items.size > 1 =>
        acc chain SemanticError("Multiple result columns with the same name are not supported", items.head.position)
       case (acc, _) =>
         acc
    }
  }

  def containsAggregate = this.exists {
    case IsAggregate(_) => true
  }
}

case class ReturnAll()(val position: InputPosition) extends ReturnItems {
  var seenIdentifiers: Option[Set[String]] = None

  def semanticCheck =
    updateSeenIdentifiers

  private def updateSeenIdentifiers = (s: SemanticState) => {
    seenIdentifiers = Some(s.currentScope.symbolNames)
    SemanticCheckResult.success(s)
  }

  def declareIdentifiers(currentState: SemanticState) = s =>
    SemanticCheckResult.success(s.importScope(currentState.currentScope))

  def containsAggregate = false
}

sealed trait ReturnItem extends ASTNode with ASTPhrase with SemanticCheckable {
  def expression: Expression
  def alias: Option[Identifier]
  def name: String

  def semanticCheck = expression.semanticCheck(Expression.SemanticContext.Results)
}

case class UnaliasedReturnItem(expression: Expression, inputText: String)(val position: InputPosition) extends ReturnItem {
  val alias = expression match {
    case i: Identifier => Some(i)
    case _ => None
  }
  val name = alias.map(_.name) getOrElse { inputText.trim }
}

case class AliasedReturnItem(expression: Expression, identifier: Identifier)(val position: InputPosition) extends ReturnItem {
  val alias = Some(identifier)
  val name = identifier.name
}
