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

import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.symbols.CTBoolean

case class Where(expression: Expression)(val position: InputPosition)
    extends ASTNode with SemanticCheckable with HasMappableExpressions[Where] {

  def dependencies: Set[LogicalVariable] = expression.dependencies

  def semanticCheck: SemanticCheck = Where.checkExpression(expression)

  override def mapExpressions(f: Expression => Expression): Where = copy(f(expression))(this.position)
}

object Where {

  def combineOrCreateBeforeCnf(
    oldWhere: Option[Where],
    maybePredicate: Option[Expression]
  )(position: InputPosition): Option[Where] =
    Where.combineOrCreateExpressionBeforeCnf(oldWhere.map(_.expression), maybePredicate)(Some(position))
      .map(newPredicate => Where(newPredicate)(position))

  def combineOrCreateExpressionBeforeCnf(
    oldPredicate: Option[Expression],
    maybePredicate: Option[Expression]
  )(position: Option[InputPosition]): Option[Expression] =
    (oldPredicate, maybePredicate) match {
      case (Some(oldPredicate), Some(newPredicate)) =>
        Some(And(oldPredicate, newPredicate)(position.get))

      case (None, newPredicate) =>
        newPredicate

      case (oldPredicate, None) => oldPredicate
    }

  def combineOrCreate(
    oldWhere: Option[Where],
    addedPredicates: ListSet[Expression]
  )(position: InputPosition): Option[Where] =
    Where.combineOrCreate(oldWhere.map(_.expression), addedPredicates)
      .map(newPredicate => Where(newPredicate)(position))

  def combineOrCreate(oldWhere: Option[Expression], addedPredicates: ListSet[Expression]): Option[Expression] =
    oldWhere match {
      case Some(Ands(oldPredicates)) =>
        Some(Ands.create(addedPredicates ++ oldPredicates))
      case Some(oldPredicate) =>
        Some(Ands.create(addedPredicates + oldPredicate))
      case None if addedPredicates.nonEmpty =>
        Some(Ands.create(addedPredicates))
      case None =>
        None
    }

  def checkExpression(expression: Expression): SemanticCheck =
    SemanticExpressionCheck.simple(expression) chain
      SemanticPatternCheck.checkValidPropertyKeyNames(
        expression.folder.findAllByClass[Property].map(prop => prop.propertyKey)
      ) chain
      SemanticExpressionCheck.expectType(CTBoolean.covariant, expression)
}
