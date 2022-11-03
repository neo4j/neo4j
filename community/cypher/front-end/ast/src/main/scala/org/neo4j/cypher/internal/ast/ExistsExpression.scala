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

import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

sealed trait ExistsExpression
    extends ScopeExpression with BooleanExpression with SubqueryExpression

case class FullExistsExpression(query: Query)(
  val position: InputPosition,
  val introducedVariables: Set[LogicalVariable],
  val scopeDependencies: Set[LogicalVariable]
) extends ExistsExpression with ExpressionWithComputedDependencies {

  self =>

  override def withIntroducedVariables(introducedVariables: Set[LogicalVariable]): ExpressionWithComputedDependencies =
    copy()(position, introducedVariables = introducedVariables, scopeDependencies)

  override def withScopeDependencies(scopeDependencies: Set[LogicalVariable]): ExpressionWithComputedDependencies =
    copy()(position, introducedVariables, scopeDependencies = scopeDependencies)

  override def subqueryAstNode: ASTNode = query

  override def dup(children: Seq[AnyRef]): this.type = {
    FullExistsExpression(
      children.head.asInstanceOf[Query]
    )(position, introducedVariables, scopeDependencies).asInstanceOf[this.type]
  }
}

case class SimpleExistsExpression(pattern: Pattern, maybeWhere: Option[Where])(
  val position: InputPosition,
  val introducedVariables: Set[LogicalVariable],
  val scopeDependencies: Set[LogicalVariable]
) extends ExistsExpression with ExpressionWithComputedDependencies {

  self =>

  override def withIntroducedVariables(introducedVariables: Set[LogicalVariable]): ExpressionWithComputedDependencies =
    copy()(position, introducedVariables = introducedVariables, scopeDependencies)

  override def withScopeDependencies(scopeDependencies: Set[LogicalVariable]): ExpressionWithComputedDependencies =
    copy()(position, introducedVariables, scopeDependencies = scopeDependencies)

  override def subqueryAstNode: ASTNode = pattern

  override def dup(children: Seq[AnyRef]): this.type = {
    SimpleExistsExpression(
      children.head.asInstanceOf[Pattern],
      children(1).asInstanceOf[Option[Where]]
    )(position, introducedVariables, scopeDependencies).asInstanceOf[this.type]
  }
}
