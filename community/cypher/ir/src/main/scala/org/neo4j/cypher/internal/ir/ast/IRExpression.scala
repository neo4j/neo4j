/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ir.ast

import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.util.ASTNode

/**
 * An Expression that contains a subquery, represented in IR.
 * @param query the query
 * @param solvedExpressionAsString the prettified string of the expression this subquery is solving.
 *                                 This is needed for EXPLAIN.
 */
abstract class IRExpression(
  val query: PlannerQuery,
  solvedExpressionAsString: String
)(
  override val computedIntroducedVariables: Option[Set[LogicalVariable]],
  override val computedScopeDependencies: Option[Set[LogicalVariable]]
) extends ScopeExpression with ExpressionWithComputedDependencies {

  override def asCanonicalStringVal: String = solvedExpressionAsString

  override def subqueryAstNode: ASTNode =
    throw new UnsupportedOperationException("Must not try to access an ASTNode on an IRExpression")
}
