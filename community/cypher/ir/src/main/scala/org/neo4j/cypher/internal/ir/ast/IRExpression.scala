/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir.ast

import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.util.InputPosition

/**
 * An Expression that contains a subquery, represented in IR.
 * @param query the query
 * @param solvedExpressionAsString the prettified string of the expression this subquery is solving.
 *                                 This is needed for EXPLAIN.
 */
abstract class IRExpression(
  val query: PlannerQuery,
  solvedExpressionAsString: String
) extends ScopeExpression {

  override def asCanonicalStringVal: String = solvedExpressionAsString

  // There is no way of sensibly computing the introduced variables, so let's throw an exception
  // if this is called on an IRExpression
  override def introducedVariables: Set[LogicalVariable] =
    throw new UnsupportedOperationException("Must not call introducedVariables on IRExpression")

  // When IRExpression supports Union subqueries, this implementation needs to change.
  override def scopeDependencies: Set[LogicalVariable] =
    query.query.asSinglePlannerQuery.queryGraph.argumentIds.map(Variable(_)(InputPosition.NONE))
}
