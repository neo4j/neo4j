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

import org.neo4j.cypher.internal.expressions.ExpressionWithComputedDependencies
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.util.InputPosition

/**
 * An Expression that contains an existential subquery, represented in IR.
 */
case class ExistsIRExpression(
  override val query: PlannerQuery,
  existsVariableName: String,
  solvedExpressionAsString: String
)(
  val position: InputPosition,
  override val introducedVariables: Set[LogicalVariable],
  override val scopeDependencies: Set[LogicalVariable]
) extends IRExpression(query, solvedExpressionAsString)(introducedVariables, scopeDependencies) {

  self =>

  override def withIntroducedVariables(introducedVariables: Set[LogicalVariable]): ExpressionWithComputedDependencies =
    copy()(position, introducedVariables = introducedVariables, scopeDependencies)

  override def withScopeDependencies(scopeDependencies: Set[LogicalVariable]): ExpressionWithComputedDependencies =
    copy()(position, introducedVariables, scopeDependencies = scopeDependencies)

  override def dup(children: Seq[AnyRef]): this.type = {
    ExistsIRExpression(
      children.head.asInstanceOf[PlannerQuery],
      children(1).asInstanceOf[String],
      children(2).asInstanceOf[String]
    )(position, introducedVariables, scopeDependencies).asInstanceOf[this.type]
  }
}
