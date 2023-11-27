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
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.util.InputPosition

/**
 * An Expression that contains an existential subquery, represented in IR.
 */
case class ExistsIRExpression(
  override val query: PlannerQuery,
  existsVariable: LogicalVariable,
  solvedExpressionAsString: String
)(
  val position: InputPosition,
  override val computedIntroducedVariables: Option[Set[LogicalVariable]],
  override val computedScopeDependencies: Option[Set[LogicalVariable]]
) extends IRExpression(query, solvedExpressionAsString)(computedIntroducedVariables, computedScopeDependencies) {

  self =>

  override def withComputedIntroducedVariables(computedIntroducedVariables: Set[LogicalVariable])
    : ExpressionWithComputedDependencies =
    copy()(position, computedIntroducedVariables = Some(computedIntroducedVariables), computedScopeDependencies)

  override def withComputedScopeDependencies(computedScopeDependencies: Set[LogicalVariable])
    : ExpressionWithComputedDependencies =
    copy()(position, computedIntroducedVariables, computedScopeDependencies = Some(computedScopeDependencies))

  override def dup(children: Seq[AnyRef]): this.type = {
    ExistsIRExpression(
      children.head.asInstanceOf[PlannerQuery],
      children(1).asInstanceOf[LogicalVariable],
      children(2).asInstanceOf[String]
    )(position, computedIntroducedVariables, computedScopeDependencies).asInstanceOf[this.type]
  }

  /**
   * The string comparison here is done so that comparisons on Windows machines work
   * the same as on Linux based ones.
   */
  override def equals(existsIRExpression: Any): Boolean = existsIRExpression match {
    case ee: ExistsIRExpression =>
      this.query.equals(ee.query) &&
      this.existsVariable.equals(ee.existsVariable) &&
      this.solvedExpressionAsString.replaceAll("\r\n", "\n")
        .equals(ee.solvedExpressionAsString.replaceAll("\r\n", "\n"))
    case _ => false
  }
}
