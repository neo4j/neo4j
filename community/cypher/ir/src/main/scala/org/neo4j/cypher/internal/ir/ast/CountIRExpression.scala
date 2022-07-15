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

import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.util.InputPosition

/**
 * An Expression that contains a COUNT subquery, represented in IR.
 */
case class CountIRExpression(
  override val query: PlannerQuery,
  countVariableName: String,
  solvedExpressionAsString: String
)(val position: InputPosition)
    extends IRExpression(query, solvedExpressionAsString) {

  def renameCountVariable(newName: String): CountIRExpression = {
    copy(
      countVariableName = newName,
      query = query.copy(
        query.query.asSinglePlannerQuery.withHorizon(
          AggregatingQueryProjection(aggregationExpressions = Map(newName -> CountStar()(position)))
        )
      )
    )(position)
  }
}
