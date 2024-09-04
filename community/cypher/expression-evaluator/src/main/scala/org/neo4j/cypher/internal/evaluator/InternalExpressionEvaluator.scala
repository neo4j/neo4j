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
package org.neo4j.cypher.internal.evaluator

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.MapValue

/**
 * Evaluates an arbitrary Cypher expression
 */
trait InternalExpressionEvaluator {

  /**
   * Evaluates a Cypher expression provided as a parsed [[Expression]] to an instance of [[AnyValue]]
   *
   * @param expression The cypher expression string
   * @param params The parameters in scope
   * @return An instance of [[AnyValue]] corresponding to the provided expression string
   * @throws EvaluationException if evaluation fails
   */
  @throws(classOf[EvaluationException])
  def evaluate(
    expression: Expression,
    params: MapValue = MapValue.EMPTY,
    context: CypherRow = CypherRow.empty
  ): AnyValue
}
