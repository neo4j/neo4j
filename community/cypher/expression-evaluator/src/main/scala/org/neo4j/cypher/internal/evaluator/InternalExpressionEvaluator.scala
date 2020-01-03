/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.evaluator

import org.neo4j.values.AnyValue

/**
  * Evaluates an arbitrary Cypher expression
  */
trait InternalExpressionEvaluator {

  /**
    * Evaluates a Cypher expression provided as a String to an instance of [[AnyValue]]
    *
    * @param expression The cypher expression string
    * @return An instance of [[AnyValue]] corresponding to the provided expression string
    * @throws EvaluationException if evaluation fails
    */
  @throws(classOf[EvaluationException])
  def evaluate(expression: String): AnyValue
}


