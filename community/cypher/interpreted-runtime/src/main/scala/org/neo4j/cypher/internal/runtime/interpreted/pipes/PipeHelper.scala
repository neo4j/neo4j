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
package org.neo4j.cypher.internal.runtime.interpreted.pipes

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NumericHelper
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.values.storable.FloatingPointValue

object PipeHelper {
  /**
   * Evaluate a statically known expressions. Assert that it is a long value.
   *
   * @param exp the expression
   * @param state the query state
   * @param prefix a prefix for error messages
   * @param suffix a suffix for error messages
   * @return the number
   */
  def evaluateStaticLongOrThrow(exp: Expression,
                                state: QueryState,
                                prefix: String,
                                suffix: String
                               ): Long = {
    val number = NumericHelper.evaluateStaticallyKnownNumber(exp, state)
    if (number.isInstanceOf[FloatingPointValue]) {
      val n = number.doubleValue()
      throw new InvalidArgumentException(s"$prefix: Invalid input. '$n' is not a valid value.$suffix")
    }
    number.longValue()
  }
}
