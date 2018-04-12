/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_4

import org.neo4j.cypher.internal.runtime.InternalExecutionResult
import org.neo4j.cypher.{CypherTypeException, ExecutionEngineFunSuite}
import org.neo4j.kernel.impl.util.ValueUtils

class TypeConversionTest extends ExecutionEngineFunSuite {
  test("should not allow adding node and number") {
    val x = createNode()
    val failure = intercept[CypherTypeException] {
      val result = execute("debug=generate_java_source debug=show_java_source profile match (n) return n + {x} as res", "x" -> 5)
      // should not get here, if we do, this is for debugging:
      println(result.executionPlanDescription())
    }

    failure.getMessage should equal(s"Don't know how to add `${ValueUtils.of(x)}` and `5`")
  }

  test("shouldHandlePatternMatchingWithParameters") {
    val x = createNode()

    val result = execute("match (x) where x = {startNode} return x", "startNode" -> x)

    result.toList should equal(List(Map("x" -> x)))
  }

  override def execute(q: String, params: (String, Any)*): InternalExecutionResult =
    super.execute(s"cypher runtime=compiled $q", params:_*)
}
