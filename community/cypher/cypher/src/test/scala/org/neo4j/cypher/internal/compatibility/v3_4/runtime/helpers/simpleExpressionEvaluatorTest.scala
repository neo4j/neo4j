/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers

import org.neo4j.cypher.internal.util.v3_4.DummyPosition
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.helpers.simpleExpressionEvaluator.isNonDeterministic
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.v3_4.expressions.{FunctionInvocation, FunctionName}

class SimpleExpressionEvaluatorTest extends CypherFunSuite {
  private val pos = DummyPosition(-1)

  test("isNonDeterministic should not care about capitalization") {
    isNonDeterministic(
      FunctionInvocation(FunctionName("ranD")(pos), distinct = false, IndexedSeq.empty)(pos)) shouldBe true
    isNonDeterministic(
      FunctionInvocation(FunctionName("Timestamp")(pos), distinct = false, IndexedSeq.empty)(pos)) shouldBe true
  }
}
