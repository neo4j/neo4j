/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.PipelineInformation
import org.neo4j.cypher.internal.frontend.v3_3.InternalException
import org.neo4j.cypher.internal.frontend.v3_3.test_helpers.CypherFunSuite

class PrimitiveExecutionContextTest extends CypherFunSuite {

  private def longSize(s: Int) = new PipelineInformation(Map.empty, s, 0)

  test("copy fills upp the first few elements") {
    val input = PrimitiveExecutionContext(longSize(2))
    val result = PrimitiveExecutionContext(longSize(4))

    input.setLongAt(0, 42)
    input.setLongAt(1, 666)

    result.copyFrom(input)

    result.getLongAt(0) should equal(42)
    result.getLongAt(1) should equal(666)
  }

  test("copy fails if copy from larger") {
    val input = PrimitiveExecutionContext(longSize(4))
    val result = PrimitiveExecutionContext(longSize(2))

    intercept[InternalException](result.copyFrom(input))
  }

}
