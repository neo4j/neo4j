/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_4.helpers

import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.QueryGraph

class CachedFunctionTest extends CypherFunSuite {
  test("does not re-calculate stuff") {
    val f: QueryGraph => Unit = mock[QueryGraph => Unit]

    val cachedF = CachedFunction(f)

    val qg1 = QueryGraph(patternNodes = Set("a"))
    val qg2 = QueryGraph(patternNodes = Set("a"))

    cachedF(qg1)
    cachedF(qg2)

    verify(f, times(1)).apply(any())
  }
}
