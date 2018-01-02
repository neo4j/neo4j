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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import org.neo4j.cypher.internal.compiler.v2_3.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.IdName
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class CachedFunctionTest extends CypherFunSuite {
  test("does not re-calculate stuff") {
    val f: QueryGraph => Unit = mock[QueryGraph => Unit]

    val cachedF = CachedFunction(f)

    val qg1 = QueryGraph(patternNodes = Set(IdName("a")))
    val qg2 = QueryGraph(patternNodes = Set(IdName("a")))

    cachedF(qg1)
    cachedF(qg2)

    verify(f, times(1)).apply(any())
  }
}
