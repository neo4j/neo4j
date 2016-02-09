/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_0.ast.functions

import org.neo4j.cypher.internal.frontend.v3_0.symbols._

class DistanceTest extends FunctionTestBase("distance")  {

  test("should accept correct types") {
    val t1 = CTPoint
    testValidTypes(t1, CTPoint)(CTFloat)
  }

  test("should fail type check for incompatible arguments") {
    testInvalidApplication(CTList(CTAny), CTList(CTAny))(
      "Type mismatch: expected Point but was Collection<Any>"
    )
    testInvalidApplication(CTString, CTString)(
      "Type mismatch: expected Point but was String"
    )
  }

  test("should fail if wrong number of arguments") {
    testInvalidApplication()(
      "Insufficient parameters for function 'distance'"
    )
    testInvalidApplication(CTMap)(
      "Insufficient parameters for function 'distance'"
    )
    testInvalidApplication(CTPoint, CTPoint, CTPoint)(
      "Too many parameters for function 'distance'"
    )
  }
}
