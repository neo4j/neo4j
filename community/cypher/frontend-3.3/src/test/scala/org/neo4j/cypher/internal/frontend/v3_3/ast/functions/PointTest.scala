/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_3.ast.functions

import org.neo4j.cypher.internal.frontend.v3_3.symbols._

class PointTest extends FunctionTestBase("point")  {

  test("should accept correct types") {
    testValidTypes(CTMap)(CTPoint)
  }

  test("should fail type check for incompatible arguments") {
    // TODO: Fix error message which seems to assume that Node and Relationships are acceptable maps
    testInvalidApplication(CTList(CTAny))(
      "Type mismatch: expected Map, Node or Relationship but was List<Any>"
    )
    testInvalidApplication(CTString)(
      "Type mismatch: expected Map, Node or Relationship but was String"
    )
  }

  test("should fail if wrong number of arguments") {
    testInvalidApplication()(
      "Insufficient parameters for function 'point'"
    )
    testInvalidApplication(CTMap, CTMap)(
      "Too many parameters for function 'point'"
    )
  }
}
