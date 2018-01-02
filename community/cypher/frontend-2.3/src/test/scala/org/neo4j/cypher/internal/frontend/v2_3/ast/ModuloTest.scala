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
package org.neo4j.cypher.internal.frontend.v2_3.ast

import org.neo4j.cypher.internal.frontend.v2_3.DummyPosition
import org.neo4j.cypher.internal.frontend.v2_3.symbols._

class ModuloTest extends InfixExpressionTestBase(Modulo(_, _)(DummyPosition(0))) {

  // Infix specializations:
  // 1 % 1 => 0
  // 1 % 1.1 => 1.0
  // 1.1 % 1 => 0.1
  // 1.1 % 1.1 => 0.0

  test("should handle all specializations") {
    testValidTypes(CTInteger, CTInteger)(CTInteger)
    testValidTypes(CTInteger, CTFloat)(CTFloat)
    testValidTypes(CTFloat, CTInteger)(CTFloat)
    testValidTypes(CTFloat, CTFloat)(CTFloat)
  }

  test("should handle combined specializations") {
    testValidTypes(CTFloat | CTInteger, CTFloat | CTInteger)(CTFloat | CTInteger)
  }

  test("should fail type check when adding incompatible") {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Float or Integer but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected Float or Integer but was Boolean"
    )
  }
}
