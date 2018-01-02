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

class AddTest extends InfixExpressionTestBase(Add(_, _)(DummyPosition(0))) {

  // Infix specializations:
  // "a" + "b" => "ab"
  // "a" + 1 => "a1"
  // "a" + 1.1 => "a1.1"
  // 1 + "b" => "1b"
  // 1 + 1 => 2
  // 1 + 1.1 => 2.1
  // 1.1 + "b" => "1.1b"
  // 1.1 + 1 => 2.1
  // 1.1 + 1.1 => 2.2
  // [a] + [b] => [a, b]
  // [a] + b => [a, b]
  // a + [b] => [a, b]

  test("shouldHandleAllSpecializations") {
    testValidTypes(CTString, CTString)(CTString)
    testValidTypes(CTString, CTInteger)(CTString)
    testValidTypes(CTString, CTFloat)(CTString)
    testValidTypes(CTInteger, CTString)(CTString)
    testValidTypes(CTInteger, CTInteger)(CTInteger)
    testValidTypes(CTInteger, CTFloat)(CTFloat)
    testValidTypes(CTFloat, CTString)(CTString)
    testValidTypes(CTFloat, CTInteger)(CTFloat)
    testValidTypes(CTFloat, CTFloat)(CTFloat)

    testValidTypes(CTCollection(CTNode), CTCollection(CTNode))(CTCollection(CTNode))
    testValidTypes(CTCollection(CTFloat), CTCollection(CTFloat))(CTCollection(CTFloat))

    testValidTypes(CTCollection(CTNode), CTNode)(CTCollection(CTNode))
    testValidTypes(CTCollection(CTFloat), CTFloat)(CTCollection(CTFloat))

    testValidTypes(CTNode, CTCollection(CTNode))(CTCollection(CTNode))
    testValidTypes(CTFloat, CTCollection(CTFloat))(CTCollection(CTFloat))
  }

  test("shouldHandleCombinedSpecializations") {
    testValidTypes(CTFloat | CTString, CTInteger)(CTFloat | CTString)
    testValidTypes(CTFloat | CTCollection(CTFloat), CTFloat)(CTFloat | CTCollection(CTFloat))
    testValidTypes(CTFloat, CTFloat | CTCollection(CTFloat))(CTFloat | CTCollection(CTFloat))
  }

  test("shouldHandleCoercions") {
    testValidTypes(CTCollection(CTFloat), CTInteger)(CTCollection(CTFloat))
    testValidTypes(CTFloat | CTCollection(CTFloat), CTInteger)(CTFloat | CTCollection(CTFloat))
  }

  test("shouldFailTypeCheckForIncompatibleArguments") {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Float, Integer, String or Collection<Integer> but was Boolean"
    )
    testInvalidApplication(CTCollection(CTInteger), CTString)(
      "Type mismatch: expected Integer, Collection<Integer> or Collection<Collection<Integer>> but was String"
    )
    testInvalidApplication(CTCollection(CTInteger), CTCollection(CTString))(
      "Type mismatch: expected Integer, Collection<Integer> or Collection<Collection<Integer>> but was Collection<String>"
    )
  }
}
