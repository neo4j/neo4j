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

class OrTest extends InfixExpressionTestBase(Or(_, _)(DummyPosition(0))) {

  test("shouldCombineBooleans") {
    testValidTypes(CTBoolean, CTBoolean)(CTBoolean)
  }

  test("shouldCoerceArguments") {
    testInvalidApplication(CTInteger, CTBoolean)("Type mismatch: expected Boolean but was Integer")
    testInvalidApplication(CTBoolean, CTInteger)("Type mismatch: expected Boolean but was Integer")
  }

  test("shouldReturnErrorIfInvalidArgumentTypes") {
    testInvalidApplication(CTNode, CTBoolean)("Type mismatch: expected Boolean but was Node")
    testInvalidApplication(CTBoolean, CTNode)("Type mismatch: expected Boolean but was Node")
  }
}
