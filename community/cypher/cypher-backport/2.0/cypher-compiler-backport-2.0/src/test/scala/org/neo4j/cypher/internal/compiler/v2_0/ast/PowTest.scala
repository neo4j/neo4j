/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.functions.FunctionTestBase

class PowTest extends InfixExpressionTestBase(Pow(_, _)(DummyPosition(0))) {

  // Infix specializations:
  // 1 ^ 1 => 1
  // 1 ^ 1.1 => 1
  // 1.1 ^ 1 => 1.1
  // 1.1 ^ 1.1 => 1.1105

  @Test
  def shouldHandleAllSpecializations() {
    testValidTypes(CTInteger, CTInteger)(CTFloat)
    testValidTypes(CTInteger, CTFloat)(CTFloat)
    testValidTypes(CTFloat, CTInteger)(CTFloat)
    testValidTypes(CTFloat, CTFloat)(CTFloat)
  }

  @Test
  def shouldHandleCombinedSpecializations() {
    testValidTypes(CTFloat | CTInteger, CTFloat | CTInteger)(CTFloat)
  }

  @Test
  def shouldFailTypeCheckWhenAddingIncompatible() {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Float but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected Float but was Boolean"
    )
  }
}
