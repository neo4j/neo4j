/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Test

class ModuloTest extends FunctionTestBase("%") {

  // Infix specializations:
  // 1 % 1 => 0
  // 1 % 1.1 => 1.0
  // 1.1 % 1 => 0.1
  // 1.1 % 1.1 => 0.0

  @Test
  def shouldHandleAllSpecializations() {
    testValidTypes(CTLong, CTLong)(CTLong)
    testValidTypes(CTLong, CTDouble)(CTDouble)
    testValidTypes(CTDouble, CTLong)(CTDouble)
    testValidTypes(CTDouble, CTDouble)(CTDouble)
  }

  @Test
  def shouldHandleCombinedSpecializations() {
    testValidTypes(CTDouble | CTLong, CTDouble | CTLong)(CTDouble | CTLong)
  }

  @Test
  def shouldFailIfWrongArguments() {
    testInvalidApplication(CTDouble)("Insufficient parameters for function '%'")
    testInvalidApplication(CTDouble, CTDouble, CTDouble)("Too many parameters for function '%'")
  }

  @Test
  def shouldFailTypeCheckWhenAddingIncompatible() {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Double, Integer or Long but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected Double, Integer or Long but was Boolean"
    )
  }
}
