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

class GreaterThanOrEqualTest extends InfixExpressionTestBase(GreaterThanOrEqual(_, _)(DummyPosition(0))) {

  @Test
  def shouldSupportComparingIntegers() {
    testValidTypes(CTInteger, CTInteger)(CTBoolean)
  }

  @Test
  def shouldSupportComparingDoubles() {
    testValidTypes(CTFloat, CTFloat)(CTBoolean)
  }

  @Test
  def shouldSupportComparingStrings() {
    testValidTypes(CTString, CTString)(CTBoolean)
  }

  @Test
  def shouldReturnErrorIfInvalidArgumentTypes() {
    testInvalidApplication(CTNode, CTInteger)("Type mismatch: expected Float, Integer or String but was Node")
    testInvalidApplication(CTInteger, CTNode)("Type mismatch: expected Integer but was Node")
  }
}
