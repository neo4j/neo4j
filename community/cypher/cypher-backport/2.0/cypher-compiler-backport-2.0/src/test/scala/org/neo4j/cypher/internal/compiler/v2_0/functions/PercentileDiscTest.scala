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
package org.neo4j.cypher.internal.compiler.v2_0.functions

import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_0.ast.Expression.SemanticContext

class PercentileDiscTest extends FunctionTestBase("percentileDisc") {

  override val context = SemanticContext.Results

  @Test
  def shouldHandleAllSpecializations() {
    testValidTypes(CTInteger, CTInteger)(CTInteger)
    testValidTypes(CTInteger, CTFloat)(CTInteger)
    testValidTypes(CTFloat, CTInteger)(CTFloat)
    testValidTypes(CTFloat, CTFloat)(CTFloat)
  }

  @Test
  def shouldHandleCombinedSpecializations() {
    testValidTypes(CTFloat | CTInteger, CTFloat | CTInteger)(CTInteger | CTFloat)
  }

  @Test
  def shouldFailIfWrongArguments() {
    testInvalidApplication(CTFloat)("Insufficient parameters for function 'percentileDisc'")
    testInvalidApplication(CTFloat, CTFloat, CTFloat)("Too many parameters for function 'percentileDisc'")
  }

  @Test
  def shouldFailTypeCheckWhenAddingIncompatible() {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Float but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected Float or Integer but was Boolean"
    )
  }
}
