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
package org.neo4j.cypher.internal.compiler.v2_1.functions

import org.neo4j.cypher.internal.compiler.v2_1._
import symbols._
import org.junit.Test
import org.neo4j.cypher.internal.compiler.v2_1.ast.Expression.SemanticContext

class PercentileContTest extends FunctionTestBase("percentileCont") {

  override val context = SemanticContext.Results

  @Test
  def shouldHandleAllSpecializations() {
    testValidTypes(CTInteger, CTInteger)(CTDouble)
    testValidTypes(CTInteger, CTDouble)(CTDouble)
    testValidTypes(CTDouble, CTInteger)(CTDouble)
    testValidTypes(CTDouble, CTDouble)(CTDouble)
  }

  @Test
  def shouldHandleCombinedSpecializations() {
    testValidTypes(CTDouble | CTInteger, CTDouble | CTInteger)(CTDouble)
  }

  @Test
  def shouldFailIfWrongArguments() {
    testInvalidApplication(CTDouble)("Insufficient parameters for function 'percentileCont'")
    testInvalidApplication(CTDouble, CTDouble, CTDouble)("Too many parameters for function 'percentileCont'")
  }

  @Test
  def shouldFailTypeCheckWhenAddingIncompatible() {
    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Double but was Boolean"
    )
    testInvalidApplication(CTBoolean, CTInteger)(
      "Type mismatch: expected Double but was Boolean"
    )
  }
}
