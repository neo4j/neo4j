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

class AddTest extends FunctionTestBase("+") {

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

  @Test
  def shouldHandleAllSpecializations() {
    testValidTypes(CTLong)(CTLong)
    testValidTypes(CTInteger)(CTInteger)
    testValidTypes(CTDouble)(CTDouble)

    testValidTypes(CTString, CTString)(CTString)
    testValidTypes(CTString, CTLong)(CTString)
    testValidTypes(CTString, CTDouble)(CTString)
    testValidTypes(CTLong, CTString)(CTString)
    testValidTypes(CTLong, CTLong)(CTLong)
    testValidTypes(CTLong, CTDouble)(CTDouble)
    testValidTypes(CTDouble, CTString)(CTString)
    testValidTypes(CTDouble, CTLong)(CTDouble)
    testValidTypes(CTDouble, CTDouble)(CTDouble)

    testValidTypes(CTCollection(CTNode), CTCollection(CTNode))(CTCollection(CTNode))
    testValidTypes(CTCollection(CTDouble), CTCollection(CTDouble))(CTCollection(CTDouble))

    testValidTypes(CTCollection(CTNode), CTNode)(CTCollection(CTNode))
    testValidTypes(CTCollection(CTDouble), CTDouble)(CTCollection(CTDouble))

    testValidTypes(CTNode, CTCollection(CTNode))(CTCollection(CTNode))
    testValidTypes(CTDouble, CTCollection(CTDouble))(CTCollection(CTDouble))
  }

  @Test
  def shouldHandleCombinedSpecializations() {
    testValidTypes(CTDouble | CTString, CTLong)(CTDouble | CTString)
    testValidTypes(CTDouble | CTCollection(CTDouble), CTDouble)(CTDouble | CTCollection(CTDouble))
    testValidTypes(CTDouble, CTDouble | CTCollection(CTDouble))(CTDouble | CTCollection(CTDouble))
  }

  @Test
  def shouldHandleCoercions() {
    testValidTypes(CTCollection(CTDouble), CTLong)(CTCollection(CTDouble))
    testValidTypes(CTDouble | CTCollection(CTDouble), CTLong)(CTDouble | CTCollection(CTDouble))
  }

  @Test
  def shouldFailIfWrongArguments() {
    testInvalidApplication()("Insufficient parameters for function '+'")
    testInvalidApplication(CTDouble, CTDouble, CTDouble)("Too many parameters for function '+'")
  }

  @Test
  def shouldFailTypeCheckForIncompatibleArguments() {
    testInvalidApplication(CTBoolean)(
      "Type mismatch: expected Double, Integer or Long but was Boolean"
    )

    testInvalidApplication(CTInteger, CTBoolean)(
      "Type mismatch: expected Double, Integer, Long, String or Collection<Integer> but was Boolean"
    )
    testInvalidApplication(CTCollection(CTInteger), CTString)(
      "Type mismatch: expected Integer, Collection<Integer> or Collection<Collection<Integer>> but was String"
    )
    testInvalidApplication(CTCollection(CTInteger), CTCollection(CTString))(
      "Type mismatch: expected Integer, Collection<Integer> or Collection<Collection<Integer>> but was Collection<String>"
    )
  }
}
