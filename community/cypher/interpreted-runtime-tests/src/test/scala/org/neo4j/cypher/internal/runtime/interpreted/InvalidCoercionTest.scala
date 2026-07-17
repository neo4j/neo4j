/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.CoerceTo
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.operations.CypherCoercions
import org.neo4j.exceptions.CypherTypeException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.ByteArrayType

class InvalidCoercionTest extends InterpretedRuntimeTestSuite {

  test("testGqlInfoInInvalidCoerceToNeo4jType") {
    val exception = intercept[CypherTypeException] {
      CoerceTo.toNeo4jType(PropertyValueType(isNullable = false)(InputPosition.NONE))
    }
    exception.gqlStatus() should be("22G03")
    exception.statusDescription() should be("error: data exception - invalid value type")

    exception.cause().isEmpty should be(false)
    val exceptionCause = exception.cause().get()
    exceptionCause.gqlStatus() should be("22N37")
    exceptionCause.statusDescription() should be(
      "error: data exception - invalid coercion. Cannot coerce  to PROPERTY VALUE."
    )

    exceptionCause.cause().isEmpty should be(true)
  }

  test("testGqlInfoInInvalidCoercionFromType") {
    val exception = intercept[CypherTypeException] {
      CypherCoercions.coercerFromType(new ByteArrayType())
    }
    exception.gqlStatus() should be("22G03")
    exception.statusDescription() should be("error: data exception - invalid value type")

    exception.cause().isEmpty should be(false)
    val exceptionCause = exception.cause().get()
    exceptionCause.gqlStatus() should be("22N37")
    exceptionCause.statusDescription() should be(
      "error: data exception - invalid coercion. Cannot coerce  to BYTEARRAY."
    )

    exceptionCause.cause().isEmpty should be(true)
  }
}
