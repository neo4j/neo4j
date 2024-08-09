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
package org.neo4j.cypher.internal.planning

import org.neo4j.common.EntityType
import org.neo4j.common.TokenNameLookup
import org.neo4j.cypher.internal.macros.TranslateExceptionMacros.translateException
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.KernelException
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException
import org.neo4j.kernel.api.exceptions.Status

/**
 * This test class lives here instead of in the same module as [[org.neo4j.cypher.internal.macros.TranslateExceptionMacros]],
 * since we cannot easily import the necessary classes in the macro expansion module,
 * without having problems with compilation order of macros and calling code.
 */
class TranslateExceptionMacrosTest extends CypherFunSuite {

  class MyKernelException(message: String) extends KernelException(null: Status, message) {

    override def getUserMessage(tokenNameLookup: TokenNameLookup): String =
      s"$message with ${tokenNameLookup.propertyKeyGetName(1)}"
  }

  val tokenNameLookup: TokenNameLookup = new TokenNameLookup {
    def propertyKeyGetName(propertyKeyId: Int): String = s"prop$propertyKeyId"
    def labelGetName(labelId: Int): String = ""
    def relationshipTypeGetName(relTypeId: Int): String = ""
  }

  test("should not do anything if no exception thrown") {
    translateException(tokenNameLookup, 42) should be(42)
  }

  test("should rethrow KernelException as CypherExecutionException") {
    val exception = new EntityNotFoundException(EntityType.NODE, "1")
    the[CypherExecutionException] thrownBy translateException(
      tokenNameLookup,
      throw exception
    ) should have message exception.getMessage
  }

  test("should rethrow KernelException as CypherExecutionException and use getUserMessage") {
    val exception = new MyKernelException("kaboom")
    the[CypherExecutionException] thrownBy translateException(
      tokenNameLookup,
      throw exception
    ) should have message "kaboom with prop1"
  }

  test("should rethrow ConstraintViolationException") {
    val exception = new org.neo4j.graphdb.ConstraintViolationException("foo")
    the[org.neo4j.exceptions.ConstraintViolationException] thrownBy translateException(
      tokenNameLookup,
      throw exception
    ) should have message exception.getMessage
  }

  test("should rethrow ResourceCloseFailureException") {
    val exception = new org.neo4j.kernel.api.exceptions.ResourceCloseFailureException("foo", null)
    the[CypherExecutionException] thrownBy translateException(
      tokenNameLookup,
      throw exception
    ) should have message exception.getMessage
  }

  test("should rethrow ArithmeticException") {
    val exception = new java.lang.ArithmeticException("foo")
    the[org.neo4j.exceptions.ArithmeticException] thrownBy translateException(
      tokenNameLookup,
      throw exception
    ) should have message exception.getMessage
  }
}
