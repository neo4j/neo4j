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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryStateHelper
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class ToIntFunctionTest extends CypherFunSuite {

  test("should return null if argument is null") {
    assert(toInt(null) === null)
  }

  test("should convert a string to an integer") {
    toInt("10") should equal(10)
  }

  test("should convert a double to an integer") {
    toInt(23.5d) should equal(23)
  }

  test("should parse float and truncate to int if the argument is a float literal") {
    toInt("20.5") should equal(20)
  }

  test("should return null if the argument is a partially numeric string") {
    assert(toInt("20foobar2") === null)
  }

  test("should return null if the argument is a hexadecimal string") {
    assert(toInt("0x20") === null)
  }

  test("should convert a string with leading zeros to an integer") {
    toInt("000123121") should equal(123121)
  }

  test("should convert a string with leading minus in a negative integer") {
    toInt("-12") should equal(-12)
  }

  test("should convert a string with leading minus and zeros in a negative integer") {
    toInt("-00012") should equal(-12)
  }

  test("should throw an exception if the argument is an object which cannot be converted to integer") {
    evaluating { toInt(new Object) } should produce[CypherTypeException]
  }

  test("given an integer should give the same value back") {
    toInt(50) should equal(50)
  }

  test("should truncate floats if given a float") {
    toInt(20.6f) should equal(20)
  }

  test("should fail for larger integers larger that 8 bytes") {
    evaluating { toInt("10508455564958384115") } should produce[CypherTypeException]
  }

  test("should handle floats larger than 2^31 - 1") {
    //2^33 = 8589934592
    toInt("8589934592.0") should equal(8589934592L)
  }

  test("should handle -2^63") {
    toInt("-9223372036854775808") should equal(Long.MinValue)
  }

  test("cannot handle -2^63-1") {
    evaluating { toInt("-9223372036854775809") } should produce[CypherTypeException]
  }

  test("should handle 2^63 - 1") {
    toInt("9223372036854775807") should equal(Long.MaxValue)
  }

  test("cannot handle 2^63") {
    evaluating { toInt("9223372036854775808") } should produce[CypherTypeException]
  }

  private def toInt(orig: Any) = {
    ToIntFunction(Literal(orig))(ExecutionContext.empty)(QueryStateHelper.empty)
  }
}
