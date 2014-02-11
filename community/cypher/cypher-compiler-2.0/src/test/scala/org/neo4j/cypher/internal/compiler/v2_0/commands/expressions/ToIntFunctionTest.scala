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
package org.neo4j.cypher.internal.compiler.v2_0.commands.expressions

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FunSuite}
import org.neo4j.cypher.internal.compiler.v2_0.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_0.pipes.QueryStateHelper

@RunWith(classOf[JUnitRunner])
class ToIntFunctionTest extends FunSuite with Matchers {

  test("should return null if argument is null") {
    toInt(null.asInstanceOf[Any]) should be(null.asInstanceOf[Int])
  }

  test("should convert a string to an integer") {
    toInt("10") should be(10)
  }

  test("should convert a double to an integer") {
    toInt(23.5d) should be(23)
  }

  test("should throw an exception if the argument is a double literal") {
    evaluating { toInt("20.5") } should produce[IllegalArgumentException]
  }

  test("should throw an exception if the argument is a non-numeric string") {
    evaluating { toInt("20foobar2") } should produce[IllegalArgumentException]
  }

  test("should throw an exception if the argument is a hexadecimal string") {
    evaluating { toInt("0x20") } should produce[IllegalArgumentException]
  }

  test("should convert a string with leading zeros to an integer") {
    toInt("000123121") should be(123121)
  }

  test("should convert a string with leading minus in a negative integer") {
    toInt("-12") should be(-12)
  }

  test("should convert a string with leading minus and zeros in a negative integer") {
    toInt("-00012") should be(-12)
  }

  test("should throw an exception if the argument is an object which cannot be converted to integer") {
    evaluating { toInt(new Object) } should produce[IllegalArgumentException]
  }

  test("given an integer should give the same value back") {
    toInt(50) should be(50)
  }

  test("given a boolean should return the numeric value") {
    toInt(false) should be(0)
    toInt(true) should be(1)
  }

  test("should throw an exception if the argument is a boolean string literal") {
    evaluating { toInt("false") } should produce[IllegalArgumentException]
    evaluating { toInt("true") } should produce[IllegalArgumentException]
  }

  private def toInt(orig: Any) = {
    ToIntFunction(Literal(orig))(ExecutionContext.empty)(QueryStateHelper.empty)
  }
}
