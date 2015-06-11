/*
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen

import org.neo4j.cypher.internal.compiler.v2_3.{CypherTypeException, IncomparableValuesException}
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite

class CompiledConversionUtilsTest extends CypherFunSuite {

  val tests = Seq(
    (true, true),
    (false, false),
    (null, false),
    (Array(), false),
    (Array(1), true),
    (Array("foo"), true),
    (Array[Int](), false)
  )

  tests.foreach {
    case (v, expected) =>
      test(s"$v") {
        CompiledConversionUtils.coerceToPredicate(v) should equal(expected)
      }
  }

  test("should throw for string and int") {
    intercept[CypherTypeException](CompiledConversionUtils.coerceToPredicate("APA"))
    intercept[CypherTypeException](CompiledConversionUtils.coerceToPredicate(12))
  }

  test("should convert List") {
    import scala.collection.JavaConverters._

    val col = CompiledConversionUtils.toCollection(List("a", "b", "c").asJava)

    col shouldBe a [java.util.Collection[_]]
    col.asScala.toSeq should equal(Seq("a", "b", "c"))
  }

  test("should throw if converting from non-collection") {
    intercept[CypherTypeException](CompiledConversionUtils.toCollection("this is not a collection"))
  }

  test("should handle null") {
    CompiledConversionUtils.toCollection(null) shouldBe empty
  }

}
