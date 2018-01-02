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
package org.neo4j.cypher.internal.codegen

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

class CompiledConversionUtilsTest extends CypherFunSuite {

  val testPredicates = Seq(
    (true, true),
    (false, false),
    (null, false),
    (Array(), false),
    (Array(1), true),
    (Array("foo"), true),
    (Array[Int](), false)
  )

  testPredicates.foreach {
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

  test("should be able to use a composite key in a hash map") {
    //given
    val theKey = CompiledConversionUtils.compositeKey(1l, 2L, 11L)
    val theObject = mock[Object]
    val theMap = Map(theKey -> theObject)

    //when/then
    theMap(theKey) should equal(theObject)
  }

  val testEquality = Seq(
    (null, "foo") -> null,
    (false, false) -> true,
    (9007199254740993L, 9007199254740992D) -> false,
    (9007199254740992D, 9007199254740993L) -> false,
    (1, null) -> null,
    ("foo", "foo") -> true,
    ("foo", "bar") -> false,
    (42L, 42) -> true,
    (42, 43) -> false)

  testEquality.foreach {
    case (v, expected) =>
      test(s"${v._1} == ${v._2}") {
        CompiledConversionUtils.equals _ tupled v should equal(expected)
      }
  }

  val testOr = Seq(
    (null, true) -> true,
    (null, false) -> null,
    (true, null) -> true,
    (false, null) -> null,
    (true, true) -> true,
    (true, false) -> true,
    (false, true) -> true,
    (false, false) -> false)

  testOr.foreach {
    case (v, expected) =>
      test(s"${v._1} || ${v._2}") {
        CompiledConversionUtils.or _ tupled v should equal(expected)
      }
  }

  val testNot= Seq(
    (null, null),
    (false, true),
    (true, false)
  )

  testNot.foreach {
    case (v, expected) =>
      test(s"$v != $expected)") {
        CompiledConversionUtils.not(v) should equal(expected)
      }
  }

  val node = mock[Node]
  when(node.getId).thenReturn(11L)
  val rel = mock[Relationship]
  when(rel.getId).thenReturn(13L)

  val testLoadParameter = Seq(
    (null, null),
    (node, new NodeIdWrapper(11L)),
    (rel, new RelationshipIdWrapper(13L))
  )

  testLoadParameter.foreach {
    case (v, expected) =>
      test(s"loadParameter($v) == $expected)") {
        CompiledConversionUtils.loadParameter(v) should equal(expected)
      }
  }
}
