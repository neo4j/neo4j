/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util
import java.util.stream.{DoubleStream, IntStream, LongStream}

import org.mockito.Mockito.when
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.makeValueNeoSafe
import org.neo4j.cypher.internal.frontend.v3_4.CypherTypeException
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}

import scala.collection.JavaConverters._

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
    val col = CompiledConversionUtils.toCollection(List("a", "b", "c").asJava)

    col shouldBe a[java.util.Collection[_]]
    col.asScala.toSeq should equal(Seq("a", "b", "c"))
  }

  test("should throw if converting from non-collection") {
    intercept[CypherTypeException](CompiledConversionUtils.toCollection("this is not a collection"))
  }

  test("should handle null") {
    CompiledConversionUtils.toCollection(null) shouldBe empty
  }

  test("should be able to turn an array into a collection") {
    CompiledConversionUtils.toCollection(Array("a", 42L)).asScala.toList should equal(List("a", 42))
  }

  test("should be able to turn a primitive array into a collection") {
    CompiledConversionUtils.toCollection(Array(1337L, 42L)).asScala.toList should equal(List(1337L, 42))
  }

  test("should be able to use a composite key in a hash map") {
    //given
    val theKey = CompiledConversionUtils.compositeKey(1l, 2L, 11L)
    val theObject = mock[Object]
    val theMap = Map(theKey -> theObject)

    //when/then
    theMap(theKey) should equal(theObject)
  }

  test("should handle toSet") {
    import scala.collection.JavaConverters._
    CompiledConversionUtils.toSet(null) should equal(Set.empty.asJava)
    CompiledConversionUtils.toSet(List(1, 1, 2, 3).asJava) should equal(Set(1, 2, 3).asJava)
    CompiledConversionUtils.toSet(IntStream.of(1, 2, 3, 1)) should equal(Set(1, 2, 3).asJava)
    CompiledConversionUtils.toSet(LongStream.of(1L, 2L, 3L, 1L)) should equal(Set(1L, 2L, 3L).asJava)
    CompiledConversionUtils.toSet(DoubleStream.of(1.1, 2.2, 3.3, 1.1)) should equal(Set(1.1, 2.2, 3.3).asJava)
    CompiledConversionUtils.toSet(Array(1, 1, 3, 2)) should equal(Set(1, 2, 3).asJava)
  }

  val testMakeSafe = Seq(
    Array(1, 2, 3) -> classOf[Array[Int]],
    Array[AnyRef](Byte.box(1), Byte.box(2), Byte.box(3)) -> classOf[Array[java.lang.Byte]],
    Array[AnyRef](Byte.box(1), Byte.box(2), Short.box(3)) -> classOf[Array[java.lang.Short]],
    Array[AnyRef](Byte.box(1), Long.box(2), Short.box(3)) -> classOf[Array[java.lang.Long]],
    Array[AnyRef](Double.box(1), Long.box(2), Float.box(3)) -> classOf[Array[java.lang.Double]],
    Array[AnyRef](Byte.box(1), Long.box(2), Float.box(3)) -> classOf[Array[java.lang.Float]],
    Array[AnyRef]("foo", "bar", "baz") -> classOf[Array[java.lang.String]],
    Array[AnyRef](Boolean.box(true), Boolean.box(false)) -> classOf[Array[java.lang.Boolean]],

    List(Byte.box(1), Byte.box(2), Byte.box(3)).asJava -> classOf[Array[java.lang.Byte]],
    List(Byte.box(1), Byte.box(2), Short.box(3)).asJava -> classOf[Array[java.lang.Short]],
    List(Byte.box(1), Long.box(2), Short.box(3)).asJava -> classOf[Array[java.lang.Long]],
    List(Double.box(1), Long.box(2), Float.box(3)).asJava -> classOf[Array[java.lang.Double]],
    List(Byte.box(1), Long.box(2), Float.box(3)).asJava -> classOf[Array[java.lang.Float]],
    List("foo", "bar", "baz").asJava -> classOf[Array[java.lang.String]],
    List(Boolean.box(true), Boolean.box(false)).asJava -> classOf[Array[java.lang.Boolean]]
  )

  testMakeSafe.foreach {
    case (v, t) =>
      test(s"$v should have type $t") {
        makeValueNeoSafe(v).getClass should equal(t)
      }
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
    (42, 43) -> false,
    (Array(42, 43), Array(42, 43)) -> true,
    (Array(42, 43), Array(42, 41)) -> false,
    (Array(42, 43), Array(42, 43, 44)) -> false,

    (Array(42, 43), util.Arrays.asList(42, 43)) -> true,
    (Array(42, 43), util.Arrays.asList(42, 41)) -> false,
    (Array(42, 43), util.Arrays.asList(42, 43, 44)) -> false,

    (util.Arrays.asList(42, 43), Array(42, 43)) -> true,
    (util.Arrays.asList(42, 43), Array(42, 41)) -> false,
    (util.Arrays.asList(42, 43), Array(42, 43, 44)) -> false,

    (util.Arrays.asList(42, 43), util.Arrays.asList(42, 43)) -> true,
    (util.Arrays.asList(42, 43), util.Arrays.asList(42, 41)) -> false,
    (util.Arrays.asList(42, 43), util.Arrays.asList(42, 43, 44)) -> false
  )

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

  val testNot = Seq(
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

  private val node = mock[Node]
  when(node.getId).thenReturn(11L)
  private val rel = mock[Relationship]
  when(rel.getId).thenReturn(13L)
}
