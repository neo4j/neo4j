/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.codegen.CompiledConversionUtils.makeValueNeoSafe
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, Relationship}
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable._

import scala.collection.JavaConverters._

class CompiledConversionUtilsTest extends CypherFunSuite {

  val testPredicates = Seq(
    (true, true),
    (false, false),
    (null, false),
    (Array(), false),
    (Array(1), true),
    (Array("foo"), true),
    (Array[Int](), false),

    (Values.booleanValue(true), true),
    (Values.booleanValue(false), false),
    (Values.NO_VALUE, false),
    (Values.stringArray(), false),
    (Values.longArray(Array(1)), true),
    (Values.stringArray("foo"), true),
    (Values.intArray(Array[Int]()), false)
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
    // Values
    CompiledConversionUtils.toSet(Values.NO_VALUE) should equal(Set.empty.asJava)
    CompiledConversionUtils.toSet(Values.of(Array(1, 1, 3, 2))) should equal(Set(Values.intValue(1), Values.intValue(2), Values.intValue(3)).asJava)
    CompiledConversionUtils.toSet(ValueUtils.of(List(1, 1, 2, 3).asJava)) should equal(Set(Values.intValue(1), Values.intValue(2), Values.intValue(3)).asJava)
  }

  val testMakeSafe = Seq(
    Array(1, 2, 3) -> classOf[IntArray],
    Array[AnyRef](Byte.box(1), Byte.box(2), Byte.box(3)) -> classOf[ByteArray],
    Array[AnyRef](Byte.box(1), Byte.box(2), Short.box(3)) -> classOf[ShortArray],
    Array[AnyRef](Byte.box(1), Long.box(2), Short.box(3)) -> classOf[LongArray],
    Array[AnyRef](Double.box(1), Long.box(2), Float.box(3)) -> classOf[DoubleArray],
    Array[AnyRef](Byte.box(1), Long.box(2), Float.box(3)) -> classOf[FloatArray],
    Array[AnyRef]("foo", "bar", "baz") -> classOf[StringArray],
    Array[AnyRef](Boolean.box(true), Boolean.box(false)) -> classOf[BooleanArray],

    List(Byte.box(1), Byte.box(2), Byte.box(3)).asJava -> classOf[ByteArray],
    List(Byte.box(1), Byte.box(2), Short.box(3)).asJava -> classOf[ShortArray],
    List(Byte.box(1), Long.box(2), Short.box(3)).asJava -> classOf[LongArray],
    List(Double.box(1), Long.box(2), Float.box(3)).asJava -> classOf[DoubleArray],
    List(Byte.box(1), Long.box(2), Float.box(3)).asJava -> classOf[FloatArray],
    List("foo", "bar", "baz").asJava -> classOf[StringArray],
    List(Boolean.box(true), Boolean.box(false)).asJava -> classOf[BooleanArray]
  )

  testMakeSafe.foreach {
    case (v, t) =>
      test(s"$v should have type $t") {
        makeValueNeoSafe(v).getClass should equal(t)
      }
  }

  private var i = 0
  testMakeSafe.foreach {
    case (v, t) =>
      val av = ValueUtils.of(v)
      test(s"AnyValue $av should have type $t ($i)") {
        makeValueNeoSafe(av).getClass should equal(t)
      }
      i += 1
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

  testEquality.foreach {
    case ((v1, v2), expected) =>
      val av1 = ValueUtils.of(v1)
      val av2 = ValueUtils.of(v2)
      test(s"${av1} == ${av2}") {
        CompiledConversionUtils.equals _ tupled (av1 -> av2) should equal(expected)
      }
  }

  testEquality.foreach {
    case ((v1, v2), expected) =>
      val av1 = ValueUtils.of(v1)
      test(s"${av1} == ${v2}") {
        CompiledConversionUtils.equals _ tupled (av1 -> v2) should equal(expected)
      }
  }

  testEquality.foreach {
    case ((v1, v2), expected) =>
      val av2 = ValueUtils.of(v2)
      test(s"${v1} == ${av2}") {
        CompiledConversionUtils.equals _ tupled (v1 -> av2) should equal(expected)
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
    (false, false) -> false,

    (Values.NO_VALUE, Values.booleanValue(true)) -> true,
    (Values.NO_VALUE, Values.booleanValue(false)) -> null,
    (Values.booleanValue(true), Values.NO_VALUE) -> true,
    (Values.booleanValue(false), Values.NO_VALUE) -> null,
    (Values.booleanValue(true), Values.booleanValue(true)) -> true,
    (Values.booleanValue(true), Values.booleanValue(false)) -> true,
    (Values.booleanValue(false), Values.booleanValue(true)) -> true,
    (Values.booleanValue(false), Values.booleanValue(false)) -> false,

    (null, Values.booleanValue(true)) -> true,
    (null, Values.booleanValue(false)) -> null,
    (true, Values.NO_VALUE) -> true,
    (false, Values.NO_VALUE) -> null,
    (true, Values.booleanValue(true)) -> true,
    (true, Values.booleanValue(false)) -> true,
    (false, Values.booleanValue(true)) -> true,
    (false, Values.booleanValue(false)) -> false,

    (Values.NO_VALUE, true) -> true,
    (Values.NO_VALUE, false) -> null,
    (Values.booleanValue(true), null) -> true,
    (Values.booleanValue(false), null) -> null,
    (Values.booleanValue(true), true) -> true,
    (Values.booleanValue(true), false) -> true,
    (Values.booleanValue(false), true) -> true,
    (Values.booleanValue(false), false) -> false
  )

  testOr.foreach {
    case (v, expected) =>
      test(s"${v._1} || ${v._2}") {
        CompiledConversionUtils.or _ tupled v should equal(expected)
      }
  }

  val testNot = Seq(
    (null, null),
    (false, true),
    (true, false),

    (Values.NO_VALUE, null),
    (Values.booleanValue(false), true),
    (Values.booleanValue(true), false)
  )

  testNot.foreach {
    case (v, expected) =>
      test(s"$v != $expected)") {
        CompiledConversionUtils.not(v) should equal(expected)
      }
  }
}
