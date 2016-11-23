/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.commands.predicates

import java.util
import java.util.Arrays.asList
import java.util.Collections.singletonMap

import org.neo4j.cypher.internal.compiler.v3_2.{CRS, GeographicPoint}
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.graphdb.spatial.{CRS => JavaCRS, Coordinate, Point}

class EquivalentTest extends CypherFunSuite {
  shouldNotMatch(23.toByte, 23.5)

  shouldMatch(1.0, 1L)
  shouldMatch(1.0, 1)
  shouldMatch(1.0, 1.0)
  shouldMatch(0.9, 0.9)
  shouldMatch(Math.PI, Math.PI)
  shouldMatch(1.1, 1.1)
  shouldMatch(0, 0)
//  shouldMatch(Double.NaN, Double.NaN)
  shouldMatch(Integer.MAX_VALUE.toDouble, Integer.MAX_VALUE)
  shouldMatch(Long.MaxValue.toDouble, Long.MaxValue)
  shouldMatch(Int.MaxValue.toDouble + 1, Int.MaxValue.toLong + 1)
  shouldMatch(Double.PositiveInfinity, Double.PositiveInfinity)
  shouldMatch(Double.NegativeInfinity, Double.NegativeInfinity)
  shouldMatch(true, true)
  shouldMatch(false, false)
  shouldNotMatch(true, false)
  shouldNotMatch(false, true)
  shouldNotMatch(true, 0)
  shouldNotMatch(false, 0)
  shouldNotMatch(true, 1)
  shouldNotMatch(false, 1)
  shouldNotMatch(false, "false")
  shouldNotMatch(true, "true")
  shouldMatch(42.toByte, 42.toByte)
  shouldMatch(42.toByte, 42.toShort)
  shouldNotMatch(42.toByte, 42 + 256)
  shouldMatch(43.toByte, 43.toInt)
  shouldMatch(43.toByte, 43.toLong)
  shouldMatch(23.toByte, 23.0d)
  shouldMatch(23.toByte, 23.0f)
  shouldNotMatch(23.toByte, 23.5f)
  shouldMatch(11.toShort, 11.toByte)
  shouldMatch(42.toShort, 42.toShort)
  shouldNotMatch(42.toShort, 42 + 65536)
  shouldMatch(43.toShort, 43.toInt)
  shouldMatch(43.toShort, 43.toLong)
  shouldMatch(23.toShort, 23.0f)
  shouldMatch(23.toShort, 23.0d)
  shouldNotMatch(23.toShort, 23.5)
  shouldNotMatch(23.toShort, 23.5f)
  shouldMatch(11, 11.toByte)
  shouldMatch(42, 42.toShort)
  shouldNotMatch(42, 42 + 4294967296L)
  shouldMatch(43, 43)
  shouldMatch(Integer.MAX_VALUE, Integer.MAX_VALUE)
  shouldMatch(43, 43.toLong)
  shouldMatch(23, 23.0)
  shouldNotMatch(23, 23.5)
  shouldNotMatch(23, 23.5f)
  shouldMatch(11L, 11.toByte)
  shouldMatch(42L, 42.toShort)
  shouldMatch(43L, 43.toInt)
  shouldMatch(43L, 43.toLong)
  shouldMatch(87L, 87.toLong)
  shouldMatch(Long.MaxValue, Long.MaxValue)
  shouldMatch(Int.MaxValue, Int.MaxValue.toLong)
  shouldMatch(23L, 23.0)
  shouldNotMatch(23L, 23.5)
  shouldNotMatch(23L, 23.5f)
  shouldMatch(9007199254740992L, 9007199254740992D)
  shouldNotMatch(4611686018427387905L, 4611686018427387900L)
  shouldMatch(11f, 11.toByte)
  shouldMatch(42f, 42.toShort)
  shouldMatch(43f, 43.toInt)
  shouldMatch(43f, 43.toLong)
  shouldMatch(23f, 23.0)
  shouldNotMatch(23f, 23.5)
  shouldNotMatch(23f, 23.5f)
  shouldMatch(3.14f, 3.14f)
  shouldMatch(3.14f, 3.14d)
  shouldMatch(11d, 11.toByte)
  shouldMatch(42d, 42.toShort)
  shouldMatch(43d, 43.toInt)
  shouldMatch(43d, 43.toLong)
  shouldMatch(23d, 23.0)
  shouldNotMatch(23d, 23.5)
  shouldNotMatch(23d, 23.5f)
  shouldMatch(3.14d, 3.14f)
  shouldMatch(3.14d, 3.14d)
  shouldMatch("A", "A")
  shouldMatch('A', 'A')
  shouldMatch('A', "A")
  shouldMatch("A", 'A')
  shouldNotMatch("AA", 'A')
  shouldNotMatch("a", "A")
  shouldNotMatch("A", "a")
  shouldNotMatch("0", 0)
  shouldNotMatch('0', 0)

  // Lists and arrays
  shouldMatch(Array[Int](1, 2, 3), Array[Int](1, 2, 3))
  shouldMatch(Array[Array[Int]](Array(1), Array(2, 2), Array(3, 3, 3)), Array[Array[Double]](Array(1.0), Array(2.0, 2.0), Array(3.0, 3.0, 3.0)))
  shouldMatch(Array[Int](1, 2, 3), Array[Long](1, 2, 3))
  shouldMatch(Array[Int](1, 2, 3), Array[Double](1.0, 2.0, 3.0))

  shouldMatch(Array[String]("A", "B", "C"), Array[String]("A", "B", "C"))
  shouldMatch(Array[String]("A", "B", "C"), Array[Char]('A', 'B', 'C'))
  shouldMatch(Array[Char]('A', 'B', 'C'), Array[String]("A", "B", "C"))
  shouldMatch(Array[Int](1, 2, 3), asList(1, 2, 3))

  shouldMatch(asList(1, 2, 3), asList(1L, 2L, 3L))
  shouldMatch(asList(1, 2, 3, null), asList(1L, 2L, 3L, null))
  shouldMatch(Array[Int](1, 2, 3), asList(1L, 2L, 3L))
  shouldMatch(Array[Int](1, 2, 3), asList(1.0D, 2.0D, 3.0D))
  shouldMatch(Array[Any](1, Array[Int](2, 2), 3), asList(1.0D, asList(2.0D, 2.0D), 3.0D))
  shouldMatch(Array[String]("A", "B", "C"), asList("A", "B", "C"))
  shouldMatch(Array[String]("A", "B", "C"), asList('A', 'B', 'C'))
  shouldMatch(Array[Char]('A', 'B', 'C'), asList("A", "B", "C"))
  shouldMatch(new util.ArrayList[AnyRef](), List.empty[AnyRef])
  shouldMatch(Array[Int](), List.empty[AnyRef])

  // Maps
  shouldMatch(Map("a" -> 42), Map("a" -> 42))
  shouldMatch(Map("a" -> 42), Map("a" -> 42.0))
  shouldMatch(Map("a" -> 42), singletonMap("a", 42.0))
  shouldMatch(singletonMap("a", asList(41.0, 42.0)), Map("a" -> List(41,42)))
  shouldMatch(Map("a" -> singletonMap("x", asList(41.0, 'c'.asInstanceOf[Character]))), singletonMap("a", Map("x" -> List(41, "c"))))

  // Geographic Values
  shouldMatch(GeographicPoint(32, 43, CRS.Cartesian), GeographicPoint(32, 43, CRS.Cartesian))
  // There are no ready made implementations of geographic points in the core API, so we need to
  // be able to accept any implementation of the interface here
  val crs = ImplementsJavaCRS("cartesian", "http://spatialreference.org/ref/sr-org/7203/", 7203)
  shouldMatch(ImplementsJavaPoint(32, 43, crs), GeographicPoint(32, 43, CRS.Cartesian))
  shouldMatch(ImplementsJavaPoint(32, 43, crs), ImplementsJavaPoint(32.0, 43.0, crs))

  private def shouldMatch(v1: Any, v2: Any) {
    test(testName(v1, v2, "=")) {
      val eq1 = Equivalent(v1)
      val eq2 = Equivalent(v2)
      eq1.equals(v2) should equal(true)
      eq2.equals(v1) should equal(true)
      eq1.hashCode() should equal(eq2.hashCode())
    }
  }

  private def shouldNotMatch(v1: Any, v2: Any) {
    test(testName(v1, v2, "<>")) {
      val eq1 = Equivalent(v1)
      val eq2 = Equivalent(v2)
      eq1.equals(v2) should equal(false)
      eq2.equals(v1) should equal(false)
    }
  }

  private def testName(v1: Any, v2: Any, operator: String): String = {
    s"$v1 (${v1.getClass.getSimpleName}) $operator $v2 (${v2.getClass.getSimpleName})\n"
  }
}

case class ImplementsJavaPoint(longitude: Double, latitude: Double, crs: JavaCRS) extends Point {
  override def getCRS: JavaCRS = crs

  override def getCoordinates: util.List[Coordinate] = asList(new Coordinate(longitude, latitude))

  override def getGeometryType: String = crs.getType
}

case class ImplementsJavaCRS(typ: String, href: String, code: Int) extends JavaCRS {
  override def getType: String = typ

  override def getHref: String = href

  override def getCode: Int = code
}
