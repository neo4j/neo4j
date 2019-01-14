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
package org.neo4j.cypher.internal.runtime.interpreted

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.DurationValue.duration
import org.neo4j.values.storable.LocalTimeValue.localTime
import org.neo4j.values.storable._
import org.neo4j.values.virtual.VirtualValues

class CastSupportTest extends CypherFunSuite {

  test("siftTest") {
    val given = Seq[Any](1, 2, "a", 3, "b", 42, "z")
    val then  = CastSupport.sift[String](given)
    then should equal(Seq("a", "b", "z"))
  }

  test("siftComplexTest") {
    val given = Seq[Any](1, 2, List("a"), 3, "b", 42, List("z"))
    val then  = CastSupport.sift[List[String]](given)
    then should equal(Seq(List("a"), List("z")))
  }

  test("downcastPfMatchTest") {
    val given: Any                          = Seq(1)
    val fun: PartialFunction[Any, Seq[Int]] = CastSupport.erasureCast[Seq[Int]]
    val then                                = fun(given)
    then should equal(Seq(1))
  }

  test("downcastPfMismatchTest") {
    val given: Any                           = "Hallo"
    val fun: PartialFunction[Any, Seq[Long]] = CastSupport.erasureCast[Seq[Long]]
    fun.isDefinedAt(given) should equal(false)
  }

  test("downcastAppMatchTest") {
    val given: Any = 1
    CastSupport.castOrFail[java.lang.Integer](given) should equal(1)
  }

  test("downcastAppMismatchTest") {
    val given: Any = Seq(1)
    intercept[CypherTypeException](CastSupport.castOrFail[Int](given))
  }

  test("should convert string lists to arrays") {
    val valueObj = Values.stringValue("test")
    val list = VirtualValues.list(valueObj)
    val array = CastSupport.getConverter(valueObj).arrayConverter(list)
    array shouldBe a[StringArray]
    array.asInstanceOf[StringArray].asObjectCopy()(0) should equal(valueObj.asObjectCopy())
  }

  test("should convert point lists to arrays") {
    val valueObj = Values.pointValue(CoordinateReferenceSystem.Cartesian, 0.0, 1.0)
    val list = VirtualValues.list(valueObj)
    val array = CastSupport.getConverter(valueObj).arrayConverter(list)
    array shouldBe a[PointArray]
    array.asInstanceOf[PointArray].asObjectCopy()(0) should equal(valueObj)
  }

  test("should convert local time lists to arrays") {
    val valueObj = localTime(3, 20, 45, 0)
    val list = VirtualValues.list(valueObj)
    val array = CastSupport.getConverter(valueObj).arrayConverter(list)
    array shouldBe a[LocalTimeArray]
    array.asInstanceOf[LocalTimeArray].asObjectCopy()(0) should equal(valueObj.asObjectCopy())
  }

  test("should convert duration lists to arrays") {
    val valueObj = duration(3, 20, 45, 0)
    val list = VirtualValues.list(valueObj)
    val array = CastSupport.getConverter(valueObj).arrayConverter(list)
    array shouldBe a[DurationArray]
    array.asInstanceOf[DurationArray].asObjectCopy()(0) should equal(valueObj.asObjectCopy())
  }
}
