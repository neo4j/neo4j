/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime.executionplan

import java.util

import org.neo4j.cypher.result.RuntimeResult
import org.neo4j.function.ThrowingBiConsumer
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Result.{ResultRow, ResultVisitor}
import org.neo4j.helpers.collection.Iterators
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._
import org.neo4j.values.virtual.{ListValue, MapValue}
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

import scala.collection.JavaConverters

// We're going to get back here.
class StandardInternalExecutionResultTestTODO extends CypherFunSuite {

  def runtimeResult(row: util.Map[String, Any] = new util.HashMap()): RuntimeResult = ???

  def executionResult(row: util.Map[String, Any] = new util.HashMap()): StandardInternalExecutionResult = ???

  test("should return scala objects") {
    val result = executionResult(javaMap("foo" -> "", "bar" -> ""))

    result.fieldNames() should equal(List("foo", "bar"))
  }

  test("should return java objects for string") {
    val result = executionResult(javaMap("foo" -> "bar"))

    Iterators.asList(result.javaColumnAs[String]("foo")) should equal(javaList("bar"))
  }

  test("should return java objects for list") {
    val result = executionResult(javaMap("foo" -> javaList(42L)))

    Iterators.asList(result.javaColumnAs[List[Integer]]("foo")) should equal(javaList(javaList(42L)))
  }

  test("should return java objects for map") {
    val result = executionResult(javaMap("foo" -> javaMap("key" -> "value")))

    Iterators.asList(result.javaColumnAs[Map[String, Any]]("foo")) should equal(javaList(javaMap("key" -> "value")))
  }

  test("should throw if non-existing column") {
    val result = executionResult(javaMap("foo" -> "bar"))

    a [NotFoundException] shouldBe thrownBy(result.javaColumnAs[String]("baz"))
  }

  test("should return a java iterator for string") {
    val result = executionResult(javaMap("foo" -> "bar"))

    Iterators.asList(result.javaIterator) should equal(javaList(javaMap("foo" -> "bar")))
  }

  test("should return a java iterator for list") {
    val result = executionResult(javaMap("foo" -> javaList(42L)))

    Iterators.asList(result.javaIterator) should equal(javaList(javaMap("foo" -> javaList(42L))))
  }

  test("should return a java iterator for map") {
    val result = executionResult(javaMap("foo" -> javaMap("key" -> "value")))

    Iterators.asList(result.javaIterator) should equal(javaList(javaMap("foo" -> javaMap("key" -> "value"))))
  }

  test("javaIterator hasNext should not call accept if results already consumed") {
    // given
    val result = executionResult()

    // when
    result.accept(new ResultVisitor[Exception] {
      override def visit(row: ResultRow): Boolean = {
        false
      }
    })

    // then
    result.javaIterator.hasNext should be(false)
  }

  test("close should work after result is consumed") {
    // given
    val result = executionResult(javaMap("a" -> "1", "b" -> "2"))

    // when
    result.accept(new ResultVisitor[Exception] {
      override def visit(row: ResultRow): Boolean = {
        true
      }
    })

    result.close()

    // then
    // call of close actually worked
  }

  import JavaConverters._
  private def toObjectConverter(a: AnyRef): AnyRef = a match {
    case Values.NO_VALUE => null
    case s: TextValue => s.stringValue()
    case b: BooleanValue => Boolean.box(b.booleanValue())
    case f: FloatingPointValue => Double.box(f.doubleValue())
    case i: IntegralValue => Long.box(i.longValue())
    case l: ListValue =>
      val list = new util.ArrayList[AnyRef]
      l.iterator().asScala.foreach(a => list.add(toObjectConverter(a)))
      list
    case m: MapValue =>
      val map = new util.HashMap[String, AnyRef]()
      m.foreach(new ThrowingBiConsumer[String, AnyValue, RuntimeException] {
        override def accept(t: String, u: AnyValue): Unit = map.put(t, toObjectConverter(u))
      })
      map
  }

  private def javaList[T](elements: T*): util.List[T] = elements.toList.asJava

  private def javaMap[K, V](pairs: (K, V)*): util.Map[K, V] = pairs.toMap.asJava

}
