package org.neo4j.cypher.internal.runtime

import org.mockito.Mockito
import org.mockito.Mockito.when
import org.neo4j.cypher.internal.runtime.ManyNodeValueIndexCursor.ascending
import org.neo4j.cypher.internal.runtime.ManyNodeValueIndexCursor.descending
import org.neo4j.cypher.internal.runtime.ManyNodeValueIndexCursor.unordered
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.internal.kernel.api.NodeValueIndexCursor
import org.neo4j.internal.kernel.api.helpers.StubNodeValueIndexCursor
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.scalatest.FunSuite

import scala.collection.mutable.ArrayBuffer

class ManyNodeValueIndexCursorTest extends CypherFunSuite {

  test("should create unordered cursor") {
    //given
    val cursor = unordered(
      Array(
        cursorFor(10, 11, 12),
        cursorFor(5),
        cursorFor(11, 15),
      ))

    //when
    val list = asList(cursor)

    //then
    list should equal(List(10, 11, 12, 5, 11, 15))
  }

  test("should create ascending cursor") {
    //given
    val cursor = ascending(
      Array(
        cursorFor(10, 11, 12),
        cursorFor(5),
        cursorFor(11, 15),
      ))

    //when
    val list = asList(cursor)

    //then
    list should equal(List(5, 10, 11, 11, 12, 15))
  }

  test("should create descending cursor") {
    //given
    val cursor = descending(
      Array(
        cursorFor(12, 11, 10),
        cursorFor(5),
        cursorFor(15, 11),
      ))

    //when
    val list = asList(cursor)

    //then
    list should equal(List(15, 12, 11, 11, 10, 5))
  }

  private def cursorFor(values: Any*): NodeValueIndexCursor = {
    val stub = new StubNodeValueIndexCursor()
      values.zipWithIndex.foreach {
        case (v, i) => stub.withNode(i, Values.of(v))
      }
     stub
  }

  private def asList(cursor: NodeValueIndexCursor): Seq[AnyRef] = {
    val values = ArrayBuffer.empty[AnyRef]
    while (cursor.next()) {
      values.append(cursor.propertyValue(0).asObject())
    }
    values
  }
}
