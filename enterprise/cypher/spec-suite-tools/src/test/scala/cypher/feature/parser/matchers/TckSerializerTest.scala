/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser.matchers

import cypher.feature.parser.ParsingTestSupport
import org.neo4j.graphdb.Node

class TckSerializerTest extends ParsingTestSupport {

  test("should serialize primitives") {
    serialize(true) should equal("true")
    serialize(1) should equal("1")
    serialize("1") should equal("'1'")
    serialize(1.5) should equal("1.5")
    serialize(null) should equal("null")
  }

  test("should serialize lists") {
    serialize(List.empty.asJava) should equal("[]")
    serialize(List(1).asJava) should equal("[1]")
    serialize(List("1", 1, 1.0).asJava) should equal("['1', 1, 1.0]")
    serialize(List(List("foo").asJava).asJava) should equal("[['foo']]")
  }

  test("should serialize arrays") {
    serialize(Array.empty) should equal("[]")
    serialize(Array(1)) should equal("[1]")
    serialize(Array[Any]("1", 1, 1.0)) should equal("['1', 1, 1.0]")
    serialize(Array(Array("foo"))) should equal("[['foo']]")
  }

  test("should serialize maps") {
    serialize(Map.empty.asJava) should equal("{}")
    serialize(Map("key" -> true, "key2" -> 1000).asJava) should equal("{key: true, key2: 1000}")
    serialize(Map("key" -> Map("inner" -> 50.0).asJava, "key2" -> List("foo").asJava).asJava) should equal("{key: {inner: 50.0}, key2: ['foo']}")
  }

  test("should serialize node") {
    serialize(node()) should equal("( {})")
    serialize(node(Seq("L1", "L2"))) should equal("(:L1:L2 {})")
    serialize(node(Seq("L1", "L2"), Map("prop1" -> "1", "prop2" -> List(true, false).asJava))) should
      equal("(:L1:L2 {prop1: '1', prop2: [true, false]})")
  }

  test("should serialize relationship") {
    serialize(relationship("T")) should equal("[:T {}]")
    serialize(relationship("T", Map("prop" -> "foo"))) should equal("[:T {prop: 'foo'}]")
  }

  test("should serialize empty path") {
    serialize(singleNodePath(node(Seq("Label"), Map("prop" -> Boolean.box(true))))) should equal("<(:Label {prop: true})>")
  }

  test("should serialize paths") {
    serialize(path(pathLink(node(Seq("Start")), relationship("T"), node(Seq("End"))))) should equal("<(:Start {})-[:T {}]->(:End {})>")
  }

  test("should serialize longer path") {
    val middle: Node = node(Seq("Middle"))
    serialize(path(pathLink(node(Seq("Start")), relationship("T1"), middle),
                   pathLink(node(Seq("End")), relationship("T2"), middle))) should
      equal("<(:Start {})-[:T1 {}]->(:Middle {})<-[:T2 {}]-(:End {})>")
  }

  private def serialize(v: Any) = TckSerializer.serialize(v)
}
