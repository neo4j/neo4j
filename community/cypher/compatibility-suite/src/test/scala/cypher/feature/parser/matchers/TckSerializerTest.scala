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
package cypher.feature.parser.matchers

import scala.collection.JavaConverters._
import org.scalatest.{FunSuite, Matchers}

class TckSerializerTest extends FunSuite with Matchers {

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

  test("should serialize maps") {
    serialize(Map.empty.asJava) should equal("{}")
    serialize(Map("key" -> true, "key2" -> 1000).asJava) should equal("{key2=1000, key=true}")
    serialize(Map("key" -> Map("inner" -> 50.0).asJava, "key2" -> List("foo").asJava).asJava) should equal("{key2=['foo'], key={inner=50.0}}")
  }

  private def serialize(v: Any) = TckSerializer.serialize(v)
}
