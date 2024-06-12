/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.procs

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues

class ParameterTransformerTest extends CypherFunSuite {

  test("should add generate") {
    ParameterTransformer()
      .generate((_, _, _) => VirtualValues.map(Array("B"), Array(Values.stringValue("B"))))
      .transform(null, null, VirtualValues.EMPTY_MAP, VirtualValues.EMPTY_MAP) shouldBe (VirtualValues.map(
      Array("B"),
      Array(Values.stringValue("B"))
    ), Set.empty)
  }

  test("should combine generate") {
    ParameterTransformer((_, _, _) => VirtualValues.map(Array("A"), Array(Values.stringValue("A"))))
      .generate((_, _, _) => VirtualValues.map(Array("B"), Array(Values.stringValue("B"))))
      .transform(null, null, VirtualValues.EMPTY_MAP, VirtualValues.EMPTY_MAP) shouldBe (VirtualValues.map(
      Array("A", "B"),
      Array(Values.stringValue("A"), Values.stringValue("B"))
    ), Set.empty)
  }

  test("should combine generate and convert") {
    ParameterTransformer((_, _, _) => VirtualValues.map(Array("A"), Array(Values.stringValue("A"))))
      .convert((_, params) => params.updatedWith("C", Values.stringValue("C")))
      .generate((_, _, _) => VirtualValues.map(Array("B"), Array(Values.stringValue("B"))))
      .transform(null, null, VirtualValues.EMPTY_MAP, VirtualValues.EMPTY_MAP) shouldBe (VirtualValues.map(
      Array("A", "B", "C"),
      Array(Values.stringValue("A"), Values.stringValue("B"), Values.stringValue("C"))
    ), Set.empty)
  }

  test("should combine generate and update") {
    ParameterTransformer((_, _, _) => VirtualValues.map(Array("A"), Array(Values.stringValue("A"))))
      .convert((_, params) => params.updatedWith("A", Values.stringValue("C")))
      .generate((_, _, _) => VirtualValues.map(Array("B"), Array(Values.stringValue("B"))))
      .transform(null, null, VirtualValues.EMPTY_MAP, VirtualValues.EMPTY_MAP) shouldBe (VirtualValues.map(
      Array("A", "B"),
      Array(Values.stringValue("C"), Values.stringValue("B"))
    ), Set.empty)
  }

}
