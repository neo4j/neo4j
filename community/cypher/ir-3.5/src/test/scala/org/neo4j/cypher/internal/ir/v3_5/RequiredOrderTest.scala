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
package org.neo4j.cypher.internal.ir.v3_5

import org.opencypher.v9_0.expressions.{Property, PropertyKeyName, Variable}
import org.opencypher.v9_0.util.DummyPosition
import org.opencypher.v9_0.util.test_helpers.CypherFunSuite

class RequiredOrderTest extends CypherFunSuite {
  protected val pos = DummyPosition(0)

  test("should rename property to variable") {
    val ro = RequiredOrder(Seq(("x.foo", AscColumnOrder)))
    // projection
    val projections = Map("xfoo" -> Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos))

    //when
    val result = ro.withRenamedColumns(projections)

    // then
    result should be(RequiredOrder(Seq(("xfoo", AscColumnOrder))))
  }

  test("should rename property to property") {
    val ro = RequiredOrder(Seq(("x.foo", AscColumnOrder)))
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    //when
    val result = ro.withRenamedColumns(projections)

    // then
    result should be(RequiredOrder(Seq(("y.foo", AscColumnOrder))))
  }

  test("should rename variable to variable") {
    val ro = RequiredOrder(Seq(("x", AscColumnOrder)))
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    //when
    val result = ro.withRenamedColumns(projections)

    // then
    result should be(RequiredOrder(Seq(("y", AscColumnOrder))))
  }
}
