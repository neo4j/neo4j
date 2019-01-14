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
package org.neo4j.cypher.internal.ir.v4_0

import org.neo4j.cypher.internal.v4_0.expressions.{Property, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.v4_0.util.DummyPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class InterestingOrderTest extends CypherFunSuite {
  protected val pos = DummyPosition(0)

  test("should project property to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("x.foo"))
    // projection
    val projections = Map("xfoo" -> Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos))

    //when
    val result = io.withProjectedColumns(projections)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("xfoo")))
  }

  test("should project property to variable (descending)") {
    val io = InterestingOrder.required(RequiredOrderCandidate.desc("x.foo"))
    // projection
    val projections = Map("xfoo" -> Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos))

    //when
    val result = io.withProjectedColumns(projections)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.desc("xfoo")))
  }

  test("should project property to property") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("x.foo"))
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    //when
    val result = io.withProjectedColumns(projections)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("y.foo")))
  }

  test("should project variable to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("x"))
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    //when
    val result = io.withProjectedColumns(projections)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("y")))
  }

  test("should reverse project property to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("xfoo"))
    // projection
    val projections = Map("xfoo" -> Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos))

    //when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("x.foo")))
  }

  test("should reverse project property to variable (descending)") {
    val io = InterestingOrder.required(RequiredOrderCandidate.desc("xfoo"))
    // projection
    val projections = Map("xfoo" -> Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos))

    //when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.desc("x.foo")))
  }

  test("should reverse project property to property") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("y.foo"))
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    //when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("x.foo")))
  }

  test("should reverse project variable to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("y"))
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    //when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("x")))
  }

  test("should not reverse project variable to variable if not argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc("y")).withReverseProjectedColumns(Map.empty, Set.empty)
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc("y")).withReverseProjectedColumns(Map.empty, Set("y"))
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("y")))
  }

  // test the interesting part of the InterestingOrder

  test("should project property to variable (for interesting)") {
    // projection
    val projections = Map("xfoo" -> Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos))

    val ioAsc = InterestingOrder.interested(InterestingOrderCandidate.asc("x.foo"))
    val ioDesc = InterestingOrder.interested(InterestingOrderCandidate.desc("x.foo"))

    //when
    val resultAsc = ioAsc.withProjectedColumns(projections)
    val resultDesc = ioDesc.withProjectedColumns(projections)

    // then
    resultAsc should be(InterestingOrder.interested(InterestingOrderCandidate.asc("xfoo")))
    resultDesc should be(InterestingOrder.interested(InterestingOrderCandidate.desc("xfoo")))
  }

  test("should project property to property and variable to variable (for interesting)") {
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    val ioProp = InterestingOrder.interested(InterestingOrderCandidate.asc("x.foo"))
    val ioVar = InterestingOrder.interested(InterestingOrderCandidate.asc("x"))

    //when
    val resultProp = ioProp.withProjectedColumns(projections)
    val resultVar = ioVar.withProjectedColumns(projections)

    // then
    resultProp should be(InterestingOrder.interested(InterestingOrderCandidate.asc("y.foo")))
    resultVar should be(InterestingOrder.interested(InterestingOrderCandidate.asc("y")))
  }

  test("should filter out empty interesting orders when projecting") {
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    val io = InterestingOrder.interested(InterestingOrderCandidate.empty)
      .interested(InterestingOrderCandidate.asc("x.foo")).interested(InterestingOrderCandidate.empty)

    //when
    val result = io.withProjectedColumns(projections)

    //then
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc("y.foo")))
  }

  test("should reverse project property to variable (for interesting)") {
    // projection
    val projections = Map("xfoo" -> Property(Variable("x")(pos), PropertyKeyName("foo")(pos))(pos))

    val ioAsc = InterestingOrder.interested(InterestingOrderCandidate.asc("xfoo"))
    val ioDesc = InterestingOrder.interested(InterestingOrderCandidate.desc("xfoo"))

    //when
    val resultAsc = ioAsc.withReverseProjectedColumns(projections, Set.empty)
    val resultDesc = ioDesc.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultAsc should be(InterestingOrder.interested(InterestingOrderCandidate.asc("x.foo")))
    resultDesc should be(InterestingOrder.interested(InterestingOrderCandidate.desc("x.foo")))
  }

  test("should reverse project property to property and variable to variable (for interesting)") {
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    val ioProp = InterestingOrder.interested(InterestingOrderCandidate.asc("y.foo"))
    val ioVar = InterestingOrder.interested(InterestingOrderCandidate.asc("y"))

    //when
    val resultProp = ioProp.withReverseProjectedColumns(projections, Set.empty)
    val resultVar = ioVar.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultProp should be(InterestingOrder.interested(InterestingOrderCandidate.asc("x.foo")))
    resultVar should be(InterestingOrder.interested(InterestingOrderCandidate.asc("x")))
  }

  test("should filter out empty interesting orders when reverse projecting") {
    // projection
    val projections = Map("y" -> Variable("x")(pos))

    val io = InterestingOrder.interested(InterestingOrderCandidate.empty)
      .interested(InterestingOrderCandidate.asc("y.foo")).interested(InterestingOrderCandidate.empty)

    //when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    //then
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc("x.foo")))
  }

  test("should not reverse project variable to variable if not argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc("y")).withReverseProjectedColumns(Map.empty, Set.empty)
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc("y")).withReverseProjectedColumns(Map.empty, Set("y"))
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc("y")))
  }
}
