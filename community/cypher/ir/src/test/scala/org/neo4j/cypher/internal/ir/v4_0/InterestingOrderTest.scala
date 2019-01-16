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
  test("should reverse project property to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo")))
    // projection
    val projections = Map("xfoo" -> prop("x", "foo"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), projections)))
  }

  test("should reverse project property to variable (descending)") {
    val io = InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo")))
    // projection
    val projections = Map("xfoo" -> prop("x", "foo"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.desc(varFor("xfoo"), projections)))
  }


  test("should reverse project property to property") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "foo")))
    // projection
    val projections = Map("y" -> varFor("x"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "foo"), projections)))
  }

  test("should reverse project variable to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y")))
    // projection
    val projections = Map("y" -> varFor("x"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projections)))
  }

  test("should reverse project variable to variable and then to another variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y")))
    // projections
    val projection1 = Map("y" -> varFor("x"))
    val projection2 = Map("x" -> varFor("z"))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("x"), projection2)))
  }

  test("should reverse project variable and property in multiple steps") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo")))
    // projections ... WITH z AS y ... WITH y AS x ... WITH x.foo AS xfoo ORDER BY xfoo
    val projection1 = Map("xfoo" -> prop("x", "foo"))
    val projection2 = Map("x" -> varFor("y"))
    val projection3 = Map("y" -> varFor("z"))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("x", "foo"), projection2)))

    // and when
    val result3 = result2.withReverseProjectedColumns(projection3, Set.empty)

    // then
    result3 should be(InterestingOrder.required(RequiredOrderCandidate.asc(prop("y", "foo"), projection3)))
  }

  test("should apply old projections") {
    val projection1 = Map("z" -> varFor("y"))
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("z"), projection1))
    val projection2 = Map("y" -> varFor("x"))

    val result = io.withReverseProjectedColumns(projection2, Set.empty)
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projection2)))
  }

  test("should only keep relevant projections") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y")))
    // projection
    val projections = Map("y" -> varFor("x"), "bar" -> varFor("baz"))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), Map("y" -> varFor("x")))))
  }

  test("should not reverse project variable to variable if not argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(Map.empty, Set.empty)
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(Map.empty, Set("y"))
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))))
  }

  test("should reverse project variable to variable if is argument when projections exist") {
    val projections = Map("y" -> varFor("x"))
    val result = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(projections, Set("y"))
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), projections)))
  }

  test("should reverse project variable to variable if is argument when old projections exist") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("y"), Map("y" -> varFor("x"))))
    val result = io.withReverseProjectedColumns(Map.empty, Set("x"))
    result should be(io)
  }

  // test the interesting part of the InterestingOrder

  test("should reverse project property to variable (for both)") {
    val projection = Map("xfoo" -> prop("x", "foo"), "yfoo" -> prop("y", "foo"))

    val io = InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"))).interested(InterestingOrderCandidate.desc(varFor("yfoo")))

    val result = io.withReverseProjectedColumns(projection, Set.empty)
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc(varFor("xfoo"), Map("xfoo" -> prop("x", "foo"))))
      .interested(InterestingOrderCandidate.desc(varFor("yfoo"), Map("yfoo" -> prop("y", "foo")))))
  }

  test("should reverse project property to variable (for interesting)") {
    // projection
    val projections = Map("xfoo" -> prop("x", "foo"))

    val ioAsc = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo")))
    val ioDesc = InterestingOrder.interested(InterestingOrderCandidate.desc(varFor("xfoo")))

    // when
    val resultAsc = ioAsc.withReverseProjectedColumns(projections, Set.empty)
    val resultDesc = ioDesc.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultAsc should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo"), projections)))
    resultDesc should be(InterestingOrder.interested(InterestingOrderCandidate.desc(varFor("xfoo"), projections)))
  }

  test("should reverse project property to property and variable to variable (for interesting)") {
    // projection
    val projections = Map("y" -> varFor("x"))

    val ioProp = InterestingOrder.interested(InterestingOrderCandidate.asc(prop("y", "foo")))
    val ioVar = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y")))

    // when
    val resultProp = ioProp.withReverseProjectedColumns(projections, Set.empty)
    val resultVar = ioVar.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultProp should be(InterestingOrder.interested(InterestingOrderCandidate.asc(prop("y", "foo"), projections)))
    resultVar should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"), projections)))
  }

  test("should filter out empty interesting orders when reverse projecting") {
    // projection
    val projections = Map("y" -> varFor("x"))

    val io = InterestingOrder.interested(InterestingOrderCandidate.empty)
      .interested(InterestingOrderCandidate.asc(prop("y", "foo"))).interested(InterestingOrderCandidate.empty)

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    //then
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc(prop("y", "foo"), projections)))
  }

  test("should reverse project variable to variable and then to another variable (for interesting)") {
    val io = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y")))
    // projections ... WITH x.foo AS xfoo ... WITH xfoo AS y
    val projection1 = Map("y" -> varFor("xfoo"))
    val projection2 = Map("xfoo" -> prop("x", "foo"))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"), projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("xfoo"), projection2)))
  }

  test("should not reverse project variable to variable if not argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(Map.empty, Set.empty)
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"))).withReverseProjectedColumns(Map.empty, Set("y"))
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc(varFor("y"))))
  }

  private def varFor(name: String) = Variable(name)(DummyPosition(0))
  private def prop(varName: String, propName: String) = Property(varFor(varName), PropertyKeyName(propName)(DummyPosition(0)))(DummyPosition(0))
}
