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

import org.neo4j.cypher.internal.v4_0.expressions.{Expression, Property, PropertyKeyName, Variable}
import org.neo4j.cypher.internal.v4_0.util.DummyPosition
import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite

class InterestingOrderTest extends CypherFunSuite {
  protected val pos = DummyPosition(0)

  private val variableX: Variable = Variable("x")(pos)
  private val variableXfoo: Variable = Variable("xfoo")(pos)
  private val variableY: Variable = Variable("y")(pos)
  private val propertyXfoo: Property = Property(variableX, PropertyKeyName("foo")(pos))(pos)
  private val propertyYfoo: Property = Property(variableY, PropertyKeyName("foo")(pos))(pos)

  test("should reverse project property to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableXfoo))
    // projection
    val projections = Map("xfoo" -> propertyXfoo)

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableXfoo, projections)))
  }

  test("should reverse project property to variable (descending)") {
    val io = InterestingOrder.required(RequiredOrderCandidate.desc("ignored", variableXfoo))
    // projection
    val projections = Map("xfoo" -> propertyXfoo)

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.desc("ignored", variableXfoo, projections)))
  }


  test("should reverse project property to property") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", propertyYfoo))
    // projection
    val projections = Map("y" -> variableX)

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", propertyYfoo, projections)))
  }

  test("should reverse project variable to variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY))
    // projection
    val projections = Map("y" -> variableX)

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY, projections)))
  }

  test("should reverse project variable to variable and then to another variable") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY))
    // projections
    val projection1 = Map("y" -> variableX)
    val projection2 = Map("x" -> Variable("z")(pos))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY, projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableX, projection2)))
  }

  test("should reverse project variable and property in multiple steps") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableXfoo))
    // projections ... WITH z AS y ... WITH y AS x ... WITH x.foo AS xfoo ORDER BY xfoo
    val projection1 = Map("xfoo" -> propertyXfoo)
    val projection2 = Map("x" -> variableY)
    val projection3 = Map("y" -> Variable("z")(pos))

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableXfoo, projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", propertyXfoo, projection2)))

    // and when
    val result3 = result2.withReverseProjectedColumns(projection3, Set.empty)

    // then
    result3 should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", propertyYfoo, projection3)))
  }

  test("should only keep relevant projections") {
    val io = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY))
    // projection
    val projections = Map("y" -> variableX, "bar" -> Variable("baz")(pos))

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    // then
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY, Map("y" -> variableX))))
  }

  test("should not reverse project variable to variable if not argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY)).withReverseProjectedColumns(Map.empty, Set.empty)
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument") {
    val result = InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY)).withReverseProjectedColumns(Map.empty, Set("y"))
    result should be(InterestingOrder.required(RequiredOrderCandidate.asc("ignored", variableY)))
  }

  // test the interesting part of the InterestingOrder

  test("should reverse project property to variable (for interesting)") {
    // projection
    val projections = Map("xfoo" -> propertyXfoo)

    val ioAsc = InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableXfoo))
    val ioDesc = InterestingOrder.interested(InterestingOrderCandidate.desc("ignored", variableXfoo))

    // when
    val resultAsc = ioAsc.withReverseProjectedColumns(projections, Set.empty)
    val resultDesc = ioDesc.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultAsc should be(InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableXfoo, projections)))
    resultDesc should be(InterestingOrder.interested(InterestingOrderCandidate.desc("ignored", variableXfoo, projections)))
  }

  test("should reverse project property to property and variable to variable (for interesting)") {
    // projection
    val projections = Map("y" -> variableX)

    val ioProp = InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", propertyYfoo))
    val ioVar = InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableY))

    // when
    val resultProp = ioProp.withReverseProjectedColumns(projections, Set.empty)
    val resultVar = ioVar.withReverseProjectedColumns(projections, Set.empty)

    // then
    resultProp should be(InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", propertyYfoo, projections)))
    resultVar should be(InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableY, projections)))
  }

  test("should filter out empty interesting orders when reverse projecting") {
    // projection
    val projections = Map("y" -> variableX)

    val io = InterestingOrder.interested(InterestingOrderCandidate.empty)
      .interested(InterestingOrderCandidate.asc("ignored", propertyYfoo)).interested(InterestingOrderCandidate.empty)

    // when
    val result = io.withReverseProjectedColumns(projections, Set.empty)

    //then
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", propertyYfoo, projections)))
  }

  test("should reverse project variable to variable and then to another variable (for interesting)") {
    val io = InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableY))
    // projections ... WITH x.foo AS xfoo ... WITH xfoo AS y
    val projection1 = Map("y" -> variableXfoo)
    val projection2 = Map("xfoo" -> propertyXfoo)

    // when
    val result1 = io.withReverseProjectedColumns(projection1, Set.empty)

    // then
    result1 should be(InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableY, projection1)))

    // and when
    val result2 = result1.withReverseProjectedColumns(projection2, Set.empty)

    // then
    result2 should be(InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableXfoo, projection2)))
  }

  test("should not reverse project variable to variable if not argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableY)).withReverseProjectedColumns(Map.empty, Set.empty)
    result should be(InterestingOrder.empty)
  }

  test("should reverse project variable to variable if is argument (for interesting)") {
    val result = InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableY)).withReverseProjectedColumns(Map.empty, Set("y"))
    result should be(InterestingOrder.interested(InterestingOrderCandidate.asc("ignored", variableY)))
  }
}
