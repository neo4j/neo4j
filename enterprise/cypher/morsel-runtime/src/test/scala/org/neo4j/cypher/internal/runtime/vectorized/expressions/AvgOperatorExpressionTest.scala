/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.runtime.vectorized.expressions

import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.internal.runtime.vectorized.MorselExecutionContext
import org.neo4j.cypher.internal.runtime.vectorized.operators.DummyExpression
import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.longValue
import org.neo4j.values.virtual.VirtualValues.list

class AvgOperatorExpressionTest extends CypherFunSuite {

  test("should do average mapping") {
    //given
    val mapper = AvgOperatorExpression(
      new DummyExpression(longValue(1), longValue(2), longValue(3), longValue(4), longValue(5))).createAggregationMapper

    //when
    1 to 5 foreach(_ => mapper.map(mock[MorselExecutionContext], mock[QueryState]))

    //then
    mapper.result should equal(list(longValue(5), longValue(15)))
  }

  test("should handle mapping of no result") {
    //given
    val mapper = AvgOperatorExpression(new DummyExpression()).createAggregationMapper

    //when doing nothing at all

    //then
    mapper.result should equal(list(longValue(0), longValue(0)))
  }

  test("should do average reducing") {
    //given
    val reducer = AvgOperatorExpression(new DummyExpression()).createAggregationReducer

    //when
    reducer.reduce(list(longValue(10), longValue(10)))
    reducer.reduce(list(longValue(5), longValue(10)))

    //then
    reducer.result should equal(Values.doubleValue(4/3.0))
  }

  test("should handle empty average reducing") {
    //given
    val reducer = AvgOperatorExpression(new DummyExpression()).createAggregationReducer

    //when doing absolutely nothing

    //then
    reducer.result should equal(Values.NO_VALUE)
  }
}
