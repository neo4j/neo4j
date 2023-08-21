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
package org.neo4j.cypher.internal.runtime

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class SelectivityTrackerTest extends CypherFunSuite {

  test("should not sort before minimum number of rows finished") {
    val st = new SelectivityTracker(3)
    st.getOrder() shouldBe Array(0, 1, 2)

    for (i <- 1L until SelectivityTracker.MIN_ROWS_BEFORE_SORT) {
      st.onPredicateResult(0, isTrue = true)
      st.onPredicateResult(1, i % 2 == 0)
      st.onPredicateResult(2, i % 3 == 0)
      st.onRowFinished()
    }

    st.getOrder() shouldBe Array(0, 1, 2)
  }

  test("should sort after minimum number of rows finished") {
    val st = new SelectivityTracker(3)
    st.getOrder() shouldBe Array(0, 1, 2)

    for (i <- 1L to SelectivityTracker.MIN_ROWS_BEFORE_SORT) {
      st.onPredicateResult(0, isTrue = true)
      st.onPredicateResult(1, i % 2 == 0)
      st.onPredicateResult(2, i % 3 == 0)
      st.onRowFinished()
    }

    st.getOrder() shouldBe Array(2, 1, 0)
  }
}
