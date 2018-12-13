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
package org.neo4j.internal.collector

import org.neo4j.cypher._

class DataCollectorQueriesAcceptanceTest extends ExecutionEngineFunSuite {

  import DataCollectorMatchers._

  test("should collect and retrieve queries") {
    // given
    execute("RETURN 'not collected!'")
    execute("CALL db.stats.collect('QUERIES')").single

    execute("MATCH (n) RETURN count(n)")
    execute("MATCH (n)-->(m) RETURN n,m")
    execute("WITH 'hi' AS x RETURN x+'-ho'")

    execute("CALL db.stats.stop('QUERIES')").single
    execute("RETURN 'not collected!'")

    // when
    val res = execute("CALL db.stats.retrieve('QUERIES')").toList

    // then
    res should beListWithoutOrder(
      beMapContaining(
        "section" -> "QUERIES",
        "data" -> beMapContaining(
          "query" -> "MATCH (n) RETURN count(n)"
        )
      ),
      beMapContaining(
        "section" -> "QUERIES",
        "data" -> beMapContaining(
          "query" -> "MATCH (n)-->(m) RETURN n,m"
        )
      ),
      beMapContaining(
        "section" -> "QUERIES",
        "data" -> beMapContaining(
          "query" -> "WITH 'hi' AS x RETURN x+'-ho'"
        )
      )
    )
  }

  test("should clear queries") {
    // given
    execute("CALL db.stats.collect('QUERIES')").single
    execute("MATCH (n) RETURN count(n)")
    execute("CALL db.stats.stop('QUERIES')").single

    // when
    execute("CALL db.stats.clear('QUERIES')").single
    execute("CALL db.stats.collect('QUERIES')").single
    execute("CALL db.stats.stop('QUERIES')").single

    // then
    execute("CALL db.stats.retrieve('QUERIES')").toList should be(empty)
  }

  test("should retrieve query execution plan and estimated rows") {
    // given
    execute("CREATE (a), (b), (c)")

    execute("CALL db.stats.collect('QUERIES')").single
    execute("MATCH (n) RETURN sum(id(n))")
    execute("MATCH (n) RETURN count(n)")
    execute("CALL db.stats.stop('QUERIES')").single

    // when
    val res = execute("CALL db.stats.retrieve('QUERIES')").toList

    // then
    res should beListWithoutOrder(
      beMapContaining(
        "section" -> "QUERIES",
        "data" -> beMapContaining(
          "query" -> "MATCH (n) RETURN sum(id(n))",
          "queryExecutionPlan" -> Map(
            "id" -> 0,
            "operator" -> "ProduceResults",
            "lhs" -> Map(
              "id" -> 1,
              "operator" -> "EagerAggregation",
              "lhs" -> Map(
                "id" -> 2,
                "operator" -> "AllNodesScan"
              )
            )
          ),
          "estimatedRows" -> List(1.0, 1.0, 3.0)
        )
      ),
      beMapContaining(
        "section" -> "QUERIES",
        "data" -> beMapContaining(
          "query" -> "MATCH (n) RETURN count(n)",
          "queryExecutionPlan" -> Map(
            "id" -> 0,
            "operator" -> "ProduceResults",
            "lhs" -> Map(
              "id" -> 1,
              "operator" -> "NodeCountFromCountStore"
            )
          ),
          "estimatedRows" -> List(1.0, 1.0)
        )
      )
    )
  }
}
