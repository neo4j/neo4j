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
package org.neo4j.cypher

/**
 * Regression tests for issue #13942.
 *
 * `keys(entity)` observes the property-key domain of a node or relationship, so a preceding
 * clause that SETs a (potentially new) property conflicts with a later `keys()` read.
 * The planner must insert an Eager barrier between the two clause phases; otherwise the
 * second clause can observe transaction state from an incomplete preceding clause, and
 * semantically equivalent query forms (e.g. with and without a Collect/Unwind
 * materialization) disagree on the result multiset.
 *
 * The correct result multiset for both the direct and the materialized form is {0, 0, 2, 2}:
 * after the MERGE clause has completely executed, `n0` has the property-key domain
 * {id, k11, k0}, so `coll.remove(keys(n0), 0)` has cardinality 2 for both input rows.
 */
class KeysEagerIT extends ExecutionEngineFunSuite {

  private val directQuery =
    """FOR j IN [0, 2]
      |MERGE (m:M {id: 129})-[:R]->(n0:N {id: 130, k11: true})
      |ON MATCH SET n0.k0 = 0
      |FOR k IN coll.remove(keys(n0), 0)
      |RETURN j AS row""".stripMargin

  private val materializedQuery =
    """FOR j IN [0, 2]
      |MERGE (m:M {id: 129})-[:R]->(n0:N {id: 130, k11: true})
      |ON MATCH SET n0.k0 = 0
      |FOR k IN coll.remove(keys(n0), 0)
      |WITH j AS row
      |WITH collect(row) AS rows
      |UNWIND rows AS row
      |RETURN row""".stripMargin

  private val reducedQuery =
    """FOR j IN [0, 1]
      |MERGE (n:N {id: 1})
      |ON MATCH SET n.k = 1
      |RETURN j, size(keys(n)) AS keyCount""".stripMargin

  private def clearGraph(): Unit = {
    execute("MATCH (n) DETACH DELETE n").toList
  }

  Seq("slotted", "pipelined").foreach { runtime =>
    test(s"direct keys() query observes completed MERGE state on $runtime") {
      clearGraph()
      val result = execute(s"CYPHER 25 runtime=$runtime $directQuery")
      val rows = result.toList.map(_("row").toString)
      rows should contain theSameElementsAs Seq("0", "0", "2", "2")

      // write-state oracle: the fix changes planning, not mutation intent.
      // propertiesSet counts the 3 created properties (m.id, n0.id, n0.k11) plus the single ON MATCH SET (n0.k0).
      val statistics = result.queryStatistics()
      statistics.getNodesCreated should equal(2)
      statistics.getRelationshipsCreated should equal(1)
      statistics.getPropertiesSet should equal(4)

      val state =
        execute("MATCH (m:M {id: 129})-[:R]->(n0:N {id: 130}) RETURN n0.id AS id, n0.k11 AS k11, n0.k0 AS k0").toList
      state should have size 1
      state.head("id").toString should equal("130")
      state.head("k11").toString should equal("true")
      state.head("k0").toString should equal("0")
    }

    test(s"materialized keys() query agrees with direct form on $runtime") {
      clearGraph()
      val rows = execute(s"CYPHER 25 runtime=$runtime $materializedQuery").toList.map(_("row").toString)
      rows should contain theSameElementsAs Seq("0", "0", "2", "2")
    }

    test(s"reduced MERGE/keys() regression on $runtime") {
      clearGraph()
      val rows = execute(s"CYPHER 25 runtime=$runtime $reducedQuery").toList
        .map(row => (row("j").toString, row("keyCount").toString))
      rows should contain theSameElementsAs Seq(("0", "2"), ("1", "2"))
    }
  }

  test("repaired plan contains an Eager barrier") {
    val description = execute(s"CYPHER 25 EXPLAIN $directQuery").executionPlanDescription()
    description.toString should include("Eager")
  }
}
