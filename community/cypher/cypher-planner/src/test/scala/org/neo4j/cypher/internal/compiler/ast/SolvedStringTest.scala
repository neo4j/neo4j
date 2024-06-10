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
package org.neo4j.cypher.internal.compiler.ast

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class SolvedStringTest extends CypherFunSuite with LogicalPlanningTestSupport with TableDrivenPropertyChecks {

  private val tests = Table(
    "Cypher" -> "Expected",
    // Test different selectors
    "ANY SHORTEST (a)-[r]->(b)" -> "SHORTEST 1 (a)-[r]->(b)",
    "SHORTEST 1 (a)-[r]->(b)" -> "SHORTEST 1 (a)-[r]->(b)",
    "SHORTEST 2 (a)-[r]->(b)" -> "SHORTEST 2 (a)-[r]->(b)",
    "SHORTEST 1 GROUPS (a)-[r]->+(b)" -> "SHORTEST 1 GROUPS (a) ((anon_2)-[r]->(anon_4)){1, } (b)",
    // Test different quantifiers
    "ANY SHORTEST (a) ((b)-[r]->(c))+ (d)" -> "SHORTEST 1 (a) ((b)-[r]->(c)){1, } (d)",
    "ANY SHORTEST (a) ((b)-[r]->(c))* (d)" -> "SHORTEST 1 (a) ((b)-[r]->(c)){0, } (d)",
    "ANY SHORTEST (a) ((b)-[r]->(c)){2, 5} (d)" -> "SHORTEST 1 (a) ((b)-[r]->(c)){2, 5} (d)",
    "ANY SHORTEST (a) ((b)-[r]->(c)){2, } (d)" -> "SHORTEST 1 (a) ((b)-[r]->(c)){2, } (d)",
    "ANY SHORTEST (a) ((b)-[r]->(c)){2} (d)" -> "SHORTEST 1 (a) ((b)-[r]->(c)){2, 2} (d)",
    "ANY SHORTEST (a) ((b)-[r]->(c)){, 5} (d)" -> "SHORTEST 1 (a) ((b)-[r]->(c)){0, 5} (d)",
    // Test predicates are not rendered
    "ANY SHORTEST (a:A)-[r:R]->(b:B)" -> "SHORTEST 1 (a)-[r]->(b)",
    "ANY SHORTEST ((a)-[r:R]->(b:B)-[r2:R2]->(c) WHERE a.prop + b.prop = c.prop)" -> "SHORTEST 1 (a)-[r]->(b)-[r2]->(c)",
    "ANY SHORTEST ((a)-[r:R]->(b:B)-[r2:R2]->(c) WHERE r.prop = 0 AND r2.prop = 0)" -> "SHORTEST 1 (a)-[r]->(b)-[r2]->(c)",
    "ANY SHORTEST (a) ((b:B)-[r:R]->(c:C) WHERE r.prop = 0)+ (d)" -> "SHORTEST 1 (a) ((b)-[r]->(c)){1, } (d)",
    // Test different patterns
    "ANY SHORTEST (a)-[r:R]-(b)<-[r2:R2]-(c) ((c_in)-[q:Q]->(d_in))+ (d) ((d_in2)-[q2:Q2]->(e_in))+ (e)" -> "SHORTEST 1 (a)-[r]-(b)<-[r2]-(c) ((c_in)-[q]->(d_in)){1, } (d) ((d_in2)-[q2]->(e_in)){1, } (e)",
    "ANY SHORTEST (a) ((a_in)-[r:R]->(b_in)<-[r2:R2]-(c_in))+ (c)-[r3:R3]-(d)" -> "SHORTEST 1 (a) ((a_in)-[r]->(b_in)<-[r2]-(c_in)){1, } (c)-[r3]-(d)",
    // Test backticked identifiers
    "ANY SHORTEST (` n@0`)-[`r`]->(`123`)" -> "SHORTEST 1 (` n@0`)-[r]->(`123`)",
    "ANY SHORTEST (` UNNAMED`) ((` n@0`)-[`r`]->(`123`))+ (`987other`)" -> "SHORTEST 1 (` UNNAMED`) ((` n@0`)-[r]->(`123`)){1, } (`987other`)"
  )

  test("solvedString is correct") {
    forAll(tests) {
      case (cypher, expected) =>
        val fullCypher = s"MATCH $cypher RETURN *"
        val query = buildSinglePlannerQuery(fullCypher)
        query.queryGraph.selectivePathPatterns should not be empty
        val spp = query.queryGraph.selectivePathPatterns.head
        val solvedString = spp.solvedString

        solvedString should equal(expected)
    }
  }

}
