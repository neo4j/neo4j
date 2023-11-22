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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalatest.prop.TableDrivenPropertyChecks

class SolvedStringTest extends CypherFunSuite with LogicalPlanningTestSupport with TableDrivenPropertyChecks {

  override val semanticFeatures: List[SemanticFeature] = List(
    SemanticFeature.GpmShortestPath
  )

  private val tests = Table(
    "Cypher" -> "Expected",
    // Test different selectors
    "ANY SHORTEST (a)-[r]->(b)" -> "SHORTEST 1 ((a)-[r]->(b))",
    "SHORTEST 1 (a)-[r]->(b)" -> "SHORTEST 1 ((a)-[r]->(b))",
    "SHORTEST 2 (a)-[r]->(b)" -> "SHORTEST 2 ((a)-[r]->(b))",
    "SHORTEST 1 GROUPS (a)-[r]->+(b)" -> "SHORTEST 1 GROUPS ((a) ((anon_2)-[r]->(anon_4)){1, } (b) WHERE unique(r))",
    // Test different quantifiers
    "ANY SHORTEST (a) ((b)-[r]->(c))+ (d)" -> "SHORTEST 1 ((a) ((b)-[r]->(c)){1, } (d) WHERE unique(r))",
    "ANY SHORTEST (a) ((b)-[r]->(c))* (d)" -> "SHORTEST 1 ((a) ((b)-[r]->(c)){0, } (d) WHERE unique(r))",
    "ANY SHORTEST (a) ((b)-[r]->(c)){2, 5} (d)" -> "SHORTEST 1 ((a) ((b)-[r]->(c)){2, 5} (d) WHERE unique(r))",
    "ANY SHORTEST (a) ((b)-[r]->(c)){2, } (d)" -> "SHORTEST 1 ((a) ((b)-[r]->(c)){2, } (d) WHERE unique(r))",
    "ANY SHORTEST (a) ((b)-[r]->(c)){2} (d)" -> "SHORTEST 1 ((a) ((b)-[r]->(c)){2, 2} (d) WHERE unique(r))",
    "ANY SHORTEST (a) ((b)-[r]->(c)){, 5} (d)" -> "SHORTEST 1 ((a) ((b)-[r]->(c)){0, 5} (d) WHERE unique(r))",
    // Test predicates
    "ANY SHORTEST (a:A)-[r:R]->(b:B)" -> "SHORTEST 1 ((a)-[r:R]->(b))", // Note: Predicates on boundary nodes are moved to outer QG
    "ANY SHORTEST ((a)-[r:R]->(b:B)-[r2:R2]->(c) WHERE a.prop + b.prop = c.prop)" -> "SHORTEST 1 ((a)-[r:R]->(b)-[r2:R2]->(c) WHERE b:B AND c.prop IN [a.prop + b.prop])",
    "ANY SHORTEST ((a)-[r:R]->(b:B)-[r2:R2]->(c) WHERE r.prop = 0 AND r2.prop = 0)" -> "SHORTEST 1 ((a)-[r:R]->(b)-[r2:R2]->(c) WHERE b:B AND r.prop IN [0] AND r2.prop IN [0])",
    "ANY SHORTEST (a) ((b:B)-[r:R]->(c:C) WHERE r.prop = 0)+ (d)" -> "SHORTEST 1 ((a) ((b)-[r:R]->(c) WHERE b:B AND c:C AND r.prop IN [0]){1, } (d) WHERE unique(r))",
    // Test different patterns
    "ANY SHORTEST (a)-[r:R]-(b)<-[r2:R2]-(c) ((c_in)-[q:Q]->(d_in))+ (d) ((d_in2)-[q2:Q2]->(e_in))+ (e)" -> "SHORTEST 1 ((a)-[r:R]-(b)<-[r2:R2]-(c) ((c_in)-[q:Q]->(d_in)){1, } (d) ((d_in2)-[q2:Q2]->(e_in)){1, } (e) WHERE unique(q) AND unique(q2))",
    "ANY SHORTEST (a) ((a_in)-[r:R]->(b_in)<-[r2:R2]-(c_in))+ (c)-[r3:R3]-(d)" -> "SHORTEST 1 ((a) ((a_in)-[r:R]->(b_in)<-[r2:R2]-(c_in)){1, } (c)-[r3:R3]-(d) WHERE unique(r + r2))"
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
