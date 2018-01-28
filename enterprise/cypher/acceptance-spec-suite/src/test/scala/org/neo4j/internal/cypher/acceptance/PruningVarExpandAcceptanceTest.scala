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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite

class PruningVarExpandAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("should handle all predicate in optional match") {
    // Given the graph:
    graph.execute("""UNWIND range(1,1000) AS index
                    |CREATE (:Scaffold)-[:REL]->()-[:REL]->(:Molecule)""".stripMargin)

    // When
    val result = innerExecuteDeprecated( // using innerExecute because 3.2 gives stack overflow
      s"""CYPHER runtime=interpreted
         |MATCH (:Scaffold)-[:REL*3]->(m:Molecule)
         |RETURN DISTINCT m""".stripMargin, Map.empty)

    // Then
    result.toList should be(empty)
  }
}
