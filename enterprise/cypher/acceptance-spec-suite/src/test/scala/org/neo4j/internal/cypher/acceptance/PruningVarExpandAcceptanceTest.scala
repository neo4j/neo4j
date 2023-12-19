/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
