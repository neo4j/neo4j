/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.tests.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder

abstract class RelationshipTypeScanConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
                                                                                         edition: Edition[CONTEXT],
                                                                                         runtime: CypherRuntime[CONTEXT],
                                                                                       ) extends ConcurrencyStressTestBase[CONTEXT](edition, runtime) {

  test("should not return relationships with null end nodes from directed relationship") {
    executeWithConcurrentDeletes(directed = true)
  }

  test("should not return relationships with null end nodes from undirected relationship") {
    executeWithConcurrentDeletes(directed = false)
  }

  private def executeWithConcurrentDeletes(directed: Boolean): Unit = {
    // given
    val sizeHint = 10000
    val rels = given {
      val (_, rels) = circleGraph(nNodes = sizeHint, relType = "R", outDegree = 1)
      rels.map(_.getId)
    }

    // when
    val pattern = s"(n)-[r:R]-${if (directed) ">" else ""}(m)"
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nId", "rId", "mId")
      .projection("id(n) AS nId", "id(r) AS rId", "id(m) AS mId")
      .relationshipTypeScan(pattern)
      .build()

    // then
    executeWithConcurrentDeletes(rels, logicalQuery)
  }
}
