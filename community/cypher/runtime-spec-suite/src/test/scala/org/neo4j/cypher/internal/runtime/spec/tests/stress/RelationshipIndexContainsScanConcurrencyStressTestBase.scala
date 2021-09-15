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

abstract class RelationshipIndexContainsScanConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
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
    val sizeHint = 10000
    val rels = given {
      relationshipIndex("R", "text")
      val (_, rels) = circleGraph(sizeHint)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _) => r.setProperty("text", "case")
      }
      rels.map(_.getId)
    }

    // when
    val pattern = s"(n)-[r:R(text CONTAINS 'as')]-${if (directed) ">" else ""}(m)"
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nId", "rId", "mId")
      .projection("id(n) AS nId", "id(r) AS rId", "id(m) AS mId")
      .relationshipIndexOperator(pattern)
      .build()

    executeWithConcurrentDeletes(rels, logicalQuery)
  }
}