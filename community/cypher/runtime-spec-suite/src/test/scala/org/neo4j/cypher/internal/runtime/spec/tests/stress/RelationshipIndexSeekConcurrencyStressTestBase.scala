/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cypher.internal.runtime.spec.tests.stress

import org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.stress.ConcurrencyStressTestBase.SIZE_HINT

abstract class RelationshipIndexSeekConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
                                                                                          edition: Edition[CONTEXT],
                                                                                          runtime: CypherRuntime[CONTEXT],
                                                                                        ) extends ConcurrencyStressTestBase[CONTEXT](edition, runtime) {

  test("should not return relationships with null end nodes from directed relationship") {
    seekWithConcurrentDeletes(directed = true)
  }

  test("should not return relationships with null end nodes from undirected relationship") {
    seekWithConcurrentDeletes(directed = false)
  }

  private def seekWithConcurrentDeletes(directed: Boolean): Unit = {
    val propValue = 42L
    val rels = given {
      relationshipIndexWithProvider(SchemaIndex.NATIVE30.providerName(), "R", "prop")
      val (_, rels) = circleGraph(nNodes = SIZE_HINT, relType = "R", outDegree = 1)
      rels.foreach(r => r.setProperty("prop", propValue))
      rels.map(_.getId)
    }

    // when
    val pattern = s"(n)-[r:R(prop = ???)]-${if (directed) ">" else ""}(m)"
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nId", "rId", "mId")
      .projection("id(n) AS nId", "id(r) AS rId", "id(m) AS mId")
      .relationshipIndexOperator(pattern, paramExpr = Some(toExpression(propValue)))
      .build()

    // then
    executeWithConcurrentDeletes(rels, logicalQuery)
  }
}