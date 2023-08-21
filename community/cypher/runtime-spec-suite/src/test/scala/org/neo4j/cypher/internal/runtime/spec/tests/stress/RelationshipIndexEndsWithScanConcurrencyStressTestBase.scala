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
package org.neo4j.cypher.internal.runtime.spec.tests.stress

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.tests.stress.ConcurrencyStressTestBase.SIZE_HINT
import org.neo4j.graphdb.schema.IndexType

abstract class RelationshipIndexEndsWithScanConcurrencyStressTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT]
) extends ConcurrencyStressTestBase[CONTEXT](edition, runtime) {

  test("should not return relationships with null end nodes from directed relationship") {
    executeWithConcurrentDeletes(directed = true)
  }

  test("should not return relationships with null end nodes from undirected relationship") {
    executeWithConcurrentDeletes(directed = false)
  }

  private def executeWithConcurrentDeletes(directed: Boolean): Unit = {
    val rels = given {
      relationshipIndex(IndexType.TEXT, "R", "text")
      val (_, rels) = circleGraph(SIZE_HINT)
      rels.zipWithIndex.foreach {
        case (r, i) if i % 2 == 0 => r.setProperty("text", "CASE")
        case (r, _)               => r.setProperty("text", "case")
      }
      rels.map(_.getId)
    }

    // when
    val pattern = s"(n)-[r:R(text ENDS WITH 'se')]-${if (directed) ">" else ""}(m)"
    val logicalQuery = new LogicalQueryBuilder(this)
      .produceResults("nId", "rId", "mId")
      .projection("id(n) AS nId", "id(r) AS rId", "id(m) AS mId")
      .relationshipIndexOperator(pattern, indexType = IndexType.TEXT)
      .build()

    executeWithConcurrentDeletes(rels, logicalQuery)
  }
}
