/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler

import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.exceptions.CantCompileQueryException

class ExecutionModelTest extends CypherFunSuite {
  Seq(1, 2, 3, 4, 1000, 1024, 2047, 2048, 2049, 2051, 2551, 9973, 10000, 1046527, 9999999, 13466917, 20482047, 20482048, 9999999999L, Long.MaxValue).foreach {
    explicitBatchSize =>
      test(s"explicit batch size: $explicitBatchSize") {
        val executionModel = ExecutionModel.Batched.default
        val batchSize = executionModel.selectBatchSize(null, null, Some(explicitBatchSize))
        val diff = explicitBatchSize % batchSize
        val offBy = if (diff != 0) s" off by $diff" else ""
        println(s"$explicitBatchSize => $batchSize$offBy")
      }
  }

  Seq(0, -1, Long.MinValue).foreach { explicitBatchSize =>
    test(s"explicit batch size not supported: $explicitBatchSize") {
      val executionModel = ExecutionModel.Batched.default
      a [CantCompileQueryException] should be thrownBy {
        executionModel.selectBatchSize(null, null, Some(explicitBatchSize))
      }
    }
  }
}
