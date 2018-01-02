/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.{CypherFunSuite, CypherTestSupport}
import org.neo4j.cypher.internal.helpers.TxCounts

trait TxCountsTrackingTestSupport extends CypherTestSupport {
  self: CypherFunSuite with GraphDatabaseTestSupport with ExecutionEngineTestSupport =>

  def executeAndTrackTxCounts(queryText: String, params: (String, Any)*): (InternalExecutionResult, TxCounts) = {
    val (result, txCounts) = prepareAndTrackTxCounts(execute(queryText, params: _*))
    (result, txCounts)
  }

  def executeScalarAndTrackTxCounts[T](queryText: String, params: (String, Any)*): (T, TxCounts) =
    prepareAndTrackTxCounts(executeScalar[T](queryText, params: _*))

  def prepareAndTrackTxCounts[T](f: => T): (T, TxCounts) = {
    // prepare
    f
    deleteAllEntities()

    val initialTxCounts = graph.txCounts
    val result = f
    val txCounts = graph.txCounts - initialTxCounts

    (result, txCounts)
  }
}
