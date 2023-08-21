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
package org.neo4j.cypher.testing.impl.embedded

import org.neo4j.cypher.testing.api.CypherExecutor
import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Result
import org.neo4j.kernel.impl.coreapi.TransactionImpl

import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters.MapHasAsJava

case class EmbeddedCypherExecutor(private val graph: GraphDatabaseService) extends CypherExecutor
    with EmbeddedExceptionConverter {
  override def beginTransaction(): CypherExecutorTransaction = EmbeddedTransaction(graph.beginTx())

  override def beginTransaction(conf: CypherExecutor.TransactionConfig): CypherExecutorTransaction = {
    val tx = conf.timeout match {
      case Some(timeout) => graph.beginTx(timeout.toMillis, TimeUnit.MILLISECONDS)
      case None          => graph.beginTx()
    }
    conf.metadata.foreach(metadata => tx.asInstanceOf[TransactionImpl].setMetaData(metadata.asJava))
    EmbeddedTransaction(tx)
  }

  override def execute[T](
    queryToExecute: String,
    neo4jParams: Map[String, Object],
    converter: StatementResult => T
  ): T = convertExceptions {
    graph.executeTransactionally(
      queryToExecute,
      neo4jParams.asJava,
      (graphDbResult: Result) => converter(EmbeddedStatementResult(graphDbResult))
    )
  }

  override def close(): Unit = {}
  override def sessionBased: Boolean = false
}
