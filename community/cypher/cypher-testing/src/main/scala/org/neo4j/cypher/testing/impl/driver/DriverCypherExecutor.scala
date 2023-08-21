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
package org.neo4j.cypher.testing.impl.driver

import org.neo4j.cypher.testing.api.CypherExecutor
import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.driver.Session
import org.neo4j.driver.TransactionConfig

import java.time.Duration

import scala.jdk.CollectionConverters.MapHasAsJava

case class DriverCypherExecutor(private val session: Session) extends CypherExecutor with DriverExceptionConverter {

  override def beginTransaction(): CypherExecutorTransaction = DriverTransaction(session.beginTransaction())

  override def beginTransaction(conf: CypherExecutor.TransactionConfig): CypherExecutorTransaction = {
    DriverTransaction(session.beginTransaction(asDriverConf(conf)))
  }

  override def execute[T](
    queryToExecute: String,
    neo4jParams: Map[String, Object],
    converter: StatementResult => T
  ): T = convertExceptions {
    converter(DriverStatementResult(session.run(queryToExecute, neo4jParams.asJava, TransactionConfig.empty())))
  }

  override def close(): Unit = session.close()
  override def sessionBased: Boolean = true

  private def asDriverConf(txConf: CypherExecutor.TransactionConfig): TransactionConfig = {
    val builder = TransactionConfig.builder()
    txConf.timeout.foreach(t => builder.withTimeout(Duration.ofMillis(t.toMillis)))
    txConf.metadata.foreach(m => builder.withMetadata(m.asJava))
    builder.build()
  }
}
