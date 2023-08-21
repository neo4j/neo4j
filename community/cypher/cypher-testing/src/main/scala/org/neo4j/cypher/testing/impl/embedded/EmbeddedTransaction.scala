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

import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.graphdb.Transaction

case class EmbeddedTransaction(private val embeddedTransaction: Transaction) extends CypherExecutorTransaction
    with EmbeddedExceptionConverter {

  override def execute(statement: String, parameters: Map[String, Any]): StatementResult = convertExceptions {
    EmbeddedStatementResult(embeddedTransaction.execute(
      statement,
      EmbeddedParameterConverter.convertParameters(parameters)
    ))
  }

  override def commit(): Unit = convertExceptions {
    embeddedTransaction.commit()
  }

  override def rollback(): Unit = convertExceptions {
    embeddedTransaction.rollback()
  }
  override def close(): Unit = embeddedTransaction.close()
}
