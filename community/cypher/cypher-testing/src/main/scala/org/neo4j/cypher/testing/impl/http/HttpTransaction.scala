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
package org.neo4j.cypher.testing.impl.http

import org.neo4j.cypher.testing.api.CypherExecutorTransaction
import org.neo4j.cypher.testing.api.StatementResult
import org.neo4j.cypher.testing.impl.http.HttpTransaction.statementPayload
import org.neo4j.test.server.HTTP

case class HttpTransaction(txHttp: HTTP.Builder, commitHttp: HTTP.Builder) extends CypherExecutorTransaction
    with HttpExceptionConverter {

  override def execute(statement: String, parameters: Map[String, Any]): StatementResult = convertExceptions {
    require(parameters.isEmpty, "Statement parameters isn't implemented yet")
    val resp = txHttp.POST("", statementPayload(statement))
    HttpStatementResult.fromResponse(resp)
  }

  override def commit(): Unit = convertExceptions {
    commitHttp.POST("")
  }

  override def rollback(): Unit = convertExceptions {
    txHttp.DELETE("")
  }
  override def close(): Unit = ???
}

object HttpTransaction {
  case class Request(statements: Seq[Statement])
  case class Statement(statement: String)

  def begin(dbHttp: HTTP.Builder): HttpTransaction = {
    val resp = dbHttp.POST("tx")
    HttpTransaction(
      dbHttp.withBaseUri(resp.location()),
      dbHttp.withBaseUri(resp.stringFromContent("commit"))
    )
  }

  def execute(dbHttp: HTTP.Builder, statement: String, parameters: Map[String, Any]): StatementResult = {
    require(parameters.isEmpty, "Statement parameters isn't implemented yet")
    val resp = dbHttp.POST("tx/commit", statementPayload(statement))
    HttpStatementResult.fromResponse(resp)
  }

  private def statementJsonString(statement: String): String = {
    HttpJson.write(Request(Seq(Statement(statement))))
  }

  private def statementPayload(statement: String): HTTP.RawPayload =
    HTTP.RawPayload.rawPayload(statementJsonString(statement))
}
