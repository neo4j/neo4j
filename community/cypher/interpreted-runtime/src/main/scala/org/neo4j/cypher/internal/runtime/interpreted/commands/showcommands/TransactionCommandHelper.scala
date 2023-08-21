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
package org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands

import org.neo4j.configuration.helpers.DatabaseNameValidator
import org.neo4j.dbms.database.DatabaseContext
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.impl.api.TransactionRegistry

import java.util.Objects

import scala.jdk.CollectionConverters.CollectionHasAsScala

case object TransactionCommandHelper {

  def isSelfOrAllows(
    username: String,
    actionOnResource: AdminActionOnResource,
    securityContext: SecurityContext
  ): Boolean =
    securityContext.subject.hasUsername(username) || securityContext.allowsAdminAction(actionOnResource).allowsAccess

  def getExecutingTransactions(databaseContext: DatabaseContext): Set[KernelTransactionHandle] = {
    val dependencies = databaseContext.dependencies
    if (dependencies != null)
      dependencies.resolveDependency(classOf[TransactionRegistry]).executingTransactions.asScala.toSet
    else Set.empty
  }
}

case class QueryId(internalId: Long) {
  private val PREFIX = "query-"
  private val EXPECTED_FORMAT_MSG = "(expected format: query-<id>)"

  if (internalId <= 0) throw new InvalidArgumentsException("Negative ids are not supported " + EXPECTED_FORMAT_MSG)

  override def toString: String = PREFIX + internalId
}

case class TransactionId(database: NormalizedDatabaseName, internalId: Long) {
  override def toString: String = database.name + TransactionId.SEPARATOR + internalId
}

object TransactionId {
  private val SEPARATOR = "-transaction-"
  private val EXPECTED_FORMAT_MSG = s"(expected format: <databasename>$SEPARATOR<id>)"

  val RUNNING_STATE = "Running"
  val CLOSING_STATE = "Closing"
  val COMMITTING_STATE = "Committing"
  val ROLLING_BACK_STATE = "Rolling back"
  val BLOCKED_STATE = "Blocked by: "
  val TERMINATED_STATE = "Terminated with reason: %s"

  def apply(dbName: String, internalId: Long): TransactionId = {
    if (internalId < 0) throw new InvalidArgumentsException("Negative ids are not supported " + EXPECTED_FORMAT_MSG)
    TransactionId(new NormalizedDatabaseName(Objects.requireNonNull(dbName)), internalId)
  }

  @throws[InvalidArgumentsException]
  def parse(transactionIdText: String): TransactionId = {
    try {
      val i = transactionIdText.lastIndexOf(SEPARATOR)
      if (i != -1) {
        val database = new NormalizedDatabaseName(transactionIdText.substring(0, i))
        DatabaseNameValidator.validateInternalDatabaseName(database)
        if (database.name.nonEmpty) {
          val tid = transactionIdText.substring(i + SEPARATOR.length)
          val internalId = tid.toLong
          return TransactionId(database.name, internalId)
        }
      }
    } catch {
      case e: NumberFormatException =>
        throw new InvalidArgumentsException("Could not parse id " + EXPECTED_FORMAT_MSG, e)
      case e: IllegalArgumentException =>
        throw new InvalidArgumentsException(e.getMessage, e)
    }
    throw new InvalidArgumentsException("Could not parse id " + EXPECTED_FORMAT_MSG)
  }
}
