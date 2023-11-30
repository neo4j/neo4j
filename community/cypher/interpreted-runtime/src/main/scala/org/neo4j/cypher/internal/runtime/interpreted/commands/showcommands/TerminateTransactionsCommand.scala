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

import org.neo4j.cypher.internal.ast.CommandResultItem
import org.neo4j.cypher.internal.ast.ShowColumn
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause.messageColumn
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause.transactionIdColumn
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause.usernameColumn
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Expression
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.exceptions.InvalidSemanticsException
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.PrivilegeAction.TERMINATE_TRANSACTION
import org.neo4j.internal.kernel.api.security.UserSegment
import org.neo4j.kernel.api.KernelTransactionHandle
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.database.NamedDatabaseId
import org.neo4j.logging.InternalLog
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

// TERMINATE TRANSACTION[S] transaction-id[,...]
case class TerminateTransactionsCommand(
  givenIds: Either[List[String], Expression],
  columns: List[ShowColumn],
  yieldColumns: List[CommandResultItem]
) extends Command(columns, yieldColumns) {

  override def originalNameRows(state: QueryState, baseRow: CypherRow): ClosingIterator[Map[String, AnyValue]] = {
    val ids = Command.extractNames(givenIds, state, baseRow)
    if (ids.isEmpty) throw new InvalidSemanticsException(
      "Missing transaction id to terminate, the transaction id can be found using `SHOW TRANSACTIONS`."
    )
    val ctx = state.query
    val securityContext = ctx.transactionalContext.securityContext
    val executingUser = securityContext.subject.executingUser()
    val authenticatedUser = securityContext.subject.authenticatedUser()
    val loggingUser = if (securityContext.impersonating()) s"$authenticatedUser:$executingUser" else executingUser
    val log: InternalLog = state.query.logProvider.getLog(getClass)

    log.info("User %s trying to terminate transactions: %s.", loggingUser, ids.mkString("[", ", ", "]"))

    val databaseManager = ctx.getDatabaseContextProvider
    val databaseIdRepository = databaseManager.databaseIdRepository

    val (transactionsByDatabase, otherTxIds) =
      ids.foldLeft[(Map[NamedDatabaseId, Set[TransactionId]], Set[TransactionId])]((Map.empty, Set.empty)) {
        case ((accMap, accSet), idText) =>
          val id = TransactionId.parse(idText)
          val namedDatabaseId = databaseIdRepository.getByName(id.database)
          if (namedDatabaseId.isPresent) {
            val databaseId = namedDatabaseId.get
            val transactions = accMap.getOrElse(databaseId, Set.empty[TransactionId])
            (accMap ++ Map(databaseId -> (transactions + id)), accSet)
          } else {
            // transactions on non-existing databases
            // added to get a result for 'transaction not found' instead of just missing from the result set
            (accMap, accSet + id)
          }
      }

    val rows = transactionsByDatabase.flatMap {
      case (databaseId: NamedDatabaseId, txIds: Set[TransactionId]) =>
        val maybeDatabaseContext = databaseManager.getDatabaseContext(databaseId)
        val allowedTransactions =
          if (maybeDatabaseContext.isPresent) {
            val dbName = databaseId.name
            val databaseContext = maybeDatabaseContext.get
            val dbScope = new AdminActionOnResource.DatabaseScope(dbName)
            TransactionCommandHelper.getExecutingTransactions(databaseContext).map(tx => {
              val txIdRepresentation = TransactionId(dbName, tx.getTransactionSequenceNumber)
              (txIdRepresentation, tx)
            }).filter {
              case (txIdRepresentation, tx) =>
                val username = tx.subject.executingUser()
                val action = new AdminActionOnResource(TERMINATE_TRANSACTION, dbScope, new UserSegment(username))
                TransactionCommandHelper.isSelfOrAllows(username, action, securityContext) && txIds.contains(
                  txIdRepresentation
                )
            }.toMap[TransactionId, KernelTransactionHandle]
          } else Map.empty[TransactionId, KernelTransactionHandle]

        txIds.map(txId => {
          val txHandle = allowedTransactions.get(txId)
          val (username, message) = txHandle.map {
            case handle if handle.isClosing =>
              (handle.subject.executingUser(), "Unable to terminate closing transactions.")
            case handle =>
              handle.markForTermination(Status.Transaction.Terminated)
              log.info("User %s terminated transaction %s.", loggingUser, txId.toString)
              (handle.subject.executingUser(), "Transaction terminated.")
          }.getOrElse((null, "Transaction not found."))
          getResultMap(txId, username, message)
        })
    }
    // Add 'transaction not found' results for the ids on non-existing databases as well
    val updatedWithExtraRows = rows ++ otherTxIds.map(txId =>
      getResultMap(txId, null, "Transaction not found.")
    )

    val updatedColumnNameRows = updateRowsWithPotentiallyRenamedColumns(updatedWithExtraRows.toList)
    ClosingIterator.apply(updatedColumnNameRows.iterator)
  }

  private def getResultMap(txId: TransactionId, username: String, message: String): Map[String, AnyValue] =
    requestedColumnsNames.map {
      case `transactionIdColumn` => transactionIdColumn -> Values.stringValue(txId.toString)
      case `usernameColumn`      => usernameColumn -> Values.stringOrNoValue(username)
      case `messageColumn`       => messageColumn -> Values.stringValue(message)
      case unknown               =>
        // This match should cover all existing columns but we get scala warnings
        // on non-exhaustive match due to it being string values
        throw new IllegalStateException(s"Missing case for column: $unknown")
    }.toMap[String, AnyValue]
}
