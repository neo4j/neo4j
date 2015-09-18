/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility

import org.neo4j.cypher.internal.compiler.v2_2.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.compiler.v2_2.{CypherException => InternalCypherException}
import org.neo4j.cypher.{ArithmeticException, CypherException => ExternalCypherException, CypherTypeException, EntityNotFoundException, FailedIndexException, HintException, IncomparableValuesException, IndexHintException, InternalException, InvalidArgumentException, InvalidSemanticsException, LabelScanHintException, LoadCsvStatusWrapCypherException, LoadExternalResourceException, MergeConstraintConflictException, NodeStillHasRelationshipsException, ParameterNotFoundException, ParameterWrongTypeException, PatternException, PeriodicCommitInOpenTransactionException, ProfilerStatisticsNotReadyException, SyntaxException, UniquePathNotUniqueException, UnknownLabelException, _}
import org.neo4j.graphdb.Transaction

object exceptionHandlerFor2_2 extends MapToPublicExceptions[ExternalCypherException] {
  def syntaxException(message: String, query: String, offset: Option[Int]) = new SyntaxException(message, query, offset)

  def arithmeticException(message: String, cause: Throwable) = new ArithmeticException(message, cause)

  def profilerStatisticsNotReadyException() = new ProfilerStatisticsNotReadyException()

  def incomparableValuesException(lhs: String, rhs: String) = new IncomparableValuesException(lhs, rhs)

  def unknownLabelException(s: String) = new UnknownLabelException(s)

  def patternException(message: String) = new PatternException(message)

  def invalidArgumentException(message: String, cause: Throwable) = new InvalidArgumentException(message, cause)

  def mergeConstraintConflictException(message: String) = new MergeConstraintConflictException(message)

  def internalException(message: String) = new InternalException(message)

  def missingConstraintException() = new MissingConstraintException

  def loadCsvStatusWrapCypherException(extraInfo: String, cause: InternalCypherException) =
    new LoadCsvStatusWrapCypherException(extraInfo, cause.mapToPublic(exceptionHandlerFor2_2))

  def loadExternalResourceException(message: String, cause: Throwable) = new LoadExternalResourceException(message, cause)

  def parameterNotFoundException(message: String, cause: Throwable) = new ParameterNotFoundException(message, cause)

  def uniquePathNotUniqueException(message: String) = new UniquePathNotUniqueException(message)

  def entityNotFoundException(message: String, cause: Throwable) = new EntityNotFoundException(message, cause)

  def cypherTypeException(message: String, cause: Throwable) = new CypherTypeException(message, cause)

  def hintException(message: String): ExternalCypherException = new HintException(message)

  def labelScanHintException(identifier: String, label: String, message: String) = new LabelScanHintException(identifier, label, message)

  def invalidSemanticException(message: String) = new InvalidSemanticsException(message)

  def parameterWrongTypeException(message: String, cause: Throwable) = new ParameterWrongTypeException(message, cause)

  def outOfBoundsException(message: String) = new OutOfBoundsException(message)

  def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable) = new NodeStillHasRelationshipsException(nodeId, cause)

  def indexHintException(identifier: String, label: String, property: String, message: String) = new IndexHintException(identifier, label, property, message)

  def periodicCommitInOpenTransactionException() = new PeriodicCommitInOpenTransactionException

  def failedIndexException(indexName: String) = new FailedIndexException(indexName)

  def runSafely[T](body: => T)(implicit errorHandler: Throwable => Unit = (_) => ()) = {
    try {
      body
    }
    catch {
      case e: InternalCypherException =>
        errorHandler(e)
        throw e.mapToPublic(exceptionHandlerFor2_2)
      case e: Throwable =>
        errorHandler(e)
        throw e
    }
  }

  /**
   * Marks given transaction as successful or failed depending on the exception type and transaction type.
   *
   * If transaction is [[org.neo4j.kernel.TopLevelTransaction]] than it is marked as failed right away because it is
   * either a manually started transaction (via [[ExecutionEngine]],
   * [[org.neo4j.graphdb.GraphDatabaseService#beginTx()]], etc.) or a periodic commit transaction.
   *
   * If transaction is [[org.neo4j.kernel.PlaceboTransaction]] that it is marked as failed only if given exception
   * requires rollback.
   *
   * @see [[org.neo4j.kernel.api.exceptions.Status.Classification]]
   *
   * @param error failure that occurred during transaction execution
   * @param isTopLevelTx marker if given transaction is either [[org.neo4j.kernel.TopLevelTransaction]]
   *                     or [[org.neo4j.kernel.PlaceboTransaction]]
   * @param tx current transaction that had execution failure
   */
  def handle(error: Throwable, isTopLevelTx: Boolean, tx: Transaction) = {
    if (isTopLevelTx)
      tx.failure()
    else
      error match {
        case e: InternalCypherException if shouldNotRollback(e) =>
          tx.success()
        case e: ExternalCypherException if shouldNotRollback(e) =>
          tx.success()
        case _ =>
          tx.failure()
      }
  }

  private def shouldNotRollback(e: InternalCypherException): Boolean = shouldNotRollback(e.mapToPublic(this))

  private def shouldNotRollback(e: ExternalCypherException): Boolean = !e.status.code().classification().rollbackTransaction()
}

