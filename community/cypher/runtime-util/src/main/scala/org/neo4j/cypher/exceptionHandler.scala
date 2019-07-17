/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.v4_0._
import org.neo4j.cypher.internal.v4_0.util.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.v4_0.util.{CypherException => InternalCypherException}
import org.neo4j.values.utils._

object exceptionHandler extends MapToPublicExceptions[CypherException] {
  override def syntaxException(message: String, query: String, offset: Option[Int], cause: Throwable) = new SyntaxException(message, query, offset, cause)

  override def arithmeticException(message: String, cause: Throwable) = new ArithmeticException(message, cause)

  override def profilerStatisticsNotReadyException(cause: Throwable) = new ProfilerStatisticsNotReadyException(cause)

  override def incomparableValuesException(details: Option[String], lhs: String, rhs: String, cause: Throwable) = new IncomparableValuesException(details, lhs, rhs, cause)

  override def patternException(message: String, cause: Throwable) = new PatternException(message, cause)

  override def unorderableValueException(value: String) = new UnorderableValueException(value)

  override def invalidArgumentException(message: String, cause: Throwable) = new InvalidArgumentException(message, cause)

  override def mergeConstraintConflictException(message: String, cause: Throwable) = new MergeConstraintConflictException(message, cause)

  override def internalException(message: String, cause: Exception) = new InternalException(message, cause)

  override def loadCsvStatusWrapCypherException(extraInfo: String, cause: InternalCypherException) = cause match {
    case e: util.ArithmeticException => exceptionHandler.arithmeticException(LoadCsvStatusWrapCypherException.message(extraInfo, e.getMessage), e.getCause)
    case e: util.InvalidSemanticsException => exceptionHandler.invalidSemanticException(LoadCsvStatusWrapCypherException.message(extraInfo, e.getMessage), e.getCause)
    case e: util.CypherTypeException => exceptionHandler.cypherTypeException(LoadCsvStatusWrapCypherException.message(extraInfo, e.getMessage), e.getCause)
    case _ => new LoadCsvStatusWrapCypherException(extraInfo, cause.mapToPublic(exceptionHandler))
  }

  override def loadExternalResourceException(message: String, cause: Throwable) = new LoadExternalResourceException(message, cause)

  override def parameterNotFoundException(message: String, cause: Throwable) = new ParameterNotFoundException(message, cause)

  override def uniquePathNotUniqueException(message: String, cause: Throwable) = new UniquePathNotUniqueException(message, cause)

  override def entityNotFoundException(message: String, cause: Throwable) = new EntityNotFoundException(message, cause)

  override def cypherTypeException(message: String, cause: Throwable) = new CypherTypeException(message, cause)

  override def cypherExecutionException(message: String, cause: Throwable) = new CypherExecutionException(message, cause)

  override def shortestPathFallbackDisableRuntimeException(message: String, cause: Throwable): CypherException =
    new ExhaustiveShortestPathForbiddenException(message, cause)

  override def shortestPathCommonEndNodesForbiddenException(message: String, cause: Throwable): CypherException =
    new ShortestPathCommonEndNodesForbiddenException(message, cause)

  override def transactionOutOfMemoryException(message: String, cause: Throwable): CypherException =
    new TransactionOutOfMemoryException(message, cause)

  override def databaseManagementException(message: String): CypherException =
    new DatabaseManagementException(message)

  override def securityManagementException(message: String): CypherException =
    new SecurityManagementException(message)

  override def invalidSemanticException(message: String, cause: Throwable) = new InvalidSemanticsException(message, cause)

  override def parameterWrongTypeException(message: String, cause: Throwable) = new ParameterWrongTypeException(message, cause)

  override def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable) = new NodeStillHasRelationshipsException(nodeId, cause)

  def indexHintException(variable: String, label: String, properties: Seq[String], message: String, cause: Throwable) =
    new IndexHintException(variable, label, properties, message, cause)

  override def joinHintException(variable: String, message: String, cause: Throwable) = new JoinHintException(variable, message, cause)

  override def hintException(message: String, cause: Throwable): CypherException = new HintException(message, cause)

  override def periodicCommitInOpenTransactionException(cause: Throwable) = new PeriodicCommitInOpenTransactionException(cause)

  override def failedIndexException(indexName: String, failureMessage: String, cause: Throwable): CypherException = new FailedIndexException(indexName, failureMessage, cause)

  object runSafely extends RunSafely {
    override def apply[T](body: => T)(implicit onError: Throwable => T = e => throw e): T = {
      try {
        body
      }
      catch {
        case e: Throwable => onError(mapToCypher(e))
      }
    }
  }

  trait RunSafely {
    def apply[T](body: => T)(implicit onError: Throwable => T = (e :Throwable) => throw e): T
  }

  def mapToCypher(exception: Throwable): Throwable = exception match {
    case e: InternalCypherException => e.mapToPublic(exceptionHandler)
    case e: ValuesException => mapToCypher(e)
    // ValueMath do not wrap java.lang.ArithmeticExceptions, so we map it to public here
    // (This will also catch if we happened to produce arithmetic exceptions internally (as a runtime bug and not as the result of the query),
    //  which is not optimal but hopefully rare)
    case e: java.lang.ArithmeticException => exceptionHandler.arithmeticException(e.getMessage, e)
    case throwable  => throwable
  }

  def mapToCypher(exception: ValuesException): CypherException = {
    exception match {
      case e: UnsupportedTemporalUnitException =>
        exceptionHandler.cypherTypeException(e.getMessage, e)
      case e: InvalidValuesArgumentException =>
        exceptionHandler.invalidArgumentException(e.getMessage, e)
      case e: TemporalArithmeticException =>
        exceptionHandler.arithmeticException(e.getMessage, e)
      case e: TemporalParseException =>
        if (e.getParsedData == null) {
          exceptionHandler.syntaxException(e.getMessage, "", None, e)
        }
        else {
          exceptionHandler.syntaxException(e.getMessage, e.getParsedData, Option(e.getErrorIndex), e)
        }
    }
  }
}
