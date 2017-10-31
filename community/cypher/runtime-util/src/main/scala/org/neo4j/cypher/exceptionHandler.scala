/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.util.v3_4.spi.MapToPublicExceptions
import org.neo4j.cypher.internal.util.v3_4.{CypherException => InternalCypherException}

object exceptionHandler extends MapToPublicExceptions[CypherException] {
  override def syntaxException(message: String, query: String, offset: Option[Int], cause: Throwable) = new SyntaxException(message, query, offset, cause)

  override def arithmeticException(message: String, cause: Throwable) = new ArithmeticException(message, cause)

  override def profilerStatisticsNotReadyException(cause: Throwable) = {
    throw new ProfilerStatisticsNotReadyException(cause)
  }

  override def incomparableValuesException(details: Option[String], lhs: String, rhs: String, cause: Throwable) = new IncomparableValuesException(details, lhs, rhs, cause)

  override def patternException(message: String, cause: Throwable) = new PatternException(message, cause)

  override def unorderableValueException(value: String) = new UnorderableValueException(value)

  override def invalidArgumentException(message: String, cause: Throwable) = new InvalidArgumentException(message, cause)

  override def mergeConstraintConflictException(message: String, cause: Throwable) = new MergeConstraintConflictException(message, cause)

  override def internalException(message: String, cause: Exception) = new InternalException(message, cause)

  override def loadCsvStatusWrapCypherException(extraInfo: String, cause: InternalCypherException) =
    new LoadCsvStatusWrapCypherException(extraInfo, cause.mapToPublic(exceptionHandler))

  override def loadExternalResourceException(message: String, cause: Throwable) = throw new LoadExternalResourceException(message, cause)

  override def parameterNotFoundException(message: String, cause: Throwable) = throw new ParameterNotFoundException(message, cause)

  override def uniquePathNotUniqueException(message: String, cause: Throwable) = throw new UniquePathNotUniqueException(message, cause)

  override def entityNotFoundException(message: String, cause: Throwable) = throw new EntityNotFoundException(message, cause)


  override def cypherTypeException(message: String, cause: Throwable) = throw new CypherTypeException(message, cause)

  override def cypherExecutionException(message: String, cause: Throwable) = throw new CypherExecutionException(message, cause)

  override def shortestPathFallbackDisableRuntimeException(message: String, cause: Throwable): CypherException =
    throw new ExhaustiveShortestPathForbiddenException(message, cause)

  override def shortestPathCommonEndNodesForbiddenException(message: String, cause: Throwable): CypherException =
    throw new ShortestPathCommonEndNodesForbiddenException(message, cause)

  override def invalidSemanticException(message: String, cause: Throwable) = throw new InvalidSemanticsException(message, cause)

  override def parameterWrongTypeException(message: String, cause: Throwable) = throw new ParameterWrongTypeException(message, cause)

  override def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable) = throw new NodeStillHasRelationshipsException(nodeId, cause)

  def indexHintException(variable: String, label: String, properties: Seq[String], message: String, cause: Throwable) =
    throw new IndexHintException(variable, label, properties, message, cause)

  override def joinHintException(variable: String, message: String, cause: Throwable) = throw new JoinHintException(variable, message, cause)

  override def hintException(message: String, cause: Throwable): CypherException = throw new HintException(message, cause)

  override def periodicCommitInOpenTransactionException(cause: Throwable) = throw new PeriodicCommitInOpenTransactionException(cause)

  override def failedIndexException(indexName: String, cause: Throwable): CypherException = throw new FailedIndexException(indexName, cause)

  object runSafely extends RunSafely {
    override def apply[T](body: => T)(implicit f: ExceptionHandler = ExceptionHandler.default) = {
      try {
        body
      }
      catch {
        case e: InternalCypherException =>
          f(e)
          throw e.mapToPublic(exceptionHandler)
        case e: Throwable =>
          f(e)
          throw e
      }
    }
  }

  trait RunSafely {
    def apply[T](body: => T)(implicit f: ExceptionHandler = ExceptionHandler.default): T
  }
}
