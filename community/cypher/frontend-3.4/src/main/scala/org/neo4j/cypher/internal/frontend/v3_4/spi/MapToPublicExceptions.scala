/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.spi

import org.neo4j.cypher.internal.frontend.v3_4.CypherException

trait MapToPublicExceptions[T <: Throwable] {
  def failedIndexException(indexName: String, cause: Throwable): T

  def periodicCommitInOpenTransactionException(cause: Throwable): T

  def syntaxException(message: String, query: String, offset: Option[Int], cause: Throwable): T

  def loadCsvStatusWrapCypherException(extraInfo: String, cause: CypherException): T

  def loadExternalResourceException(message: String, cause: Throwable): T

  def unorderableValueException(value: String): T

  def incomparableValuesException(operator: Option[String], lhs: String, rhs: String, cause: Throwable): T

  def arithmeticException(message: String, cause: Throwable): T

  def mergeConstraintConflictException(message: String, cause: Throwable): T

  def invalidSemanticException(message: String, cause: Throwable): T

  def indexHintException(variable: String, label: String, properties: Seq[String], message: String, cause: Throwable): T

  def hintException(message: String, cause: Throwable): T

  def joinHintException(variable: String, message: String, cause: Throwable): T

  def profilerStatisticsNotReadyException(cause: Throwable): T

  def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable): T

  def internalException(message: String, cause: Exception): T

  def patternException(message: String, cause: Throwable): T

  def invalidArgumentException(message: String, cause: Throwable): T

  def parameterWrongTypeException(message: String, cause: Throwable): T

  def parameterNotFoundException(message: String, cause: Throwable): T

  def uniquePathNotUniqueException(message: String, cause: Throwable): T

  def entityNotFoundException(message: String, cause: Throwable): T

  def cypherTypeException(message: String, cause: Throwable): T

  def cypherExecutionException(message: String, cause: Throwable): T

  def shortestPathFallbackDisableRuntimeException(message: String, cause: Throwable): T

  def shortestPathCommonEndNodesForbiddenException(message: String, cause: Throwable): T

}
