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
package org.neo4j.cypher.internal.compiler.v2_3.spi

import org.neo4j.cypher.internal.compiler.v2_3.CypherException

trait MapToPublicExceptions[T <: Throwable] {
  def failedIndexException(indexName: String): T

  def periodicCommitInOpenTransactionException(): T

  def syntaxException(message: String, query: String, offset: Option[Int]): T

  def loadCsvStatusWrapCypherException(extraInfo: String, cause: CypherException): T

  def loadExternalResourceException(message: String, cause: Throwable): T

  def incomparableValuesException(lhs: String, rhs: String): T

  def arithmeticException(message: String, cause: Throwable): T

  def mergeConstraintConflictException(message: String): T

  def invalidSemanticException(message: String): T

  def indexHintException(identifier: String, label: String, property: String, message: String): T

  def labelScanHintException(identifier: String, label: String, message: String): T

  def unknownLabelException(s: String): T

  def profilerStatisticsNotReadyException(): T

  def nodeStillHasRelationshipsException(nodeId: Long, cause: Throwable): T

  def internalException(message: String): T

  def patternException(message: String): T

  def invalidArgumentException(message: String, cause: Throwable): T

  def parameterWrongTypeException(message: String, cause: Throwable): T

  def parameterNotFoundException(message: String, cause: Throwable): T

  def uniquePathNotUniqueException(message: String): T

  def entityNotFoundException(message: String, cause: Throwable): T

  def cypherTypeException(message: String, cause: Throwable): T
}
