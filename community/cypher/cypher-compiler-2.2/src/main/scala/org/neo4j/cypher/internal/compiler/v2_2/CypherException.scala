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
package org.neo4j.cypher.internal.compiler.v2_2

import org.neo4j.cypher.internal.compiler.v2_2.commands.SchemaIndex
import org.neo4j.cypher.internal.compiler.v2_2.spi.MapToPublicExceptions

abstract class CypherException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this() = this(null, null)

  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(null, cause)

  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T
}

class UniquePathNotUniqueException(message: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T = mapper.uniquePathNotUniqueException(message)
}

class FailedIndexException(indexName: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.failedIndexException(indexName)
}

class EntityNotFoundException(message: String, cause: Throwable = null) extends CypherException(cause) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.entityNotFoundException(message, cause)
}

class CypherTypeException(message: String, cause: Throwable = null) extends CypherException(cause) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.cypherTypeException(message, cause)
}

class ParameterNotFoundException(message: String, cause: Throwable) extends CypherException(cause) {
  def this(message: String) = this(message, null)

  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.parameterNotFoundException(message, cause)
}

class ParameterWrongTypeException(message: String, cause: Throwable) extends CypherException(cause) {
  def this(message: String) = this(message, null)

  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.parameterWrongTypeException(message, cause)
}

class InvalidArgumentException(message: String, cause: Throwable = null) extends CypherException(cause) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.invalidArgumentException(message, cause)
}

class PatternException(message: String) extends CypherException(message) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.patternException(message)
}

class InternalException(message: String, inner: Exception = null) extends CypherException(message, inner) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.internalException(message)
}

class NodeStillHasRelationshipsException(val nodeId: Long, cause: Throwable) extends CypherException(cause) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.nodeStillHasRelationshipsException(nodeId, cause)
}

class ProfilerStatisticsNotReadyException extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.profilerStatisticsNotReadyException()
}

class UnknownLabelException(labelName: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.unknownLabelException(labelName)
}

class HintException(message: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.hintException(message)
}

class IndexHintException(identifier: String, label: String, property: String, message: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.indexHintException(identifier, label, property, message)
}

class LabelScanHintException(identifier: String, label: String, message: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.labelScanHintException(identifier, label, message)
}

class InvalidSemanticsException(message: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.invalidSemanticException(message)
}

class MergeConstraintConflictException(message: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.mergeConstraintConflictException(message)
}

class ArithmeticException(message: String, cause: Throwable = null) extends CypherException(cause) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.arithmeticException(message, cause)
}

class IncomparableValuesException(lhs: String, rhs: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.incomparableValuesException(lhs, rhs)
}

class PeriodicCommitInOpenTransactionException extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.periodicCommitInOpenTransactionException()
}

class LoadExternalResourceException(message: String, cause: Throwable = null) extends CypherException(cause) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.loadExternalResourceException(message, cause)
}

class LoadCsvStatusWrapCypherException(extraInfo: String, cause: CypherException) extends CypherException(cause) {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.loadCsvStatusWrapCypherException(extraInfo, cause)
}

class SyntaxException(message: String, val query: String, val offset: Option[Int]) extends CypherException(message) {
  def this(message: String, query: String, offset: Int) = this(message, query, Some(offset))

  def this(message: String) = this(message, "", None)

  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.syntaxException(message, query, offset)
}

object IndexHintException {
  def apply(hint: SchemaIndex, message: String) =
    new IndexHintException(hint.identifier, hint.label, hint.property, message)
}
