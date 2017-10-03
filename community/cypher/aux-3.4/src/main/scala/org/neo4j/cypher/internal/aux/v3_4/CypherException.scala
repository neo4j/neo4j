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
package org.neo4j.cypher.internal.aux.v3_4

import org.neo4j.cypher.internal.aux.v3_4.spi.MapToPublicExceptions

abstract class CypherException(protected val message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this() = this(null, null)

  def this(message: String) = this(message, null)

  def this(cause: Throwable) = this(null, cause)

  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T
}

class UniquePathNotUniqueException(message: String) extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T = mapper.uniquePathNotUniqueException(message, this)
}

class FailedIndexException(indexName: String) extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.failedIndexException(indexName, this)
}

class EntityNotFoundException(message: String, cause: Throwable = null) extends CypherException(cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.entityNotFoundException(message, this)
}

class CypherTypeException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.cypherTypeException(message, this)
}

class ParameterNotFoundException(message: String, cause: Throwable) extends CypherException(cause) {
  def this(message: String) = this(message, null)

  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.parameterNotFoundException(message, this)
}

class ParameterWrongTypeException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.parameterWrongTypeException(message, this)
}

class InvalidArgumentException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.invalidArgumentException(message, this)
}

class PatternException(message: String) extends CypherException(message) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.patternException(message, this)
}

class InternalException(message: String, inner: Exception = null) extends CypherException(message, inner) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.internalException(message, this)
}

class NodeStillHasRelationshipsException(val nodeId: Long, cause: Throwable) extends CypherException(cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.nodeStillHasRelationshipsException(nodeId, this)
}

class ProfilerStatisticsNotReadyException extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.profilerStatisticsNotReadyException(this)
}

class IndexHintException(variable: String, label: String, properties: Seq[String], message: String) extends CypherException {
  def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.indexHintException(variable, label, properties, message, this)
}

class JoinHintException(variable: String, message: String) extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.joinHintException(variable, message, this)
}

class HintException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T = mapper.hintException(message, cause)
}

class InvalidSemanticsException(message: String) extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.invalidSemanticException(message, this)
}

class MergeConstraintConflictException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.mergeConstraintConflictException(message, this)
}

class ArithmeticException(message: String, cause: Throwable = null) extends CypherException(cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.arithmeticException(message, this)
}

class IncomparableValuesException(details: Option[String], lhs: String, rhs: String) extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.incomparableValuesException(details, lhs, rhs, this)
  def this(operator: String, lhs: String, rhs: String) = this(Some(operator), lhs, rhs)
  def this(lhs: String, rhs: String) = this(None, lhs, rhs)
}

class UnorderableValueException(value: String) extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T =
    mapper.unorderableValueException(value)
}

class PeriodicCommitInOpenTransactionException extends CypherException {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.periodicCommitInOpenTransactionException(this)
}

class LoadExternalResourceException(message: String, cause: Throwable = null) extends CypherException(cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) = mapper.loadExternalResourceException(message, this)
}

class LoadCsvStatusWrapCypherException(extraInfo: String, cause: CypherException) extends CypherException(cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) =
    // the mapper will map the cause here, so we cannot pass 'this' in as the cause...
    mapper.loadCsvStatusWrapCypherException(extraInfo, cause)
}

class SyntaxException(message: String, val query: String, val pos: Option[InputPosition]) extends CypherException(message) {
  def this(message: String, query: String, offset: InputPosition) = this(message, query, Some(offset))

  def this(message: String) = this(message, "", None)

  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]) =
    mapper.syntaxException(message, query, pos.map(_.offset), this)
}

class CypherExecutionException(message: String, cause: Throwable) extends CypherException(message, cause) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T = mapper.cypherExecutionException(message, this)
}

object ExhaustiveShortestPathForbiddenException {
  val ERROR_MSG: String =
    s"""Shortest path fallback has been explicitly disabled. That means that no full path enumeration is performed in
       |case shortest path algorithms cannot be used. This might happen in case of existential predicates on the path,
       |e.g., when searching for the shortest path containing a node with property 'name=Emil'. The problem is that
       |graph algorithms work only on universal predicates, e.g., when searching for the shortest where all nodes have
       |label 'Person'. In case this is an unexpected error please either disable the runtime error in the Neo4j
       |configuration or please improve your query by consulting the Neo4j manual.  In order to avoid planning the
       |shortest path fallback a WITH clause can be introduced to separate the MATCH describing the shortest paths and
       |the existential predicates on the path; note though that in this case all shortest paths are computed before
       |start filtering.""".stripMargin
}

class ExhaustiveShortestPathForbiddenException extends CypherExecutionException(
  ExhaustiveShortestPathForbiddenException.ERROR_MSG, null) {
override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T =
mapper.shortestPathFallbackDisableRuntimeException(message, this)
}

object ShortestPathCommonEndNodesForbiddenException {
  val ERROR_MSG: String =
    s"""The shortest path algorithm does not work when the start and end nodes are the same. This can happen if you
       |perform a shortestPath search after a cartesian product that might have the same start and end nodes for some
       |of the rows passed to shortestPath. If you would rather not experience this exception, and can accept the
       |possibility of missing results for those rows, disable this in the Neo4j configuration by setting
       |`cypher.forbid_shortestpath_common_node` to false. If you cannot accept missing results, and really want the
       |shortestPath between two common nodes, then re-write the query using a standard Cypher variable length pattern
       |expression followed by ordering by path length and limiting to one result.""".stripMargin
}

class ShortestPathCommonEndNodesForbiddenException extends CypherExecutionException(
  ShortestPathCommonEndNodesForbiddenException.ERROR_MSG, null) {
  override def mapToPublic[T <: Throwable](mapper: MapToPublicExceptions[T]): T =
    mapper.shortestPathCommonEndNodesForbiddenException(message, this)
}
