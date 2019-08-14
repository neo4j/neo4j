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
package org.neo4j.exceptions

import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.api.exceptions.Status.HasStatus

import scala.compat.Platform.EOL

abstract class Neo4jException(message: String, cause: Throwable) extends RuntimeException(message, cause)
                                                                 with Status.HasStatus {
  def status: Status
}

class CypherExecutionException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  def status: Status = cause match {
    case e: HasStatus => e.status()
    case _ => Status.Statement.ExecutionFailed
  }
}

class CantCompileQueryException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.ExecutionFailed
}

class EntityNotFoundException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.EntityNotFound
}

class CypherTypeException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.TypeError
}

class ParameterNotFoundException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.ParameterMissing
}

class ParameterWrongTypeException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.TypeError
}

class InvalidArgumentException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.ArgumentError
}

class RuntimeUnsupportedException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.RuntimeUnsupportedError
}

class PatternException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.SemanticError
}

class InternalException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.ExecutionFailed
}

object FailedIndexException {
  def msg(indexName: String, failureMessage: String): String =
    s"Index `$indexName` has failed. Drop and recreate it to get it back online." +
    (if (failureMessage != null) s" Actual failure:$EOL==================$EOL$failureMessage$EOL==================" else "")
}

class FailedIndexException(indexName: String, failureMessage: String) extends Neo4jException(FailedIndexException.msg(indexName, failureMessage), null) {
  override val status: Status = Status.General.IndexCorruptionDetected
}

object ProfilerStatisticsNotReadyException {
  val ERROR_MSG: String = "This result has not been materialised yet. Iterate over it to get profiler stats."
}

class ProfilerStatisticsNotReadyException(cause: Throwable = null) extends Neo4jException(ProfilerStatisticsNotReadyException.ERROR_MSG, cause) {
  override val status: Status = Status.Statement.ExecutionFailed
}

class HintException(message: String)
  extends Neo4jException(message, null) {
  override val status: Status = Status.Statement.ExecutionFailed
}

object IndexHintException {
  def msg(variable: String, label: String, properties: Seq[String], message: String): String =
    s"$message\nLabel: `$label`\nProperty name: ${properties.map(p => s"'$p'").mkString(", ")}"
}

class IndexHintException(variable: String, label: String, properties: Seq[String], message: String)
  extends Neo4jException(IndexHintException.msg(variable, label, properties, message), null) {
  override val status: Status = Status.Schema.IndexNotFound
}

class JoinHintException(variable: String, message: String)
  extends Neo4jException(message, null) {
  override val status: Status = Status.Statement.ExecutionFailed
}

class InvalidSemanticsException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.SemanticError
}

class MergeConstraintConflictException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Schema.ConstraintValidationFailed
}

class ConstraintValidationException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.ConstraintVerificationFailed
}

class ArithmeticException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.ArithmeticError
}

object IncomparableValuesException {
  def msg(details: Option[String], lhs: String, rhs: String): String =
    s"${details.getOrElse("Don't know how to compare that.")} Left: $lhs; Right: $rhs"
}

class IncomparableValuesException(details: Option[String], lhs: String, rhs: String, cause: Throwable)
  extends CypherTypeException(IncomparableValuesException.msg(details, lhs, rhs), cause) {
  def this(lhs: String, rhs: String, cause: Throwable) = this(None, lhs, rhs, cause)
  def this(lhs: String, rhs: String) = this(None, lhs, rhs, null)
  def this(operator: String, lhs: String, rhs: String) = this(Some(operator), lhs, rhs, null)
}

class UnorderableValueException(value: String, cause: Throwable = null)
  extends CypherTypeException(s"Do not know how to order $value", cause) {
}

object PeriodicCommitInOpenTransactionException {
  val ERROR_MSG: String = "Executing stream that use periodic commit in an open transaction is not possible."
}

class PeriodicCommitInOpenTransactionException(cause: Throwable = null)
  extends InvalidSemanticsException(PeriodicCommitInOpenTransactionException.ERROR_MSG, cause) {
}

class LoadExternalResourceException(message: String, cause: Throwable = null) extends Neo4jException(message, cause) {
  override val status: Status = Status.Statement.ExternalResourceFailed
}

object LoadCsvStatusWrapCypherException {
  def message(extraInfo: String, message: String) = s"$message ($extraInfo)"
}

class LoadCsvStatusWrapCypherException(extraInfo: String, cause: Neo4jException)
  extends Neo4jException(LoadCsvStatusWrapCypherException.message(extraInfo, cause.getMessage), cause) {
  override val status: Status = cause.status
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

class ExhaustiveShortestPathForbiddenException extends CypherExecutionException(ExhaustiveShortestPathForbiddenException.ERROR_MSG, null)

object ShortestPathCommonEndNodesForbiddenException {
  val ERROR_MSG: String =
    s"""The shortest path algorithm does not work when the start and end nodes are the same. This can happen if you
       |perform a shortestPath search after a cartesian product that might have the same start and end nodes for some
       |of the rows passed to shortestPath. If you would rather not experience this exception, and can accept the
       |possibility of missing results for those rows, disable this in the Neo4j configuration by setting
       |`cypher.forbid_shortestpath_common_nodes` to false. If you cannot accept missing results, and really want the
       |shortestPath between two common nodes, then re-write the query using a standard Cypher variable length pattern
       |expression followed by ordering by path length and limiting to one result.""".stripMargin
}

class ShortestPathCommonEndNodesForbiddenException extends CypherExecutionException(ShortestPathCommonEndNodesForbiddenException.ERROR_MSG, null)

class DatabaseAdministrationException(message: String) extends CypherExecutionException(message, null) {
  override val status: Status = Status.Statement.NotSystemDatabaseError
}

class SecurityAdministrationException(message: String) extends CypherExecutionException(message, null)

object TransactionOutOfMemoryException {
  val ERROR_MSG: String = "The transaction used more memory than was allowed."
}

class TransactionOutOfMemoryException extends CypherExecutionException(TransactionOutOfMemoryException.ERROR_MSG, null) {
  override val status: Status = Status.General.TransactionOutOfMemoryError
}
