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

import org.neo4j.kernel.api.exceptions.{KernelException, Status}

abstract class CypherException(message: String, cause: Throwable) extends RuntimeException(message, cause)
with Status.HasStatus {
  def status: Status
  def this(message: String) = this(message, null)
}

class CypherExecutionException(message: String, cause: KernelException) extends CypherException(message, cause) {
  def status = cause match {
    // These are always caused by KernelException's, so just map to the status code from the kernel exception.
    case e: KernelException if e != null => e.status()
    case _ => Status.Statement.ExecutionFailure
  }
}

class UniquePathNotUniqueException(message: String) extends CypherException(message) {
  val status = Status.Statement.ConstraintViolation
}

class EntityNotFoundException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.EntityNotFound
}

class CypherTypeException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.InvalidType
}

class ParameterNotFoundException(message: String, cause: Throwable) extends CypherException(message, cause) {
  def this(message: String) = this(message, null)

  val status = Status.Statement.ParameterMissing
}

class ParameterWrongTypeException(message: String, cause: Throwable) extends CypherException(message, cause) {
  def this(message: String) = this(message, null)

  val status = Status.Statement.InvalidType
}

class InvalidArgumentException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.InvalidArguments
}

class PatternException(message: String) extends CypherException(message, null) {
  val status = Status.Statement.InvalidSemantics
}

class InternalException(message: String, inner: Exception = null) extends CypherException(message, inner) {
  val status = Status.Statement.ExecutionFailure
}

class MissingIndexException(indexName: String) extends CypherException("Index `" + indexName + "` does not exist") {
  val status = Status.Schema.NoSuchIndex
}

class FailedIndexException(indexName: String) extends CypherException("Index `" + indexName + "` has failed. Drop and recreate it to get it back online.") {
  val status = Status.General.FailedIndex
}

class MissingConstraintException() extends CypherException("Constraint not found") {
  val status = Status.Schema.NoSuchConstraint
}

class NodeStillHasRelationshipsException(val nodeId: Long, cause: Throwable)
  extends CypherException("Node with id " + nodeId + " still has relationships, and cannot be deleted.", cause) {
  val status = Status.Schema.ConstraintViolation
}

class ProfilerStatisticsNotReadyException() extends CypherException("This result has not been materialised yet. Iterate over it to get profiler stats.") {
  val status = Status.Statement.ExecutionFailure
}

class UnknownLabelException(labelName: String) extends CypherException(s"The provided label :`$labelName` does not exist in the store") {
  val status = Status.Statement.NoSuchLabel
}

class HintException( message: String)
  extends CypherException(message) {
  val status = Status.Statement.ExecutionFailure
}

class IndexHintException(identifier: String, label: String, property: String, message: String)
  extends CypherException(s"$message\nLabel: `$label`\nProperty name: `$property`") {
  val status = Status.Schema.NoSuchIndex
}

class LabelScanHintException(identifier: String, label: String, message: String)
  extends CypherException(s"$message\nLabel: `$label`") {
  val status = Status.Statement.InvalidSemantics
}

class UnableToPickStartPointException(message: String) extends CypherException(message) {
  val status = Status.Statement.ExecutionFailure
}

class InvalidSemanticsException(message: String) extends CypherException(message) {
  val status = Status.Statement.InvalidSemantics
}

class OutOfBoundsException(message: String) extends CypherException(message) {
  val status = Status.Statement.InvalidArguments
}

class MergeConstraintConflictException(message: String) extends CypherException(message) {
  val status = Status.Statement.ConstraintViolation
}

class ArithmeticException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.ArithmeticError
}

class IncomparableValuesException(lhs: String, rhs: String)
  extends SyntaxException(s"Don't know how to compare that. Left: ${lhs}; Right: ${rhs}")

class PeriodicCommitInOpenTransactionException
  extends InvalidSemanticsException("Executing queries that use periodic commit in an open transaction is not possible.")

class LoadExternalResourceException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.ExternalResourceFailure
}

class LoadCsvStatusWrapCypherException(extraInfo: String, cause: CypherException) extends CypherException(s"${cause.getMessage} (${extraInfo})", cause) {
  val status = cause.status
}
