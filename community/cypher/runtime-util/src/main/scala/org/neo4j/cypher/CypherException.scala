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

import org.neo4j.internal.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.exceptions.Status

abstract class CypherException(message: String, cause: Throwable) extends RuntimeException(message, cause)
with Status.HasStatus {
  def status: Status
}

class CypherExecutionException(message: String, cause: Throwable) extends CypherException(message, cause) {
  def status = cause match {
    // These are always caused by KernelException's, so just map to the status code from the kernel exception.
    case e: KernelException if e != null => e.status()
    case _ => Status.Statement.ExecutionFailed
  }
}

class UniquePathNotUniqueException(message: String, cause:Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.ConstraintVerificationFailed
}

class EntityNotFoundException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.EntityNotFound
}

class CypherTypeException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.TypeError
}

class ParameterNotFoundException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.ParameterMissing
}

class ParameterWrongTypeException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.TypeError
}

class InvalidArgumentException(message: String, cause: Throwable = null) extends CypherException(message, cause) {
  val status = Status.Statement.ArgumentError
}

class PatternException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.SemanticError
  def this(message: String) = this(message,null)
}

class InternalException(message: String, inner: Exception = null) extends CypherException(message, inner) {
  val status = Status.Statement.ExecutionFailed
}

class MissingIndexException(indexName: String) extends CypherException("Index `" + indexName + "` does not exist", null) {
  val status = Status.Schema.IndexNotFound
}

class FailedIndexException(indexName: String, cause: Throwable) extends CypherException("Index `" + indexName + "` has failed. Drop and recreate it to get it back online.", cause) {
  val status = Status.General.IndexCorruptionDetected
  def this(indexName: String) = this(indexName, null)
}

class MissingConstraintException(cause: Throwable) extends CypherException("Constraint not found", cause) {
  val status = Status.Schema.ConstraintNotFound
}

class NodeStillHasRelationshipsException(val nodeId: Long, cause: Throwable)
  extends CypherException("Node with id " + nodeId + " still has relationships, and cannot be deleted.", cause) {
    val status = Status.Schema.ConstraintValidationFailed
  }

class ProfilerStatisticsNotReadyException(cause: Throwable) extends CypherException("This result has not been materialised yet. Iterate over it to get profiler stats.", cause) {
  val status = Status.Statement.ExecutionFailed
  def this() = this(null)
}

class UnknownLabelException(labelName: String, cause: Throwable) extends CypherException(s"The provided label :`$labelName` does not exist in the store", cause) {
  val status = Status.Statement.LabelNotFound
  def this(labelName: String) = this(labelName, null)
}

class HintException(message: String, cause: Throwable)
  extends CypherException(message, cause) {
  val status = Status.Statement.ExecutionFailed
}

class IndexHintException(variable: String, label: String, properties: Seq[String], message: String, cause: Throwable)
  extends CypherException(s"$message\nLabel: `$label`\nProperty name: ${properties.map(p => s"'$p'").mkString(", ")}", cause) {
  val status = Status.Schema.IndexNotFound
}

class JoinHintException(variable: String, message: String, cause: Throwable)
  extends CypherException(message, cause) {
  val status = Status.Statement.ExecutionFailed
}

class LabelScanHintException(variable: String, label: String, message: String, cause: Throwable)
  extends CypherException(s"$message\nLabel: `$label`", cause) {
  val status = Status.Statement.SemanticError
  def this(variable: String, label: String, message: String) = this(variable, label, message, null)
}

class InvalidSemanticsException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.SemanticError
  def this(message: String) = this(message,null)
}

class MergeConstraintConflictException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Schema.ConstraintValidationFailed
  def this(message: String) = this(message, null)
}

class ConstraintValidationException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.ConstraintVerificationFailed
  def this(message: String) = this(message, null)
}

class ArithmeticException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.ArithmeticError
}

class IncomparableValuesException(details: Option[String], lhs: String, rhs: String, cause: Throwable)
  extends CypherTypeException(s"${details.getOrElse("Don't know how to compare that.")} Left: $lhs; Right: $rhs", cause) {
  def this(lhs: String, rhs: String, cause: Throwable) = this(None, lhs, rhs, null)
  def this(lhs: String, rhs: String) = this(None, lhs, rhs, null)
}

class UnorderableValueException(value: String, cause: Throwable)
  extends CypherTypeException(s"Do not know how to order $value", cause) {
  def this(value: String) = this(value, null)
}

class PeriodicCommitInOpenTransactionException(cause: Throwable)
  extends InvalidSemanticsException("Executing stream that use periodic commit in an open transaction is not possible.", cause) {
  def this() = this(null)
}

class LoadExternalResourceException(message: String, cause: Throwable) extends CypherException(message, cause) {
  val status = Status.Statement.ExternalResourceFailed
}

class LoadCsvStatusWrapCypherException(extraInfo: String, cause: CypherException) extends CypherException(s"${cause.getMessage} (${extraInfo})", cause) {
  val status = cause.status
}

class ExhaustiveShortestPathForbiddenException(message: String, cause: Throwable) extends CypherExecutionException(message, cause)

class ShortestPathCommonEndNodesForbiddenException(message: String, cause: Throwable) extends CypherExecutionException(message, cause)
