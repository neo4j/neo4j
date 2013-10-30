/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.api.exceptions.KernelException

abstract class CypherException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message: String) = this(message, null)
}

class CypherExecutionException(message: String, cause: KernelException) extends CypherException(message, cause)

class UniquePathNotUniqueException(message: String) extends CypherException(message)

class EntityNotFoundException(message: String, cause: Throwable = null) extends CypherException(message, cause)

class CypherTypeException(message: String, cause: Throwable = null) extends CypherException(message, cause)

class ParameterNotFoundException(message: String, cause: Throwable) extends CypherException(message, cause) {
  def this(message: String) = this(message, null)
}

class ParameterWrongTypeException(message: String, cause: Throwable) extends CypherException(message, cause) {
  def this(message: String) = this(message, null)
}

class PatternException(message: String) extends CypherException(message, null)

class InternalException(message: String, inner: Exception = null) extends CypherException(message, inner)

class MissingIndexException(indexName: String) extends CypherException("Index `" + indexName + "` does not exist")

class FailedIndexException(indexName: String) extends CypherException("Index `" + indexName + "` has failed. Drop and recreate it to get it back online.")

class MissingConstraintException() extends CypherException("Constraint not found")

class NodeStillHasRelationshipsException(val nodeId: Long, cause: Throwable)
  extends CypherException("Node with id " + nodeId + " still has relationships, and cannot be deleted.")

class ProfilerStatisticsNotReadyException() extends CypherException("This result has not been materialised yet. Iterate over it to get profiler stats.")

class UnknownLabelException(labelName: String) extends CypherException(s"The provided label :`$labelName` does not exist in the store")

class IndexHintException(identifier: String, label: String, property: String, message: String)
  extends CypherException(s"$message\nLabel: `$label`\nProperty name: `$property`")

class LabelScanHintException(identifier: String, label: String, message: String)
  extends CypherException(s"$message\nLabel: `$label`")

class UnableToPickStartPointException(message: String) extends CypherException(message)

class InvalidSemanticsException(message: String) extends CypherException(message)

class OutOfBoundsException(message: String) extends CypherException(message)

class MergeConstraintConflictException(message: String) extends CypherException(message)

class ArithmeticException(message: String) extends CypherException(message)
