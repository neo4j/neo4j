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

import internal.commands.expressions.Expression

abstract class CypherException(message: String, cause: Throwable) extends RuntimeException(message, cause) {
  def this(message:String) = this(message, null)
}

class UniquePathNotUniqueException(message:String) extends CypherException(message)

class EntityNotFoundException(message:String, cause:Throwable=null) extends CypherException(message, cause)

class CypherTypeException(message: String, cause: Throwable = null) extends CypherException(message, cause)

class IterableRequiredException(message:String, cause:Throwable) extends CypherException(message, cause) {
  def this(message:String) = this(message, null)
  def this(expression:Expression) = this("Expected " + expression + " to be an collection, but it is not.", null)
}

class ParameterNotFoundException(message:String, cause:Throwable) extends CypherException(message, cause) {
def this(message:String)=this(message,null)
}

class ParameterWrongTypeException(message:String, cause:Throwable) extends CypherException(message, cause) {
  def this(message:String)=this(message,null)
}

class PatternException(message:String) extends CypherException(message, null)

class InternalException(message:String, inner:Exception=null) extends CypherException(message, inner)

class MissingIndexException(indexName:String) extends CypherException("Index `" + indexName + "` does not exist")

class InvalidAggregateException(message:String) extends CypherException(message)

class NodeStillHasRelationshipsException(val nodeId:Long, cause:Throwable)
  extends CypherException("Node with id " + nodeId + " still has relationships, and can not be deleted.")

class IndexAlreadyDefinedException(labelName:String, property:String, cause:Throwable)
  extends CypherException(s"Property `$property` is already indexed for label `$labelName`.", cause)

class CouldNotDropIndexException(labelName:String, property:String, cause:Throwable)
  extends CypherException(String.format(s"Unable to drop index on label `$labelName` for property `$property`."), cause)

class ProfilerStatisticsNotReadyException() extends CypherException("This result has not been materialised yet. Iterate over it to get profiler stats.")

class UnknownLabelException(labelName: String) extends CypherException(s"The provided label :`${labelName}` does not exist in the store")