/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.spi.v2_2

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.compiler.v2_2.spi._
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException
import org.neo4j.kernel.api.index.{IndexDescriptor, InternalIndexState}

class TransactionBoundPlanContext(someStatement: Statement, val gdb: GraphDatabaseService)
  extends TransactionBoundTokenContext(someStatement) with PlanContext {

  @Deprecated
  def getIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] = evalOrNone {
    val labelId = statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = statement.readOperations().propertyKeyGetForName(propertyKey)

    getOnlineIndex(statement.readOperations().indexesGetForLabelAndPropertyKey(labelId, propertyKeyId))
  }

  def getUniqueIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] = evalOrNone {
    val labelId = statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = statement.readOperations().propertyKeyGetForName(propertyKey)

    Some(statement.readOperations().uniqueIndexGetForLabelAndPropertyKey(labelId, propertyKeyId))
  }

  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try { f } catch { case _: SchemaKernelException => None }

  private def getOnlineIndex(descriptor: IndexDescriptor): Option[IndexDescriptor] =
    statement.readOperations().indexGetState(descriptor) match {
      case InternalIndexState.ONLINE => Some(descriptor)
      case _                         => None
    }

  def getUniquenessConstraint(labelName: String, propertyKey: String): Option[UniquenessConstraint] = try {
    val labelId = statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = statement.readOperations().propertyKeyGetForName(propertyKey)

    import scala.collection.JavaConverters._
    statement.readOperations().constraintsGetForLabelAndPropertyKey(labelId, propertyKeyId).asScala.collectFirst {
      case unique: UniquenessConstraint => unique
    }
  } catch {
    case _: KernelException => None
  }

  def checkNodeIndex(idxName: String) {
    if (!gdb.index().existsForNodes(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def checkRelIndex(idxName: String)  {
    if ( !gdb.index().existsForRelationships(idxName) ) {
      throw new MissingIndexException(idxName)
    }
  }

  def getOrCreateFromSchemaState[T](key: Any, f: => T): T = {
    val javaCreator = new org.neo4j.function.Function[Any, T]() {
      def apply(key: Any) = f
    }
    statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }

  val statistics: GraphStatistics =
    InstrumentedGraphStatistics(TransactionBoundGraphStatistics(statement), MutableGraphStatisticsSnapshot())

  val txIdProvider = LastCommittedTxIdProvider(gdb)
}
