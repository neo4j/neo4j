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
package org.neo4j.cypher.internal.spi.v2_3

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.compiler.v2_3.pipes.EntityProducer
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.ExpanderStep
import org.neo4j.cypher.internal.compiler.v2_3.spi.SchemaTypes.{IndexDescriptor, UniquenessConstraint}
import org.neo4j.cypher.internal.compiler.v2_3.spi._
import org.neo4j.graphdb.{GraphDatabaseService, Node}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.constraints.{UniquenessConstraint => KernelUniquenessConstraint}
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException
import org.neo4j.kernel.api.index.{InternalIndexState, IndexDescriptor => KernelIndexDescriptor}
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore

import scala.collection.JavaConverters._

class TransactionBoundPlanContext(initialStatement: Statement, val gdb: GraphDatabaseService)
  extends TransactionBoundTokenContext(initialStatement) with PlanContext with IndexDescriptorCompatibility {

  @Deprecated
  def getIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] = evalOrNone {
    val labelId = statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = statement.readOperations().propertyKeyGetForName(propertyKey)

    getOnlineIndex(statement.readOperations().indexesGetForLabelAndPropertyKey(labelId, propertyKeyId))
  }

  def hasIndexRule(labelName: String): Boolean = {
    val labelId = statement.readOperations().labelGetForName(labelName)

    val indexDescriptors = statement.readOperations().indexesGetForLabel(labelId).asScala
    val onlineIndexDescriptors = indexDescriptors.flatMap(getOnlineIndex)

    onlineIndexDescriptors.nonEmpty
  }

  def getUniqueIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] = evalOrNone {
    val labelId = statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = statement.readOperations().propertyKeyGetForName(propertyKey)

    // here we do not need to use getOnlineIndex method because uniqueness constraint creation is synchronous
    Some(statement.readOperations().uniqueIndexGetForLabelAndPropertyKey(labelId, propertyKeyId))
  }

  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try { f } catch { case _: SchemaKernelException => None }

  private def getOnlineIndex(descriptor: KernelIndexDescriptor): Option[IndexDescriptor] =
    statement.readOperations().indexGetState(descriptor) match {
      case InternalIndexState.ONLINE => Some(descriptor)
      case _                         => None
    }

  def getUniquenessConstraint(labelName: String, propertyKey: String): Option[UniquenessConstraint] = try {
    val labelId = statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = statement.readOperations().propertyKeyGetForName(propertyKey)

    val matchingConstraints = statement.readOperations().constraintsGetForLabelAndPropertyKey(labelId, propertyKeyId)

    import scala.collection.JavaConverters._
    statement.readOperations().constraintsGetForLabelAndPropertyKey(labelId, propertyKeyId).asScala.collectFirst {
      case unique: KernelUniquenessConstraint => UniquenessConstraint(unique.label(), unique.propertyKey())
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


  // Legacy traversal matchers (pre-Ronja) (These were moved out to remove the dependency on the kernel)
  override def monoDirectionalTraversalMatcher(steps: ExpanderStep, start: EntityProducer[Node]) =
    new MonoDirectionalTraversalMatcher(steps, start)

  override def bidirectionalTraversalMatcher(steps: ExpanderStep,
                                             start: EntityProducer[Node],
                                             end: EntityProducer[Node]) =
    new BidirectionalTraversalMatcher(steps, start, end)

  val statistics: GraphStatistics =
    InstrumentedGraphStatistics(TransactionBoundGraphStatistics(statement), new MutableGraphStatisticsSnapshot())

  val txIdProvider: () => Long = gdb.asInstanceOf[GraphDatabaseAPI]
    .getDependencyResolver
    .resolveDependency(classOf[TransactionIdStore])
    .getLastCommittedTransactionId
}
