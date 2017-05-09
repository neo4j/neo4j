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
package org.neo4j.cypher.internal.spi.v2_3

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.compiler.v2_3.pipes.EntityProducer
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.ExpanderStep
import org.neo4j.cypher.internal.compiler.v2_3.spi._
import org.neo4j.cypher.internal.spi.v3_3.TransactionalContextWrapper
import org.neo4j.graphdb.Node
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException
import org.neo4j.kernel.api.index.InternalIndexState
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor
import org.neo4j.kernel.api.schema.index.{IndexDescriptor => KernelIndexDescriptor}
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore

import scala.collection.JavaConverters._

class TransactionBoundPlanContext(tc: TransactionalContextWrapper)
  extends TransactionBoundTokenContext(tc.statement) with PlanContext with SchemaDescriptorTranslation {

  @Deprecated
  def getIndexRule(labelName: String, propertyKey: String): Option[SchemaTypes.IndexDescriptor] = evalOrNone {
    val labelId = tc.statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = tc.statement.readOperations().propertyKeyGetForName(propertyKey)

    getOnlineIndex(tc.statement.readOperations().indexGetForSchema(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)))
  }

  def hasIndexRule(labelName: String): Boolean = {
    val labelId = tc.statement.readOperations().labelGetForName(labelName)

    val indexDescriptors = tc.statement.readOperations().indexesGetForLabel(labelId).asScala
    val onlineIndexDescriptors = indexDescriptors.flatMap(getOnlineIndex)

    onlineIndexDescriptors.nonEmpty
  }

  def getUniqueIndexRule(labelName: String, propertyKey: String): Option[SchemaTypes.IndexDescriptor] = evalOrNone {
    val labelId = tc.statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = tc.statement.readOperations().propertyKeyGetForName(propertyKey)
    val schema = tc.statement.readOperations().indexGetForSchema(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId))

    if (schema.`type`() == KernelIndexDescriptor.Type.UNIQUE) getOnlineIndex(schema)
    else None
  }

  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try { f } catch { case _: SchemaKernelException => None }

  private def getOnlineIndex(descriptor: KernelIndexDescriptor): Option[SchemaTypes.IndexDescriptor] =
    tc.statement.readOperations().indexGetState(descriptor) match {
      case InternalIndexState.ONLINE => Some(descriptor)
      case _                         => None
    }

  def getUniquenessConstraint(labelName: String, propertyKey: String): Option[SchemaTypes.UniquenessConstraint] = try {
    val labelId = tc.statement.readOperations().labelGetForName(labelName)
    val propertyKeyId = tc.statement.readOperations().propertyKeyGetForName(propertyKey)

    import scala.collection.JavaConverters._
    tc.statement.readOperations().constraintsGetForSchema(
      SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)
    ).asScala.collectFirst {
      case constraint: ConstraintDescriptor if constraint.enforcesUniqueness() =>
        SchemaTypes.UniquenessConstraint(labelId, propertyKeyId)
    }
  } catch {
    case _: KernelException => None
  }

  def checkNodeIndex(idxName: String) {
    if (!tc.statement.readOperations().nodeLegacyIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def checkRelIndex(idxName: String)  {
    if (!tc.statement.readOperations().relationshipLegacyIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def getOrCreateFromSchemaState[T](key: Any, f: => T): T = {
    val javaCreator = new java.util.function.Function[Any, T]() {
      def apply(key: Any) = f
    }
    tc.statement.readOperations().schemaStateGetOrCreate(key, javaCreator)
  }


  // Legacy traversal matchers (pre-Ronja) (These were moved out to remove the dependency on the kernel)
  override def monoDirectionalTraversalMatcher(steps: ExpanderStep, start: EntityProducer[Node]) =
    new MonoDirectionalTraversalMatcher(steps, start)

  override def bidirectionalTraversalMatcher(steps: ExpanderStep,
                                             start: EntityProducer[Node],
                                             end: EntityProducer[Node]) =
    new BidirectionalTraversalMatcher(steps, start, end)

  val statistics: GraphStatistics =
    InstrumentedGraphStatistics(TransactionBoundGraphStatistics(tc.readOperations), new MutableGraphStatisticsSnapshot())

  val txIdProvider: () => Long = tc.graph
    .getDependencyResolver
    .resolveDependency(classOf[TransactionIdStore])
    .getLastCommittedTransactionId
}
