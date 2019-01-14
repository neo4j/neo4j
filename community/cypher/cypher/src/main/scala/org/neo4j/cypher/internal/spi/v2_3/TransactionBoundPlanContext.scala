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
package org.neo4j.cypher.internal.spi.v2_3

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.compiler.v2_3.pipes.EntityProducer
import org.neo4j.cypher.internal.compiler.v2_3.pipes.matching.ExpanderStep
import org.neo4j.cypher.internal.compiler.v2_3.spi._
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.graphdb.Node
import org.neo4j.internal.kernel.api.exceptions.KernelException
import org.neo4j.internal.kernel.api.{IndexReference, InternalIndexState}
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor
import org.neo4j.kernel.api.schema.index.{SchemaIndexDescriptor => KernelIndexDescriptor}
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore

import scala.collection.JavaConverters._

class TransactionBoundPlanContext(tc: TransactionalContextWrapper)
  extends TransactionBoundTokenContext(tc.kernelTransaction) with PlanContext with SchemaDescriptorTranslation {

  @Deprecated
  def getIndexRule(labelName: String, propertyKey: String): Option[SchemaTypes.IndexDescriptor] = evalOrNone {
    val labelId = getLabelId(labelName)
    val propertyKeyId = getPropertyKeyId(propertyKey)

    getOnlineIndex(tc.schemaRead.index(labelId, propertyKeyId))
  }

  def hasIndexRule(labelName: String): Boolean = {
    val labelId = getLabelId(labelName)

    val indexDescriptors = tc.schemaRead.indexesGetForLabel(labelId).asScala
    val onlineIndexDescriptors = indexDescriptors.flatMap(getOnlineIndex)

    onlineIndexDescriptors.nonEmpty
  }

  def getUniqueIndexRule(labelName: String, propertyKey: String): Option[SchemaTypes.IndexDescriptor] = evalOrNone {
    val labelId = getLabelId(labelName)
    val propertyKeyId = getPropertyKeyId(propertyKey)
    val ref = tc.schemaRead.index(labelId, propertyKeyId)

    if (ref.isUnique) getOnlineIndex(ref)
    else None
  }

  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try { f } catch { case _: KernelException => None }

  private def getOnlineIndex(descriptor: IndexReference): Option[SchemaTypes.IndexDescriptor] =
    tc.schemaRead.indexGetState(descriptor) match {
      case InternalIndexState.ONLINE => Some(SchemaTypes.IndexDescriptor(descriptor.label(), descriptor.properties()(0)))
      case _                         => None
    }

  def getUniquenessConstraint(labelName: String, propertyKey: String): Option[SchemaTypes.UniquenessConstraint] = evalOrNone {
    val labelId = getLabelId(labelName)
    val propertyKeyId = getPropertyKeyId(propertyKey)

    import scala.collection.JavaConverters._
    tc.schemaRead.constraintsGetForSchema(
      SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)
    ).asScala.collectFirst {
      case constraint: ConstraintDescriptor if constraint.enforcesUniqueness() =>
        SchemaTypes.UniquenessConstraint(labelId, propertyKeyId)
    }
  }

  def checkNodeIndex(idxName: String) {
    val read = tc.kernelTransaction.indexRead()

    if (!read.nodeExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def checkRelIndex(idxName: String)  {
    val read = tc.kernelTransaction.indexRead()

    if (!read.relationshipExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def getOrCreateFromSchemaState[T](key: Any, f: => T): T = {
    val javaCreator = new java.util.function.Function[Any, T]() {
      def apply(key: Any) = f
    }
    tc.schemaRead.schemaStateGetOrCreate(key, javaCreator)
  }


  // Legacy traversal matchers (pre-Ronja) (These were moved out to remove the dependency on the kernel)
  override def monoDirectionalTraversalMatcher(steps: ExpanderStep, start: EntityProducer[Node]) =
    new MonoDirectionalTraversalMatcher(steps, start)

  override def bidirectionalTraversalMatcher(steps: ExpanderStep,
                                             start: EntityProducer[Node],
                                             end: EntityProducer[Node]) =
    new BidirectionalTraversalMatcher(steps, start, end)

  val statistics: GraphStatistics =
    InstrumentedGraphStatistics(TransactionBoundGraphStatistics(tc.dataRead, tc.schemaRead), new MutableGraphStatisticsSnapshot())

  val txIdProvider: () => Long = tc.graph
    .getDependencyResolver
    .resolveDependency(classOf[TransactionIdStore])
    .getLastCommittedTransactionId
}
