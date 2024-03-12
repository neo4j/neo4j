/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.spi

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.planner.spi.EventuallyConsistent
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.OrderCapability
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.ValueCapability
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundReadTokenContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.TransactionBoundPlanContext.typeToValueCategory
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherProcedureSignature
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherType
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherValue
import org.neo4j.cypher.internal.spi.procsHelpers.asOption
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.KernelException
import org.neo4j.internal.kernel.api.InternalIndexState
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.schema
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexOrderCapability.ASC_FULLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.ASC_PARTIALLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.BOTH_FULLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.BOTH_PARTIALLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.DESC_FULLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.DESC_PARTIALLY_SORTED
import org.neo4j.internal.schema.IndexOrderCapability.NONE
import org.neo4j.internal.schema.IndexValueCapability
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.logging.Log
import org.neo4j.values.storable.ValueCategory

import scala.collection.JavaConverters.asScalaBufferConverter
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.collection.JavaConverters.seqAsJavaListConverter

object TransactionBoundPlanContext {
  def apply(tc: TransactionalContextWrapper,
            logger: InternalNotificationLogger,
            log: Log): TransactionBoundPlanContext = {

    val statistics = TransactionBoundGraphStatistics(tc.dataRead, tc.schemaRead, log)

    new TransactionBoundPlanContext(tc, logger, InstrumentedGraphStatistics(statistics, new MutableGraphStatisticsSnapshot()))
  }

  def procedureSignature(tx: KernelTransaction, name: QualifiedName): ProcedureSignature = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = tx.procedures()
    val handle = procedures.procedureGet(kn)

    asCypherProcedureSignature(name, handle.id(), handle.signature())
  }

  def functionSignature(tx: KernelTransaction, name: QualifiedName): Option[UserFunctionSignature] = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = tx.procedures()
    val func = procedures.functionGet(kn)

    val (fcn, aggregation) = if (func != null) (func, false)
    else (procedures.aggregationFunctionGet(kn), true)
    if (fcn == null) None
    else {
      val signature = fcn.signature()
      val input = signature.inputSignature().asScala
        .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
        .toIndexedSeq
      val output = asCypherType(signature.outputType())
      val deprecationInfo = asOption(signature.deprecated())
      val description = asOption(signature.description())

      Some(UserFunctionSignature(name, input, output, deprecationInfo,
        description, isAggregate = aggregation,
        id = fcn.id(), signature.isBuiltIn, threadSafe = fcn.threadSafe()))
    }
  }

  /**
   * Translate a Cypher Type to a ValueCategory that IndexReference can handle
   */
  private def typeToValueCategory(in: CypherType): ValueCategory = in match {
    case _: symbols.IntegerType |
         _: symbols.FloatType =>
      ValueCategory.NUMBER

    case _: symbols.StringType =>
      ValueCategory.TEXT

    case _: symbols.GeometryType | _: symbols.PointType =>
      ValueCategory.GEOMETRY

    case _: symbols.DateTimeType | _: symbols.LocalDateTimeType | _: symbols.DateType | _: symbols.TimeType | _: symbols.LocalTimeType | _: symbols.DurationType =>
      ValueCategory.TEMPORAL

    // For everything else, we don't know
    case _ =>
      ValueCategory.UNKNOWN
  }
}

class TransactionBoundPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger, graphStatistics: InstrumentedGraphStatistics)
  extends TransactionBoundReadTokenContext(tc) with PlanContext with IndexDescriptorCompatibility {

  override def btreeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexesGetForLabel(labelId, schema.IndexType.BTREE)
  }

  override def btreeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
    indexesGetForRelType(relTypeId, schema.IndexType.BTREE)
  }

  override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexesGetForLabel(labelId, schema.IndexType.TEXT)
  }

  override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
    indexesGetForRelType(relTypeId, schema.IndexType.TEXT)
  }

  private def indexesGetForLabel(labelId: Int, indexType: schema.IndexType): Iterator[IndexDescriptor] = {
    tc.schemaRead.getLabelIndexesNonLocking(labelId).asScala
      .filter(_.getIndexType == indexType)
      .flatMap(getOnlineIndex)
  }

  private def indexesGetForRelType(relTypeId: Int, indexType: schema.IndexType): Iterator[IndexDescriptor] = {
    tc.schemaRead.getRelTypeIndexesNonLocking(relTypeId).asScala
      .filter(_.getIndexType == indexType)
      .flatMap(getOnlineIndex)
  }

  override def propertyIndexesGetAll(): Iterator[IndexDescriptor] = tc.schemaRead.indexesGetAllNonLocking.asScala.flatMap(getOnlineIndex)

  override def btreeIndexExistsForLabel(labelId: Int): Boolean = {
    btreeIndexesGetForLabel(labelId).nonEmpty
  }

  override def btreeIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
    indexGetForLabelAndProperties(schema.IndexType.BTREE, labelName, propertyKeys)
  }

  override def textIndexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
    indexGetForLabelAndProperties(schema.IndexType.TEXT, labelName, propertyKeys)
  }

  override def btreeIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
    indexGetForRelTypeAndProperties(schema.IndexType.BTREE, relTypeName, propertyKeys)
  }

  override def textIndexGetForRelTypeAndProperties(relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
    indexGetForRelTypeAndProperties(schema.IndexType.TEXT, relTypeName, propertyKeys)
  }

  private def indexGetForLabelAndProperties(indexType: schema.IndexType, labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
    val descriptor = toLabelSchemaDescriptor(this, labelName, propertyKeys)
    descriptor.flatMap(indexGetForSchemaDescriptor(indexType))
  }

  private def indexGetForRelTypeAndProperties(indexType: schema.IndexType, relTypeName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = {
    val descriptor = toRelTypeSchemaDescriptor(this, relTypeName, propertyKeys)
    descriptor.flatMap(indexGetForSchemaDescriptor(indexType))
  }

  private def indexGetForSchemaDescriptor(indexType: schema.IndexType)(descriptor: SchemaDescriptor): Option[IndexDescriptor] = {
    val itr = tc.schemaRead.indexForSchemaNonLocking(descriptor).asScala
      .filter(_.getIndexType == indexType)
      .flatMap(getOnlineIndex)
    if (itr.hasNext) Some(itr.next) else None
  }

  override def btreeIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    btreeIndexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  override def textIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    textIndexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  override def btreeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKey: Seq[String]): Boolean = {
    btreeIndexGetForRelTypeAndProperties(relTypeName, propertyKey).isDefined
  }

  override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKey: Seq[String]): Boolean = {
    textIndexGetForRelTypeAndProperties(relTypeName, propertyKey).isDefined
  }

  private def getOnlineIndex(reference: schema.IndexDescriptor): Option[IndexDescriptor] = {
    try {
      tc.schemaRead.indexGetStateNonLocking(reference) match {
        case InternalIndexState.ONLINE if reference.schema.getPropertyIds.nonEmpty =>
          val entityType = {
            val tokenId = reference.schema().getEntityTokenIds()(0)
            reference.schema().entityType() match {
              case EntityType.NODE => IndexDescriptor.EntityType.Node(LabelId(tokenId))
              case EntityType.RELATIONSHIP => IndexDescriptor.EntityType.Relationship(RelTypeId(tokenId))
            }
          }

          val properties = reference.schema.getPropertyIds.map(PropertyKeyId)
          val isUnique = reference.isUnique
          val behaviours = reference.getCapability.behaviours().map(kernelToCypher).toSet
          val orderCapability: OrderCapability = tps => {
            // The Kernel index order gives additional information if all values are sorted, or if there can be geometry values which are not sorted.
            // From Cyphers perspective, using the Kernel API, fully and partially sorted are the same, since geometry values which come out of order
            // from the index are sorted in DefaultNodeValueIndexCursor.
            reference.getCapability.orderCapability(tps.map(typeToValueCategory): _*) match {
              case BOTH_FULLY_SORTED => IndexOrderCapability.BOTH
              case BOTH_PARTIALLY_SORTED => IndexOrderCapability.BOTH
              case ASC_FULLY_SORTED => IndexOrderCapability.ASC
              case ASC_PARTIALLY_SORTED => IndexOrderCapability.ASC
              case DESC_FULLY_SORTED => IndexOrderCapability.DESC
              case DESC_PARTIALLY_SORTED => IndexOrderCapability.DESC
              case NONE => IndexOrderCapability.NONE
            }
          }
          val valueCapability: ValueCapability = tps => {
            reference.getCapability.valueCapability(tps.map(typeToValueCategory): _*) match {
              // As soon as the kernel provides an array of IndexValueCapability, this mapping can change
              case IndexValueCapability.YES => tps.map(_ => CanGetValue)
              case IndexValueCapability.PARTIAL => tps.map(_ => DoNotGetValue)
              case IndexValueCapability.NO => tps.map(_ => DoNotGetValue)
            }
          }
          if (behaviours.contains(EventuallyConsistent)) {
            // Ignore eventually consistent indexes. Those are for explicit querying via procedures.
            None
          } else {
            kernelToCypher(reference.getIndexType) map { indexType =>
              IndexDescriptor(indexType, entityType, properties, behaviours, orderCapability, valueCapability, isUnique)
            }
          }
        case _ => None
      }
    } catch {
      case _: IndexNotFoundKernelException =>
        // The index may be dropped after acquiring the reference.
        None
    }
  }

  override def canLookupNodesByLabel: Boolean = {
    tc.schemaRead.indexForSchemaNonTransactional(SchemaDescriptors.forAnyEntityTokens(EntityType.NODE)).hasNext
  }

  override def canLookupRelationshipsByType: Boolean = {
    tc.schemaRead.indexForSchemaNonTransactional(SchemaDescriptors.forAnyEntityTokens(EntityType.RELATIONSHIP)).hasNext
  }

  override def hasNodePropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
    try {
      val labelId = getLabelId(labelName)
      val propertyKeyId = getPropertyKeyId(propertyKey)

      tc.schemaRead.constraintsGetForSchemaNonLocking(SchemaDescriptors.forLabel(labelId, propertyKeyId)).asScala
        .filter(c => c.enforcesPropertyExistence())
        .hasNext
    } catch {
      case _: KernelException => false
    }
  }

  override def getNodePropertiesWithExistenceConstraint(labelName: String): Set[String] = {
    try {
      val labelId = getLabelId(labelName)

      val constraints: Iterator[ConstraintDescriptor] = tc.schemaRead.constraintsGetForLabelNonLocking(labelId).asScala

      getPropertiesFromExistenceConstraints(constraints)
    } catch {
      case _: KernelException => Set.empty
    }
  }

  private def getPropertiesFromExistenceConstraints(constraints: Iterator[ConstraintDescriptor]):Set[String] = {
    // We are only interested in existence constraints, not unique constraints
    val existsConstraints = constraints.filter(c => c.enforcesPropertyExistence())

    // Fetch the names of all unique properties that are part of at least one existence constraint with the given label
    // i.e. the names of all properties that an entity with the given label/relationship type must have
    val distinctPropertyIds: Set[Int] = existsConstraints.flatMap(_.schema().getPropertyIds).toSet
    distinctPropertyIds.map(id => tc.tokenRead.propertyKeyName(id))
  }

  override def hasRelationshipPropertyExistenceConstraint(relTypeName: String, propertyKey: String): Boolean = {
    try {
      val relTypeId = getRelTypeId(relTypeName)
      val propertyKeyId = getPropertyKeyId(propertyKey)

      tc.schemaRead.constraintsGetForSchemaNonLocking(SchemaDescriptors.forRelType(relTypeId, propertyKeyId)).asScala
        .filter(c => c.enforcesPropertyExistence())
        .hasNext
    } catch {
      case _: KernelException => false
    }
  }

  override def getRelationshipPropertiesWithExistenceConstraint(relTypeName: String): Set[String] = {
    try {
      val relTypeId = getRelTypeId(relTypeName)

      val constraints: Iterator[ConstraintDescriptor] = tc.schemaRead.constraintsGetForRelationshipTypeNonLocking(relTypeId).asScala

      getPropertiesFromExistenceConstraints(constraints)
    } catch {
      case _: KernelException => Set.empty
    }
  }

  override def getPropertiesWithExistenceConstraint: Set[String] = {
    try {
      val constraints = tc.schemaRead.constraintsGetAllNonLocking().asScala

      getPropertiesFromExistenceConstraints(constraints)
    } catch {
      case _: KernelException => Set.empty
    }
  }

  override val statistics: InstrumentedGraphStatistics = graphStatistics

  // TODO: graph
  override val lastCommittedTxIdProvider: LastCommittedTxIdProvider = LastCommittedTxIdProvider(tc.graph)

  override def procedureSignature(name: QualifiedName): ProcedureSignature = TransactionBoundPlanContext.procedureSignature(tc.kernelTransaction, name)

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = TransactionBoundPlanContext.functionSignature(tc.kernelTransaction, name)

  override def notificationLogger(): InternalNotificationLogger = logger

  override def txStateHasChanges(): Boolean = tc.dataRead.transactionStateHasChanges()
}
