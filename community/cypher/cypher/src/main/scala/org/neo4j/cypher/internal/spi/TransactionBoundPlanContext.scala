/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.spi

import org.neo4j.common.EntityType
import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.logical.plans.CanGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.planner.spi.EventuallyConsistent
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor
import org.neo4j.cypher.internal.planner.spi.IndexOrderCapability
import org.neo4j.cypher.internal.planner.spi.InstrumentedGraphStatistics
import org.neo4j.cypher.internal.planner.spi.MutableGraphStatisticsSnapshot
import org.neo4j.cypher.internal.planner.spi.PlanContext
import org.neo4j.cypher.internal.planner.spi.TokenIndexDescriptor
import org.neo4j.cypher.internal.runtime.interpreted.TransactionBoundReadTokenContext
import org.neo4j.cypher.internal.runtime.interpreted.TransactionalContextWrapper
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherProcedureSignature
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherType
import org.neo4j.cypher.internal.spi.procsHelpers.asCypherValue
import org.neo4j.cypher.internal.spi.procsHelpers.asOption
import org.neo4j.cypher.internal.util.InternalNotificationLogger
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.exceptions.KernelException
import org.neo4j.internal.kernel.api.InternalIndexState
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.schema
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.SchemaDescriptor
import org.neo4j.internal.schema.SchemaDescriptors
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.logging.InternalLog

import scala.jdk.CollectionConverters.IteratorHasAsScala
import scala.jdk.CollectionConverters.ListHasAsScala
import scala.jdk.CollectionConverters.SeqHasAsJava

object TransactionBoundPlanContext {

  def apply(
    tc: TransactionalContextWrapper,
    logger: InternalNotificationLogger,
    log: InternalLog
  ): TransactionBoundPlanContext = {

    val statistics = TransactionBoundGraphStatistics(tc.dataRead, tc.schemaRead, log)

    new TransactionBoundPlanContext(
      tc,
      logger,
      InstrumentedGraphStatistics(statistics, new MutableGraphStatisticsSnapshot())
    )
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

    val (fcn, aggregation) =
      if (func != null) (func, false)
      else (procedures.aggregationFunctionGet(kn), true)
    if (fcn == null) None
    else {
      val signature = fcn.signature()
      val input = signature.inputSignature().asScala
        .map(s =>
          FieldSignature(
            s.name(),
            asCypherType(s.neo4jType()),
            asOption(s.defaultValue()).map(asCypherValue),
            deprecated = s.isDeprecated,
            sensitive = s.isSensitive
          )
        )
        .toIndexedSeq
      val output = asCypherType(signature.outputType())
      val deprecationInfo = asOption(signature.deprecated())
      val description = asOption(signature.description())

      Some(UserFunctionSignature(
        name,
        input,
        output,
        deprecationInfo,
        description,
        isAggregate = aggregation,
        id = fcn.id(),
        signature.isBuiltIn,
        threadSafe = fcn.threadSafe()
      ))
    }
  }
}

class TransactionBoundPlanContext(
  tc: TransactionalContextWrapper,
  logger: InternalNotificationLogger,
  graphStatistics: InstrumentedGraphStatistics
) extends TransactionBoundReadTokenContext(tc) with PlanContext with IndexDescriptorCompatibility {

  override def rangeIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexesGetForLabel(labelId, Some(schema.IndexType.RANGE))
  }

  override def rangeIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
    indexesGetForRelType(relTypeId, Some(schema.IndexType.RANGE))
  }

  override def textIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexesGetForLabel(labelId, Some(schema.IndexType.TEXT))
  }

  override def textIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
    indexesGetForRelType(relTypeId, Some(schema.IndexType.TEXT))
  }

  override def pointIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    indexesGetForLabel(labelId, Some(schema.IndexType.POINT))
  }

  override def pointIndexesGetForRelType(relTypeId: Int): Iterator[IndexDescriptor] = {
    indexesGetForRelType(relTypeId, Some(schema.IndexType.POINT))
  }

  private def indexesGetForLabel(labelId: Int, indexType: Option[schema.IndexType]): Iterator[IndexDescriptor] = {
    val selector: schema.IndexDescriptor => Boolean = indexType match {
      case Some(it) => _.getIndexType == it
      case None     => _ => true
    }

    tc.schemaRead.getLabelIndexesNonLocking(labelId).asScala
      .filter(selector)
      .flatMap(getOnlineIndex)
  }

  private def indexesGetForRelType(relTypeId: Int, indexType: Option[schema.IndexType]): Iterator[IndexDescriptor] = {
    val selector: schema.IndexDescriptor => Boolean = indexType match {
      case Some(it) => _.getIndexType == it
      case None     => _ => true
    }

    tc.schemaRead.getRelTypeIndexesNonLocking(relTypeId).asScala
      .filter(selector)
      .flatMap(getOnlineIndex)
  }

  override def propertyIndexesGetAll(): Iterator[IndexDescriptor] =
    tc.schemaRead.indexesGetAllNonLocking.asScala.flatMap(getOnlineIndex)

  override def indexExistsForLabel(labelId: Int): Boolean = {
    indexesGetForLabel(labelId, None).nonEmpty
  }

  override def indexExistsForRelType(relTypeId: Int): Boolean = {
    indexesGetForRelType(relTypeId, None).nonEmpty
  }

  override def textIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    indexGetForLabelAndProperties(schema.IndexType.TEXT, labelName, propertyKeys)
  }

  override def rangeIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    indexGetForLabelAndProperties(schema.IndexType.RANGE, labelName, propertyKeys)
  }

  override def pointIndexGetForLabelAndProperties(
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    indexGetForLabelAndProperties(schema.IndexType.POINT, labelName, propertyKeys)
  }

  override def textIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    indexGetForRelTypeAndProperties(schema.IndexType.TEXT, relTypeName, propertyKeys)
  }

  override def rangeIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    indexGetForRelTypeAndProperties(schema.IndexType.RANGE, relTypeName, propertyKeys)
  }

  override def pointIndexGetForRelTypeAndProperties(
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    indexGetForRelTypeAndProperties(schema.IndexType.POINT, relTypeName, propertyKeys)
  }

  private def indexGetForLabelAndProperties(
    indexType: schema.IndexType,
    labelName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    val descriptor = toLabelSchemaDescriptor(this, labelName, propertyKeys)
    descriptor.flatMap(indexGetForSchemaDescriptor(indexType))
  }

  private def indexGetForRelTypeAndProperties(
    indexType: schema.IndexType,
    relTypeName: String,
    propertyKeys: Seq[String]
  ): Option[IndexDescriptor] = {
    val descriptor = toRelTypeSchemaDescriptor(this, relTypeName, propertyKeys)
    descriptor.flatMap(indexGetForSchemaDescriptor(indexType))
  }

  private def indexGetForSchemaDescriptor(indexType: schema.IndexType)(descriptor: SchemaDescriptor)
    : Option[IndexDescriptor] = {
    val itr = tc.schemaRead.indexForSchemaNonLocking(descriptor).asScala
      .filter(_.getIndexType == indexType)
      .flatMap(getOnlineIndex)
    if (itr.hasNext) Some(itr.next()) else None
  }

  override def textIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    textIndexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  override def rangeIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    rangeIndexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  override def pointIndexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    pointIndexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  override def textIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKey: Seq[String]): Boolean = {
    textIndexGetForRelTypeAndProperties(relTypeName, propertyKey).isDefined
  }

  override def rangeIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKey: Seq[String]): Boolean = {
    rangeIndexGetForRelTypeAndProperties(relTypeName, propertyKey).isDefined
  }

  override def pointIndexExistsForRelTypeAndProperties(relTypeName: String, propertyKey: Seq[String]): Boolean = {
    pointIndexGetForRelTypeAndProperties(relTypeName, propertyKey).isDefined
  }

  private def getOnlineIndex(reference: schema.IndexDescriptor): Option[IndexDescriptor] = {
    try {
      tc.schemaRead.indexGetStateNonLocking(reference) match {
        case InternalIndexState.ONLINE if reference.schema.getPropertyIds.nonEmpty =>
          val entityType = {
            val tokenId = reference.schema().getEntityTokenIds()(0)
            reference.schema().entityType() match {
              case EntityType.NODE         => IndexDescriptor.EntityType.Node(LabelId(tokenId))
              case EntityType.RELATIONSHIP => IndexDescriptor.EntityType.Relationship(RelTypeId(tokenId))
            }
          }

          val properties = reference.schema.getPropertyIds.map(PropertyKeyId)
          val isUnique = reference.isUnique
          val behaviours = reference.getCapability.behaviours().map(kernelToCypher).toSet
          val orderCapability =
            if (reference.getCapability.supportsOrdering()) {
              IndexOrderCapability.BOTH
            } else {
              IndexOrderCapability.NONE
            }
          val valueCapability =
            if (reference.getCapability.supportsReturningValues()) {
              CanGetValue
            } else {
              DoNotGetValue
            }
          if (behaviours.contains(EventuallyConsistent)) {
            // Ignore eventually consistent indexes. Those are for explicit querying via procedures.
            None
          } else if (isUnique && (tc.schemaRead.indexGetOwningUniquenessConstraintIdNonLocking(reference) eq null)) {
            // Unique indexes must have a matching constraint. If not, something went wrong during constraint creation.
            // Shouldn't really happen.
            None
          } else {
            kernelToCypher(reference.getIndexType) map { indexType =>
              IndexDescriptor(
                indexType,
                entityType,
                properties,
                behaviours,
                orderCapability,
                valueCapability,
                Some(reference.getCapability),
                isUnique
              )
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

  private def getTokenIndexDescriptor(indexes: java.util.Iterator[schema.IndexDescriptor])
    : Option[TokenIndexDescriptor] = {
    indexes.asScala
      .nextOption()
      .map { kernelIndexDescriptor =>
        val typ = kernelIndexDescriptor.schema().entityType()
        val orderCapability =
          if (kernelIndexDescriptor.getCapability.supportsOrdering()) {
            IndexOrderCapability.BOTH
          } else {
            IndexOrderCapability.NONE
          }
        TokenIndexDescriptor(typ, orderCapability)
      }
  }

  override def nodeTokenIndex: Option[TokenIndexDescriptor] = {
    getTokenIndexDescriptor(
      tc.schemaRead.indexForSchemaNonTransactional(SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR)
    )
  }

  override def relationshipTokenIndex: Option[TokenIndexDescriptor] = {
    getTokenIndexDescriptor(
      tc.schemaRead.indexForSchemaNonTransactional(SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR)
    )
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

  private def getPropertiesFromExistenceConstraints(constraints: Iterator[ConstraintDescriptor]): Set[String] = {
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

      val constraints: Iterator[ConstraintDescriptor] =
        tc.schemaRead.constraintsGetForRelationshipTypeNonLocking(relTypeId).asScala

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

  override def hasNodePropertyTypeConstraint(
    labelName: String,
    propertyKey: String,
    cypherType: SchemaValueType
  ): Boolean = {
    getNodePropertiesWithTypeConstraint(labelName).get(propertyKey) match {
      case Some(Seq(`cypherType`)) => true
      case _                       => false
    }
  }

  override def getNodePropertiesWithTypeConstraint(labelName: String): Map[String, Seq[SchemaValueType]] = {
    try {
      val labelId = getLabelId(labelName)

      val constraints: Iterator[ConstraintDescriptor] = tc.schemaRead.constraintsGetForLabelNonLocking(labelId).asScala
      val typeConstraints = constraints.filter(c => c.enforcesPropertyType()).map(_.asPropertyTypeConstraint())

      typeConstraints.map(typeConstraint =>
        (
          tc.tokenRead.propertyKeyName(typeConstraint.schema().getPropertyId),
          typeConstraint.propertyType().values().toSeq
        )
      ).toMap
    } catch {
      case _: KernelException => Map.empty
    }
  }

  override def hasRelationshipPropertyTypeConstraint(
    relTypeName: String,
    propertyKey: String,
    cypherType: SchemaValueType
  ): Boolean = {
    getRelationshipPropertiesWithTypeConstraint(relTypeName).get(propertyKey) match {
      case Some(Seq(`cypherType`)) => true
      case _                       => false
    }
  }

  override def getRelationshipPropertiesWithTypeConstraint(relTypeName: String): Map[String, Seq[SchemaValueType]] = {
    try {
      val relTypeId = getRelTypeId(relTypeName)

      val constraints: Iterator[ConstraintDescriptor] =
        tc.schemaRead.constraintsGetForRelationshipTypeNonLocking(relTypeId).asScala
      val typeConstraints = constraints.filter(c => c.enforcesPropertyType()).map(_.asPropertyTypeConstraint())

      typeConstraints.map(typeConstraint =>
        (
          tc.tokenRead.propertyKeyName(typeConstraint.schema().getPropertyId),
          typeConstraint.propertyType().values().toSeq
        )
      ).toMap
    } catch {
      case _: KernelException => Map.empty
    }
  }

  override val statistics: InstrumentedGraphStatistics = graphStatistics

  override val lastCommittedTxIdProvider: LastCommittedTxIdProvider = LastCommittedTxIdProvider(tc.graph)

  override def procedureSignature(name: QualifiedName): ProcedureSignature =
    TransactionBoundPlanContext.procedureSignature(tc.kernelTransaction, name)

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] =
    TransactionBoundPlanContext.functionSignature(tc.kernelTransaction, name)

  override def notificationLogger(): InternalNotificationLogger = logger

  override def txStateHasChanges(): Boolean = tc.dataRead.transactionStateHasChanges()

  override def procedureSignatureVersion: Long = tc.procedures.signatureVersion

  override def withNotificationLogger(notificationLogger: InternalNotificationLogger): PlanContext =
    new TransactionBoundPlanContext(tc, notificationLogger, graphStatistics)
}
