/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.planner.spi.IndexDescriptor.{OrderCapability, ValueCapability}
import org.neo4j.cypher.internal.planner.spi._
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.spi.procsHelpers._
import org.neo4j.cypher.internal.v4_0.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.v4_0.util.symbols._
import org.neo4j.cypher.internal.v4_0.util.{LabelId, PropertyKeyId, symbols => types}
import org.neo4j.exceptions.KernelException
import org.neo4j.internal.kernel.api.{InternalIndexState, procs}
import org.neo4j.internal.schema
import org.neo4j.internal.schema.{ConstraintDescriptor, IndexLimitation, IndexOrder, IndexType, IndexValueCapability, SchemaDescriptor}
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.values.storable.ValueCategory

import scala.collection.JavaConverters._

object TransactionBoundPlanContext {
  def apply(tc: TransactionalContextWrapper,
            logger: InternalNotificationLogger): TransactionBoundPlanContext = {

    val statistics = TransactionBoundGraphStatistics(tc.dataRead, tc.schemaRead)

    new TransactionBoundPlanContext(tc, logger, InstrumentedGraphStatistics(statistics, new MutableGraphStatisticsSnapshot()))
  }

  def procedureSignature(tx: KernelTransaction, name: QualifiedName): ProcedureSignature = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = tx.procedures()
    val handle = procedures.procedureGet(kn)
    val signature = handle.signature()
    val input = signature.inputSignature().asScala
      .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
      .toIndexedSeq
    val output = if (signature.isVoid) None else Some(
      signature.outputSignature().asScala.map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), deprecated = s.isDeprecated)).toIndexedSeq)
    val deprecationInfo = asOption(signature.deprecated())
    val mode = asCypherProcMode(signature.mode(), signature.allowed())
    val description = asOption(signature.description())
    val warning = asOption(signature.warning())

    ProcedureSignature(name, input, output, deprecationInfo, mode, description, warning, signature.eager(), handle.id(), signature.systemProcedure())
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
        signature.allowed(), description, isAggregate = aggregation,
        id = fcn.id(), threadSafe = fcn.threadSafe()))
    }
  }
}

class TransactionBoundPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger, graphStatistics: InstrumentedGraphStatistics)
  extends TransactionBoundTokenContext(tc.kernelTransaction) with PlanContext with IndexDescriptorCompatibility {

  override def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    tc.schemaRead.indexesGetForLabel(labelId).asScala.flatMap(getOnlineIndex)
  }

  override def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    tc.schemaRead.indexesGetForLabel(labelId).asScala
      .filter(_.isUnique)
      .flatMap(getOnlineIndex)
  }

  override def indexExistsForLabel(labelId: Int): Boolean = {
    tc.schemaRead.indexesGetForLabel(labelId).asScala.flatMap(getOnlineIndex).nonEmpty
  }

  override def indexGetForLabelAndProperties(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = evalOrNone {
    try {
      val descriptor = toLabelSchemaDescriptor(this, labelName, propertyKeys)
      val itr = tc.schemaRead.index(descriptor).asScala.flatMap(getOnlineIndex)
      if (itr.hasNext) Some(itr.next) else None
    } catch {
      case _: KernelException => None
    }
  }

  override def indexExistsForLabelAndProperties(labelName: String, propertyKey: Seq[String]): Boolean = {
    indexGetForLabelAndProperties(labelName, propertyKey).isDefined
  }

  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try {
      f
    } catch {
      case _: KernelException => None
    }

  private def getOnlineIndex(reference: schema.IndexDescriptor): Option[IndexDescriptor] =
    tc.schemaRead.indexGetState(reference) match {
      case InternalIndexState.ONLINE =>
        val label = LabelId(reference.schema().getEntityTokenIds()(0))
        val properties = reference.schema.getPropertyIds.map(PropertyKeyId)
        val isUnique = reference.isUnique
        val limitations = reference.getCapability.limitations().map(kernelToCypher).toSet
        val orderCapability: OrderCapability = tps => {
           reference.getCapability.orderCapability(tps.map(typeToValueCategory): _*) match {
            case Array() => IndexOrderCapability.NONE
            case Array(IndexOrder.ASCENDING, IndexOrder.DESCENDING) => IndexOrderCapability.BOTH
            case Array(IndexOrder.DESCENDING, schema.IndexOrder.ASCENDING) => IndexOrderCapability.BOTH
            case Array(schema.IndexOrder.ASCENDING) => IndexOrderCapability.ASC
            case Array(schema.IndexOrder.DESCENDING) => IndexOrderCapability.DESC
            case _ => IndexOrderCapability.NONE
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
        if (reference.getIndexType != IndexType.BTREE || reference.getCapability.limitations().contains(IndexLimitation.EVENTUALLY_CONSISTENT)) {
          // Ignore IndexKind.SPECIAL indexes, because we don't know how to correctly plan for and query them. Not yet, anyway.
          // Also, ignore eventually consistent indexes. Those are for explicit querying via procedures.
          None
        } else {
          Some(IndexDescriptor(label, properties, limitations, orderCapability, valueCapability, isUnique))
        }
      case _ => None
    }

  /**
    * Translate a Cypher Type to a ValueCategory that IndexReference can handle
    */
  private def typeToValueCategory(in: CypherType): ValueCategory = in match {
    case _: types.IntegerType |
         _: types.FloatType =>
      ValueCategory.NUMBER

    case _: types.StringType =>
      ValueCategory.TEXT

    case _: types.GeometryType | _: types.PointType =>
      ValueCategory.GEOMETRY

    case _: types.DateTimeType | _: types.LocalDateTimeType | _: types.DateType | _: types.TimeType | _: types.LocalTimeType | _: types.DurationType =>
      ValueCategory.TEMPORAL

    // For everything else, we don't know
    case _ =>
      ValueCategory.UNKNOWN
  }

  override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
   try {
      val labelId = getLabelId(labelName)
      val propertyKeyId = getPropertyKeyId(propertyKey)

      tc.schemaRead.constraintsGetForSchema(SchemaDescriptor.forLabel(labelId, propertyKeyId)).hasNext
    } catch {
      case _: KernelException => false
    }
  }

  override def getPropertiesWithExistenceConstraint(labelName: String): Set[String] = {
    try {
      val labelId = getLabelId(labelName)

      val constraints: Iterator[ConstraintDescriptor] = tc.schemaRead.constraintsGetForLabel(labelId).asScala

      // We are only interested of existence and node key constraints, not unique constraints
      val existsConstraints = constraints.filter(c => c.enforcesPropertyExistence())

      // Fetch the names of all unique properties that are part of at least one existence/node key constraint with the given label
      // i.e. the name of all properties that a node with the given label must have
      val distinctPropertyIds: Set[Int] = existsConstraints.flatMap(_.schema().getPropertyIds).toSet
      distinctPropertyIds.map(id => tc.tokenRead.propertyKeyName(id))
    } catch {
      case _: KernelException => Set.empty
    }
  }

  override val statistics: InstrumentedGraphStatistics = graphStatistics

  override val txIdProvider = LastCommittedTxIdProvider(tc.graph)

  override def procedureSignature(name: QualifiedName): ProcedureSignature = TransactionBoundPlanContext.procedureSignature(tc.kernelTransaction, name)

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = TransactionBoundPlanContext.functionSignature(tc.kernelTransaction, name)

  override def notificationLogger(): InternalNotificationLogger = logger
}
