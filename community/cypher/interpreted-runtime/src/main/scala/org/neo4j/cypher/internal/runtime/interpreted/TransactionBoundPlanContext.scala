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
package org.neo4j.cypher.internal.runtime.interpreted

import java.util.Optional

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.planner.v3_5.spi.IndexDescriptor.{OrderCapability, ValueCapability}
import org.neo4j.cypher.internal.planner.v3_5.spi._
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.internal.kernel.api
import org.neo4j.internal.kernel.api.exceptions.KernelException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.{DefaultParameterValue, Neo4jTypes}
import org.neo4j.internal.kernel.api.{IndexReference, InternalIndexState, procs}
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.procedure.Mode
import org.neo4j.values.storable.ValueCategory
import org.neo4j.cypher.internal.v3_5.frontend.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.v3_5.util.symbols._
import org.neo4j.cypher.internal.v3_5.util.{CypherExecutionException, LabelId, PropertyKeyId, symbols => types}

import scala.collection.JavaConverters._

object TransactionBoundPlanContext {
  def apply(tc: TransactionalContextWrapper, logger: InternalNotificationLogger) =
    new TransactionBoundPlanContext(tc, logger, InstrumentedGraphStatistics(TransactionBoundGraphStatistics(tc.dataRead,
                                                                                                            tc.schemaRead),
      new MutableGraphStatisticsSnapshot()))
}

class TransactionBoundPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger, graphStatistics: GraphStatistics)
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
      getOnlineIndex(tc.schemaRead.index(descriptor.getLabelId, descriptor.getPropertyIds:_*))
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

  private def getOnlineIndex(reference: IndexReference): Option[IndexDescriptor] =
    tc.schemaRead.indexGetState(reference) match {
      case InternalIndexState.ONLINE =>
        val label = LabelId(reference.schema().getEntityTokenIds()(0))
        val properties = reference.properties().map(PropertyKeyId)
        val limitations = reference.limitations().map(kernelToCypher).toSet
        val orderCapability: OrderCapability = tps => {
           reference.orderCapability(tps.map(typeToValueCategory): _*) match {
            case Array() => IndexOrderCapability.NONE
            case Array(api.IndexOrder.ASCENDING, api.IndexOrder.DESCENDING) => IndexOrderCapability.BOTH
            case Array(api.IndexOrder.DESCENDING, api.IndexOrder.ASCENDING) => IndexOrderCapability.BOTH
            case Array(api.IndexOrder.ASCENDING) => IndexOrderCapability.ASC
            case Array(api.IndexOrder.DESCENDING) => IndexOrderCapability.DESC
            case _ => IndexOrderCapability.NONE
          }
        }
        val valueCapability: ValueCapability = tps => {
          reference.valueCapability(tps.map(typeToValueCategory): _*) match {
              // As soon as the kernel provides an array of IndexValueCapability, this mapping can change
            case api.IndexValueCapability.YES => tps.map(_ => CanGetValue)
            case api.IndexValueCapability.PARTIAL => tps.map(_ => DoNotGetValue)
            case api.IndexValueCapability.NO => tps.map(_ => DoNotGetValue)
          }
        }
        if (reference.isFulltextIndex || reference.isEventuallyConsistent) {
          // Ignore fulltext indexes for now, because we don't know how to correctly plan for and query them. Not yet, anyway.
          // Also, ignore eventually consistent indexes. Those are for explicit querying via procesures.
          None
        } else {
          Some(IndexDescriptor(label, properties, limitations, orderCapability, valueCapability))
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

      tc.schemaRead.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)).hasNext
    } catch {
      case _: KernelException => false
    }
  }

  override def checkNodeIndex(idxName: String) {
    if (!tc.kernelTransaction.indexRead().nodeExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  override def checkRelIndex(idxName: String) {
    if (!tc.kernelTransaction.indexRead().relationshipExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  override val statistics: GraphStatistics = graphStatistics

  override val txIdProvider = LastCommittedTxIdProvider(tc.graph)

  override def procedureSignature(name: QualifiedName) = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = tc.kernelTransaction.procedures()
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

    ProcedureSignature(name, input, output, deprecationInfo, mode, description, warning, signature.eager(), Some(handle.id()))
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = tc.kernelTransaction.procedures()
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
                                 signature.allowed(), description, isAggregate = aggregation, id = Some(fcn.id())))
    }
  }

  private def asOption[T](optional: Optional[T]): Option[T] = if (optional.isPresent) Some(optional.get()) else None

  private def asCypherProcMode(mode: Mode, allowed: Array[String]): ProcedureAccessMode = mode match {
    case Mode.READ => ProcedureReadOnlyAccess(allowed)
    case Mode.DEFAULT => ProcedureReadOnlyAccess(allowed)
    case Mode.WRITE => ProcedureReadWriteAccess(allowed)
    case Mode.SCHEMA => ProcedureSchemaWriteAccess(allowed)
    case Mode.DBMS => ProcedureDbmsAccess(allowed)

    case _ => throw new CypherExecutionException(
      "Unable to execute procedure, because it requires an unrecognized execution mode: " + mode.name(), null)
  }

  private def asCypherValue(neo4jValue: DefaultParameterValue) = CypherValue(neo4jValue.value,
                                                                  asCypherType(neo4jValue.neo4jType()))

  private def asCypherType(neoType: AnyType): CypherType = neoType match {
    case Neo4jTypes.NTString => CTString
    case Neo4jTypes.NTInteger => CTInteger
    case Neo4jTypes.NTFloat => CTFloat
    case Neo4jTypes.NTNumber => CTNumber
    case Neo4jTypes.NTBoolean => CTBoolean
    case l: Neo4jTypes.ListType => CTList(asCypherType(l.innerType()))
    case Neo4jTypes.NTByteArray => CTList(CTAny)
    case Neo4jTypes.NTDateTime => CTDateTime
    case Neo4jTypes.NTLocalDateTime => CTLocalDateTime
    case Neo4jTypes.NTDate => CTDate
    case Neo4jTypes.NTTime => CTTime
    case Neo4jTypes.NTLocalTime => CTLocalTime
    case Neo4jTypes.NTDuration => CTDuration
    case Neo4jTypes.NTPoint => CTPoint
    case Neo4jTypes.NTNode => CTNode
    case Neo4jTypes.NTRelationship => CTRelationship
    case Neo4jTypes.NTPath => CTPath
    case Neo4jTypes.NTGeometry => CTGeometry
    case Neo4jTypes.NTMap => CTMap
    case Neo4jTypes.NTAny => CTAny
  }

  override def notificationLogger(): InternalNotificationLogger = logger
}
