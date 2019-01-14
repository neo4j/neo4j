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
package org.neo4j.cypher.internal.spi.v3_3

import java.util.Optional

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.compiler.v3_3.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_3.spi._
import org.neo4j.cypher.internal.frontend.v3_3.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v3_3.{CypherExecutionException, symbols}
import org.neo4j.cypher.internal.v3_3.logical.plans._
import org.neo4j.internal.kernel.api.exceptions.KernelException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.{DefaultParameterValue, Neo4jTypes}
import org.neo4j.internal.kernel.api.{IndexReference, InternalIndexState, procs}
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.procedure.Mode

import scala.collection.JavaConverters._

class TransactionBoundPlanContext(txSupplier: () => KernelTransaction, logger: InternalNotificationLogger, graphStatistics: GraphStatistics)
  extends TransactionBoundTokenContext(txSupplier) with PlanContext with IndexDescriptorCompatibility {

  def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    txSupplier().schemaRead().indexesGetForLabel(labelId).asScala
        .filterNot(_.isUnique)
      .flatMap(getOnlineIndex)
  }

  def indexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = evalOrNone {
    val descriptor = toLabelSchemaDescriptor(this, labelName, propertyKeys)
    getOnlineIndex(txSupplier().schemaRead.index(descriptor.getLabelId, descriptor.getPropertyIds:_*))
  }

  def indexExistsForLabel(labelName: String): Boolean = {
    try {
      val labelId = getLabelId(labelName)
      val onlineIndexDescriptors = txSupplier().schemaRead.indexesGetForLabel(labelId).asScala
        .filterNot(_.isUnique)
        .flatMap(getOnlineIndex)

      onlineIndexDescriptors.nonEmpty
    } catch {
      case _: KernelException => false
    }
  }

  def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    txSupplier().schemaRead.indexesGetForLabel(labelId).asScala
      .filter(_.isUnique)
      .flatMap(getOnlineIndex)
  }

  def uniqueIndexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = evalOrNone {
    val descriptor = toLabelSchemaDescriptor(this, labelName, propertyKeys)
    getOnlineIndex(txSupplier().schemaRead.index(descriptor.getLabelId, descriptor.getPropertyIds:_*))
  }

  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try {
      f
    } catch {
      case _: KernelException => None
    }

  private def getOnlineIndex(reference: IndexReference): Option[IndexDescriptor] =
    txSupplier().schemaRead.indexGetState(reference) match {
      case InternalIndexState.ONLINE => Some(IndexDescriptor(reference.label(), reference.properties()))
      case _ => None
    }
  override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
    try {
      val labelId = getLabelId(labelName)
      val propertyKeyId = getPropertyKeyId(propertyKey)

      txSupplier().schemaRead.constraintsGetForSchema(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)).hasNext
    } catch {
      case _: KernelException => false
    }
  }

  def checkNodeIndex(idxName: String) {
    if (!txSupplier().indexRead().nodeExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def checkRelIndex(idxName: String) {
    if (!txSupplier().indexRead().relationshipExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def getOrCreateFromSchemaState[T](key: Any, f: => T): T = {
    val javaCreator = new java.util.function.Function[Any, T]() {
      def apply(key: Any) = f
    }
    txSupplier().schemaRead.schemaStateGetOrCreate(key, javaCreator)
  }

  val statistics: GraphStatistics = graphStatistics

  // This should never be used in 3.4 code, because the txIdProvider will be used from 3.4 context in v3_3/Compatibility
  def txIdProvider: () => Long = ???

  override def procedureSignature(name: QualifiedName) = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = txSupplier().procedures()
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

    // TODO: Add a trailing argument `ks.eager()` after upgrading to the next 3.3 release
    //ProcedureSignature(name, input, output, deprecationInfo, mode, description, warning, ks.eager())
    ProcedureSignature(name, input, output, deprecationInfo, mode, description, warning)
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = txSupplier().procedures()
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
                                 signature.allowed(), description, isAggregate = aggregation))
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
    case Neo4jTypes.NTString => symbols.CTString
    case Neo4jTypes.NTInteger => symbols.CTInteger
    case Neo4jTypes.NTFloat => symbols.CTFloat
    case Neo4jTypes.NTNumber => symbols.CTNumber
    case Neo4jTypes.NTBoolean => symbols.CTBoolean
    case l: Neo4jTypes.ListType => symbols.CTList(asCypherType(l.innerType()))
    case Neo4jTypes.NTPoint => symbols.CTPoint
    case Neo4jTypes.NTNode => symbols.CTNode
    case Neo4jTypes.NTRelationship => symbols.CTRelationship
    case Neo4jTypes.NTPath => symbols.CTPath
    case Neo4jTypes.NTGeometry => symbols.CTGeometry
    case Neo4jTypes.NTMap => symbols.CTMap
    case Neo4jTypes.NTAny => symbols.CTAny
    case Neo4jTypes.NTByteArray => symbols.CTList(symbols.CTAny)
    case Neo4jTypes.NTDateTime => symbols.CTAny
    case Neo4jTypes.NTLocalDateTime => symbols.CTAny
    case Neo4jTypes.NTDate => symbols.CTAny
    case Neo4jTypes.NTTime => symbols.CTAny
    case Neo4jTypes.NTLocalTime => symbols.CTAny
    case Neo4jTypes.NTDuration => symbols.CTAny
  }

  override def notificationLogger(): InternalNotificationLogger = logger
}
