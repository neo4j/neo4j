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
package org.neo4j.cypher.internal.spi.v3_1

import java.util.Optional

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.compiler.v3_1.InternalNotificationLogger
import org.neo4j.cypher.internal.compiler.v3_1.pipes.EntityProducer
import org.neo4j.cypher.internal.compiler.v3_1.pipes.matching.ExpanderStep
import org.neo4j.cypher.internal.compiler.v3_1.spi._
import org.neo4j.cypher.internal.frontend.v3_1.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v3_1.{CypherExecutionException, symbols}
import org.neo4j.cypher.internal.runtime.interpreted.LastCommittedTxIdProvider
import org.neo4j.graphdb.Node
import org.neo4j.internal.kernel.api.exceptions.KernelException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.{DefaultParameterValue, Neo4jTypes}
import org.neo4j.internal.kernel.api.{IndexReference, InternalIndexState, procs}
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema.constaints.ConstraintDescriptor
import org.neo4j.kernel.api.schema.index.{SchemaIndexDescriptor => KernelIndexDescriptor}
import org.neo4j.procedure.Mode

import scala.collection.JavaConverters._

class TransactionBoundPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger)
  extends TransactionBoundTokenContext(tc.kernelTransaction) with PlanContext with SchemaDescriptorTranslation {

  @Deprecated
  def getIndexRule(labelName: String, propertyKey: String): Option[SchemaTypes.IndexDescriptor] = evalOrNone {
    val labelId = getLabelId(labelName)
    val propertyKeyId = getPropertyKeyId(propertyKey)

    getOnlineIndex(tc.kernelTransaction.schemaRead.index(labelId, propertyKeyId))
  }


  def hasIndexRule(labelName: String): Boolean = {
    val labelId = getLabelId(labelName)

    val indexDescriptors = tc.kernelTransaction.schemaRead().indexesGetForLabel(labelId).asScala
    val onlineIndexDescriptors = indexDescriptors.flatMap(getOnlineIndex)

    onlineIndexDescriptors.nonEmpty
  }

  def getUniqueIndexRule(labelName: String, propertyKey: String): Option[SchemaTypes.IndexDescriptor] = evalOrNone {
    val labelId = getLabelId(labelName)
    val propertyKeyId = getPropertyKeyId(propertyKey)
    val ref = tc.kernelTransaction.schemaRead.index(labelId, propertyKeyId)

    if (ref.isUnique) getOnlineIndex(ref)
    else None
  }


  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try { f } catch { case _: KernelException => None }

  private def getOnlineIndex(descriptor: IndexReference): Option[SchemaTypes.IndexDescriptor] =
    tc.kernelTransaction.schemaRead.indexGetState(descriptor) match {
      case InternalIndexState.ONLINE => Some(SchemaTypes.IndexDescriptor(descriptor.label(), descriptor.properties()(0)))
      case _                         => None
    }

  def getUniquenessConstraint(labelName: String, propertyKey: String): Option[SchemaTypes.UniquenessConstraint] = evalOrNone {
    val labelId = getLabelId(labelName)
    val propertyKeyId = getPropertyKeyId(propertyKey)

    import scala.collection.JavaConverters._
    tc.kernelTransaction.schemaRead.constraintsGetForSchema(
      SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)
    ).asScala.collectFirst {
      case constraint: ConstraintDescriptor if constraint.enforcesUniqueness() =>
        SchemaTypes.UniquenessConstraint(labelId, propertyKeyId)
    }
  }

  override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
    try {
      val labelId = getLabelId(labelName)
      val propId = getPropertyKeyId(propertyKey)
      tc.kernelTransaction.schemaRead().constraintsGetForSchema(SchemaDescriptorFactory.forLabel(labelId, propId)).hasNext
    } catch {
      case _: KernelException => false
    }
  }

  def checkNodeIndex(idxName: String) {
    if (!tc.kernelTransaction.indexRead().nodeExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def checkRelIndex(idxName: String)  {
    if (!tc.kernelTransaction.indexRead().relationshipExplicitIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def getOrCreateFromSchemaState[T](key: Any, f: => T): T = {
    val javaCreator = new java.util.function.Function[Any, T]() {
      def apply(key: Any) = f
    }
    tc.kernelTransaction.schemaRead().schemaStateGetOrCreate(key, javaCreator)
  }


  // Legacy traversal matchers (pre-Ronja) (These were moved out to remove the dependency on the kernel)
  override def monoDirectionalTraversalMatcher(steps: ExpanderStep, start: EntityProducer[Node]) =
    new MonoDirectionalTraversalMatcher(steps, start)

  override def bidirectionalTraversalMatcher(steps: ExpanderStep,
                                             start: EntityProducer[Node],
                                             end: EntityProducer[Node]) =
    new BidirectionalTraversalMatcher(steps, start, end)

  val statistics: GraphStatistics =
    InstrumentedGraphStatistics(TransactionBoundGraphStatistics(tc.kernelTransaction.dataRead(), tc.kernelTransaction.schemaRead()), new MutableGraphStatisticsSnapshot())

  val txIdProvider = LastCommittedTxIdProvider(tc.graph)

  override def procedureSignature(name: QualifiedName) = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
    val procedures = tc.tc.kernelTransaction().procedures()
    val handle = procedures.procedureGet(kn)
    val signature = handle.signature()
    val input = signature.inputSignature().asScala
      .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
      .toIndexedSeq
    val output = if (signature.isVoid) None else Some(
      signature.outputSignature().asScala.map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()))).toIndexedSeq)
    val deprecationInfo = asOption(signature.deprecated())
    val mode = asCypherProcMode(signature.mode(), signature.allowed())
    val description = asOption(signature.description())

    ProcedureSignature(name, input, output, deprecationInfo, mode, description)
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val kn = new procs.QualifiedName(name.namespace.asJava, name.name)
  val procedures = tc.tc.kernelTransaction().procedures()
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
                               signature.allowed(), description))
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
      "Unable to execute procedure, because it requires an unrecognized execution mode: " + mode.name(), null )
  }

  private def asCypherValue(neo4jValue: DefaultParameterValue) = CypherValue(neo4jValue.value, asCypherType(neo4jValue.neo4jType()))

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
    case Neo4jTypes.NTDateTime => symbols.CTAny
    case Neo4jTypes.NTLocalDateTime => symbols.CTAny
    case Neo4jTypes.NTDate => symbols.CTAny
    case Neo4jTypes.NTTime => symbols.CTAny
    case Neo4jTypes.NTLocalTime => symbols.CTAny
    case Neo4jTypes.NTDuration => symbols.CTAny
  }

  override def notificationLogger(): InternalNotificationLogger = logger
}
