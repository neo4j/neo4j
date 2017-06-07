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
package org.neo4j.cypher.internal.spi.v3_2

import java.util.Optional

import org.neo4j.cypher.MissingIndexException
import org.neo4j.cypher.internal.LastCommittedTxIdProvider
import org.neo4j.cypher.internal.compiler.v3_2.IndexDescriptor
import org.neo4j.cypher.internal.compiler.v3_2.spi._
import org.neo4j.cypher.internal.frontend.v3_2.phases.InternalNotificationLogger
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CypherType
import org.neo4j.cypher.internal.frontend.v3_2.{CypherExecutionException, symbols}
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.index.InternalIndexState
import org.neo4j.kernel.api.proc.Neo4jTypes.AnyType
import org.neo4j.kernel.api.proc.{Neo4jTypes, QualifiedName => KernelQualifiedName}
import org.neo4j.kernel.api.schema.SchemaDescriptorFactory
import org.neo4j.kernel.api.schema.index.{IndexDescriptor => KernelIndexDescriptor}
import org.neo4j.kernel.impl.proc.Neo4jValue
import org.neo4j.procedure.Mode

import scala.collection.JavaConverters._

class TransactionBoundPlanContext(tc: TransactionalContextWrapper, logger: InternalNotificationLogger)
  extends TransactionBoundTokenContext(tc.statement) with PlanContext with IndexDescriptorCompatibility {

  def indexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    tc.statement.readOperations().indexesGetForLabel(labelId).asScala
      .filter(_.`type`() == KernelIndexDescriptor.Type.GENERAL)
      .flatMap(getOnlineIndex)
  }

  def indexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = evalOrNone {
    val descriptor = toLabelSchemaDescriptor(this, labelName, propertyKeys)
    getOnlineIndex(tc.statement.readOperations().indexGetForSchema(descriptor))
  }

  def indexExistsForLabel(labelName: String): Boolean = {
    val labelId = getLabelId(labelName)

    val onlineIndexDescriptors = tc.statement.readOperations().indexesGetForLabel(labelId).asScala
                                  .filter(_.`type`() == KernelIndexDescriptor.Type.GENERAL)
                                  .flatMap(getOnlineIndex)

    onlineIndexDescriptors.nonEmpty
  }

  def uniqueIndexesGetForLabel(labelId: Int): Iterator[IndexDescriptor] = {
    tc.statement.readOperations().indexesGetForLabel(labelId).asScala
      .filter(_.`type`() == KernelIndexDescriptor.Type.UNIQUE)
      .flatMap(getOnlineIndex)
  }

  def uniqueIndexGet(labelName: String, propertyKeys: Seq[String]): Option[IndexDescriptor] = evalOrNone {
    val descriptor = toLabelSchemaDescriptor(this, labelName, propertyKeys)
    getOnlineIndex(tc.statement.readOperations().indexGetForSchema(descriptor))
  }

  private def evalOrNone[T](f: => Option[T]): Option[T] =
    try {
      f
    } catch {
      case _: KernelException => None
    }

  private def getOnlineIndex(descriptor: KernelIndexDescriptor): Option[IndexDescriptor] =
    tc.statement.readOperations().indexGetState(descriptor) match {
      case InternalIndexState.ONLINE => Some(IndexDescriptor(descriptor.schema().getLabelId, descriptor.schema().getPropertyIds))
      case _ => None
    }

  override def hasPropertyExistenceConstraint(labelName: String, propertyKey: String): Boolean = {
    constraintGet(labelName, propertyKey).isDefined
  }

  private def constraintGet(labelName: String, propertyKey: String): Option[Boolean] = evalOrNone {
    val labelId = getLabelId(labelName)
    val propertyKeyId = getPropertyKeyId(propertyKey)

    Some(tc.statement.readOperations().constraintsGetForSchema(SchemaDescriptorFactory.forLabel(labelId, propertyKeyId)).hasNext)
  }

  def checkNodeIndex(idxName: String) {
    if (!tc.statement.readOperations().nodeLegacyIndexesGetAll().contains(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def checkRelIndex(idxName: String) {
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

  val statistics: GraphStatistics =
    InstrumentedGraphStatistics(TransactionBoundGraphStatistics(tc.readOperations),
                                new MutableGraphStatisticsSnapshot())

  val txIdProvider = LastCommittedTxIdProvider(tc.graph)

  override def procedureSignature(name: QualifiedName) = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val ks = tc.statement.readOperations().procedureGet(kn)
    val input = ks.inputSignature().asScala
      .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
      .toIndexedSeq
    val output = if (ks.isVoid) None else Some(
      ks.outputSignature().asScala.map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), deprecated = s.isDeprecated)).toIndexedSeq)
    val deprecationInfo = asOption(ks.deprecated())
    val mode = asCypherProcMode(ks.mode(), ks.allowed())
    val description = asOption(ks.description())
    val warning = asOption(ks.warning())

    ProcedureSignature(name, input, output, deprecationInfo, mode, description, warning)
  }

  override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] = {
    val kn = new KernelQualifiedName(name.namespace.asJava, name.name)
    val maybeFunction = tc.statement.readOperations().functionGet(kn)
    val (fcn, aggregation) = if (maybeFunction.isPresent) (Some(maybeFunction.get), false)
    else (asOption(tc.statement.readOperations().aggregationFunctionGet(kn)), true)
    fcn.map(f => {
      val input = f.inputSignature().asScala
        .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue)))
        .toIndexedSeq
      val output = asCypherType(f.outputType())
      val deprecationInfo = asOption(f.deprecated())
      val description = asOption(f.description())

      UserFunctionSignature(name, input, output, deprecationInfo, f.allowed(), description, isAggregate = aggregation)
    })
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

  private def asCypherValue(neo4jValue: Neo4jValue) = CypherValue(neo4jValue.value,
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
  }

  override def notificationLogger(): InternalNotificationLogger = logger
}
