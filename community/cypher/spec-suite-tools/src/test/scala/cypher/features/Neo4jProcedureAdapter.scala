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
package cypher.features

import org.neo4j.collection.ResourceRawIterator
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.testing.impl.FeatureDatabaseManagementService
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.kernel.api.ResourceMonitor
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue
import org.opencypher.tools.tck.api.CypherValueRecords
import org.opencypher.tools.tck.api.Graph
import org.opencypher.tools.tck.api.ProcedureSupport
import org.opencypher.tools.tck.values.CypherValue

import scala.util.Failure
import scala.util.Success
import scala.util.Try

case class Neo4jValueRecords(header: List[String], rows: List[Array[AnyRef]])

object Neo4jValueRecords {

  def apply(record: CypherValueRecords): Neo4jValueRecords = {
    val tableValues = record.rows.map {
      (row: Map[String, CypherValue]) =>
        record.header.map { columnName =>
          val value = row(columnName)
          val convertedValue = TCKValueToNeo4jValue(value)
          convertedValue
        }.toArray
    }
    Neo4jValueRecords(record.header, tableValues)
  }
}

trait Neo4jProcedureAdapter extends ProcedureSupport {
  self: Graph =>

  protected def dbms: FeatureDatabaseManagementService

  protected val parser = new ProcedureSignatureParser

  override def registerProcedure(signature: String, values: CypherValueRecords): Unit = {
    val parsedSignature = parser.parse(signature)
    val kernelProcedure = buildProcedure(parsedSignature, values)
    Try(dbms.registerProcedure(kernelProcedure)) match {
      case Success(_) =>
      case Failure(e) => System.err.println(s"\nRegistration of procedure $signature failed: " + e.getMessage)
    }
  }

  private def buildProcedure(parsedSignature: ProcedureSignature, values: CypherValueRecords) = {
    val signatureFields = parsedSignature.fields
    val neo4jValues = Neo4jValueRecords(values)
    if (neo4jValues.header != signatureFields)
      throw new scala.IllegalArgumentException(
        s"Data table columns must be the same as all signature fields (inputs + outputs) in order (Actual: ${neo4jValues.rows} Expected: $signatureFields)"
      )
    val kernelSignature = asKernelSignature(parsedSignature)
    val rows = neo4jValues.rows.map(_.map(ValueUtils.of))
    val kernelProcedure = new BasicProcedure(kernelSignature) {
      override def apply(
        ctx: Context,
        input: Array[AnyValue],
        resourceMonitor: ResourceMonitor
      ): ResourceRawIterator[Array[AnyValue], ProcedureException] = {
        ResourceRawIterator.of(
          rows
            // For example of usage see ProcedureCallAcceptance.feature e.g. "Standalone call to procedure with explicit arguments"
            .filter(_.startsWith(input))
            .map(_.drop(input.length)): _*
        )
      }
    }
    kernelProcedure
  }

  private def asKernelSignature(parsedSignature: ProcedureSignature): procs.ProcedureSignature = {
    val builder = procs.ProcedureSignature.procedureSignature(new QualifiedName(
      parsedSignature.namespace.toArray,
      parsedSignature.name
    ))
    builder.mode(Mode.READ)
    parsedSignature.inputs.foreach { case (name, tpe) => builder.in(name, asKernelType(tpe)) }
    parsedSignature.outputs match {
      case Some(fields) => fields.foreach { case (name, tpe) => builder.out(name, asKernelType(tpe)) }
      case None         => builder.out(procs.ProcedureSignature.VOID)
    }
    builder.build()
  }

  private def asKernelType(tpe: CypherType): Neo4jTypes.AnyType = tpe match {
    case CTMap                 => Neo4jTypes.NTMap
    case CTNode                => Neo4jTypes.NTNode
    case CTRelationship        => Neo4jTypes.NTRelationship
    case CTPath                => Neo4jTypes.NTPath
    case ListType(innerTpe, _) => Neo4jTypes.NTList(asKernelType(innerTpe))
    case CTString              => Neo4jTypes.NTString
    case CTBoolean             => Neo4jTypes.NTBoolean
    case CTNumber              => Neo4jTypes.NTNumber
    case CTInteger             => Neo4jTypes.NTInteger
    case CTFloat               => Neo4jTypes.NTFloat
    case x                     => throw new InternalError(s"Unexpected CypherType ${x.getClass}")
  }
}
