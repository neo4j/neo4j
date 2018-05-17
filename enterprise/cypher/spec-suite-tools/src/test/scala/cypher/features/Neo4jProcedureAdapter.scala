/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.features

import org.neo4j.collection.RawIterator
import org.opencypher.v9_0.util.symbols.{CTBoolean, CTFloat, CTInteger, CTMap, CTNode, CTNumber, CTPath, CTRelationship, CTString, CypherType, ListType}
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.kernel.api.proc.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.proc.Context
import org.neo4j.kernel.api.{InwardKernel, ResourceTracker}
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Mode
import org.opencypher.tools.tck.api.{CypherValueRecords, Graph, ProcedureSupport}
import org.opencypher.tools.tck.values.CypherValue

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class Neo4jValueRecords(header: List[String], rows: List[Array[AnyRef]])

object Neo4jValueRecords {
  def apply(record: CypherValueRecords): Neo4jValueRecords = {
    val tableValues = record.rows.map {
      row: Map[String, CypherValue] =>
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

  protected def instance: GraphDatabaseAPI

  protected val parser = new ProcedureSignatureParser

  override def registerProcedure(signature: String, values: CypherValueRecords): Unit = {
    val parsedSignature = parser.parse(signature)
    val kernelProcedure = buildProcedure(parsedSignature, values)
    Try(instance.getDependencyResolver.resolveDependency(classOf[InwardKernel]).registerProcedure(kernelProcedure)) match {
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
    val kernelProcedure = new BasicProcedure(kernelSignature) {
      override def apply(ctx: Context,
                         input: Array[AnyRef],
                         resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] = {
        // For example of usage see ProcedureCallAcceptance.feature e.g. "Standalone call to procedure with explicit arguments"
        val rowsWithMatchingInput = neo4jValues.rows.filter { row =>
          row.startsWith(input)
        }
        val extractResultsFromRows = rowsWithMatchingInput.map { row =>
          row.drop(input.length)
        }

        val rawIterator = RawIterator.wrap[Array[AnyRef], ProcedureException](extractResultsFromRows.toIterator.asJava)
        rawIterator
      }
    }
    kernelProcedure
  }

  private def asKernelSignature(parsedSignature: ProcedureSignature): procs.ProcedureSignature = {
    val builder = procs.ProcedureSignature.procedureSignature(parsedSignature.namespace.toArray, parsedSignature.name)
    builder.mode(Mode.READ)
    parsedSignature.inputs.foreach { case (name, tpe) => builder.in(name, asKernelType(tpe)) }
    parsedSignature.outputs match {
      case Some(fields) => fields.foreach { case (name, tpe) => builder.out(name, asKernelType(tpe)) }
      case None => builder.out(procs.ProcedureSignature.VOID)
    }
    builder.build()
  }

  private def asKernelType(tpe: CypherType): Neo4jTypes.AnyType = tpe match {
    case CTMap => Neo4jTypes.NTMap
    case CTNode => Neo4jTypes.NTNode
    case CTRelationship => Neo4jTypes.NTRelationship
    case CTPath => Neo4jTypes.NTPath
    case ListType(innerTpe) => Neo4jTypes.NTList(asKernelType(innerTpe))
    case CTString => Neo4jTypes.NTString
    case CTBoolean => Neo4jTypes.NTBoolean
    case CTNumber => Neo4jTypes.NTNumber
    case CTInteger => Neo4jTypes.NTInteger
    case CTFloat => Neo4jTypes.NTFloat
  }
}
