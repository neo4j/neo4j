/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.features

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTBoolean, CTFloat, CTInteger, CTMap, CTNode, CTNumber, CTPath, CTRelationship, CTString, CypherType, ListType}
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
