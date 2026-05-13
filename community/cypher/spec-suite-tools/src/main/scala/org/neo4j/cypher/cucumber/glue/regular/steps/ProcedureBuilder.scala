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
package org.neo4j.cypher.cucumber.glue.regular.steps

import cypher.features.ProcedureSignature
import cypher.features.ProcedureSignatureParser
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
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.kernel.api.QueryLanguage
import org.neo4j.kernel.api.ResourceMonitor
import org.neo4j.kernel.api.procedure.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue

object ProcedureBuilder {

  def createProcedure(signatureString: String, output: Array[Array[AnyValue]]): BasicProcedure =
    createProcedure(signatureString, output, Set(QueryLanguage.CYPHER_5, QueryLanguage.CYPHER_25))

  def createProcedure(
    signatureString: String,
    output: Array[Array[AnyValue]],
    supportedLanguages: Set[QueryLanguage]
  ): BasicProcedure = {
    val signature = asKernelSignature(new ProcedureSignatureParser().parse(signatureString), supportedLanguages)
    new FeatureTestProcedure(signature, output)
  }

  private def asKernelSignature(
    parsedSignature: ProcedureSignature,
    supportedLanguages: Set[QueryLanguage]
  ): procs.ProcedureSignature = {
    val builder = procs.ProcedureSignature.procedureSignature(new QualifiedName(
      parsedSignature.namespace.toArray,
      parsedSignature.name
    ))
    builder.mode(Mode.READ)
    builder.supportedQueryLanguages(supportedLanguages.toSeq: _*)
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

class FeatureTestProcedure(
  signature: org.neo4j.internal.kernel.api.procs.ProcedureSignature,
  output: Array[Array[AnyValue]]
) extends BasicProcedure(signature) {

  override def apply(
    ctx: Context,
    input: Array[AnyValue],
    resourceMonitor: ResourceMonitor
  ): ResourceRawIterator[Array[AnyValue], ProcedureException] = {
    val rows =
      if (input.isEmpty) output
      else output.filter(_.startsWith(input)).map(_.drop(input.length))
    ResourceRawIterator.of(rows: _*)
  }
}
