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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.compiler.planner.ProcedureTestSupport.FunctionSignatureBuilder
import org.neo4j.cypher.internal.compiler.planner.ProcedureTestSupport.ProcedureSignatureBuilder
import org.neo4j.cypher.internal.frontend.phases.DeprecationInfo
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureAccessMode
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CypherType

trait ProcedureTestSupport {
  def procedureSignature(qualifiedName: String): ProcedureSignatureBuilder = ProcedureSignatureBuilder(qualifiedName)
  def functionSignature(qualifiedName: String): FunctionSignatureBuilder = FunctionSignatureBuilder(qualifiedName)
}

object ProcedureTestSupport {

  case class ProcedureSignatureBuilder(
    qualifiedName: String,
    inputSignature: IndexedSeq[FieldSignature] = IndexedSeq(),
    outputSignature: Option[IndexedSeq[FieldSignature]] = None,
    deprecationInfo: Option[String] = None,
    accessMode: ProcedureAccessMode = ProcedureReadOnlyAccess
  ) {

    def withInputField(name: String, inputType: CypherType): ProcedureSignatureBuilder =
      copy(inputSignature = inputSignature :+ FieldSignature(name, inputType))

    def withOutputField(name: String, inputType: CypherType): ProcedureSignatureBuilder =
      copy(outputSignature = Some(outputSignature.getOrElse(IndexedSeq.empty) :+ FieldSignature(name, inputType)))

    def withAccessMode(accessMode: ProcedureAccessMode): ProcedureSignatureBuilder =
      copy(accessMode = accessMode)

    def build(): ProcedureSignature = {
      val splitName = qualifiedName.split("\\.")

      ProcedureSignature(
        name = QualifiedName(splitName.init.toSeq, splitName.last),
        inputSignature = inputSignature,
        outputSignature = outputSignature,
        deprecationInfo = Some(DeprecationInfo(deprecationInfo.isDefined, deprecationInfo)),
        accessMode = accessMode,
        id = 1
      )
    }
  }

  case class FunctionSignatureBuilder(
    qualifiedName: String,
    inputSignature: IndexedSeq[FieldSignature] = IndexedSeq(),
    outputType: CypherType = CTAny,
    deprecationInfo: Option[String] = None,
    isAggregate: Boolean = false
  ) {

    def withInputField(name: String, inputType: CypherType): FunctionSignatureBuilder =
      copy(inputSignature = inputSignature :+ FieldSignature(name, inputType))

    def withOutputType(outputType: CypherType): FunctionSignatureBuilder =
      copy(outputType = outputType)

    def build(): UserFunctionSignature = {
      val splitName = qualifiedName.split("\\.")

      UserFunctionSignature(
        name = QualifiedName(splitName.init.toSeq, splitName.last),
        inputSignature = inputSignature,
        outputType = outputType,
        deprecationInfo = Some(DeprecationInfo(deprecationInfo.isDefined, deprecationInfo)),
        description = None,
        isAggregate = isAggregate,
        id = 1,
        builtIn = true
      )
    }
  }
}
