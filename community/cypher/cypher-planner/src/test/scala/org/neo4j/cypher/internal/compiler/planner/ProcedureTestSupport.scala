/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner

import org.neo4j.cypher.internal.compiler.planner.ProcedureTestSupport.ProcedureSignatureBuilder
import org.neo4j.cypher.internal.logical.plans.FieldSignature
import org.neo4j.cypher.internal.logical.plans.ProcedureAccessMode
import org.neo4j.cypher.internal.logical.plans.ProcedureDbmsAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSchemaWriteAccess
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.procedure.Mode

trait ProcedureTestSupport {
  def procedureSignature(qualifiedName: String): ProcedureSignatureBuilder = ProcedureSignatureBuilder(qualifiedName)
}

object ProcedureTestSupport {

  case class ProcedureSignatureBuilder(
    qualifiedName: String,
    inputSignature: IndexedSeq[FieldSignature] = IndexedSeq(),
    outputSignature: Option[IndexedSeq[FieldSignature]] = None,
    deprecationInfo: Option[String] = None,
    accessMode: ProcedureAccessMode = ProcedureReadOnlyAccess(Array.empty),
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
        deprecationInfo = deprecationInfo,
        accessMode = accessMode,
        id = 1)
    }
  }
}
