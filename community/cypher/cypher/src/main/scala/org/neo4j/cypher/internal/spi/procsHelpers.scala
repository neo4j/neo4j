/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.spi

import java.util.Optional

import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.util.symbols.{CTAny, CTBoolean, CTDate, CTDateTime, CTDuration, CTFloat, CTGeometry, CTInteger, CTList, CTLocalDateTime, CTLocalTime, CTMap, CTNode, CTNumber, CTPath, CTPoint, CTRelationship, CTString, CTTime, CypherType}
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.internal.kernel.api.procs.{DefaultParameterValue, Neo4jTypes}
import org.neo4j.procedure.Mode

import scala.collection.JavaConverters._

object procsHelpers {

  def asOption[T](optional: Optional[T]): Option[T] = if (optional.isPresent) Some(optional.get()) else None

  def asCypherProcMode(mode: Mode, allowed: Array[String]): ProcedureAccessMode = mode match {
    case Mode.READ => ProcedureReadOnlyAccess(allowed)
    case Mode.DEFAULT => ProcedureReadOnlyAccess(allowed)
    case Mode.WRITE => ProcedureReadWriteAccess(allowed)
    case Mode.SCHEMA => ProcedureSchemaWriteAccess(allowed)
    case Mode.DBMS => ProcedureDbmsAccess(allowed)

    case _ => throw new CypherExecutionException(
      "Unable to execute procedure, because it requires an unrecognized execution mode: " + mode.name(), null)
  }

  def asCypherValue(neo4jValue: DefaultParameterValue): CypherValue = CypherValue(neo4jValue.value, asCypherType(neo4jValue.neo4jType()))

  def asCypherType(neoType: AnyType): CypherType = neoType match {
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

  def asCypherProcedureSignature(name: QualifiedName, id: Int, signature: org.neo4j.internal.kernel.api.procs.ProcedureSignature): ProcedureSignature = {
    val input = signature.inputSignature().asScala
      .map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), asOption(s.defaultValue()).map(asCypherValue), sensitive = s.isSensitive))
      .toIndexedSeq
    val output = if (signature.isVoid) None else Some(
      signature.outputSignature().asScala.map(s => FieldSignature(s.name(), asCypherType(s.neo4jType()), deprecated = s.isDeprecated)).toIndexedSeq)
    val deprecationInfo = asOption(signature.deprecated())
    val mode = asCypherProcMode(signature.mode(), signature.allowed())
    val description = asOption(signature.description())
    val warning = asOption(signature.warning())

    ProcedureSignature(name, input, output, deprecationInfo, mode, description, warning, signature.eager(), id, signature.systemProcedure())
  }
}
