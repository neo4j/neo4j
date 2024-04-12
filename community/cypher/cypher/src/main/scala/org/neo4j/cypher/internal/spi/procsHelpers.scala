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
package org.neo4j.cypher.internal.spi

import org.neo4j.cypher.internal.frontend.phases.DeprecationInfo
import org.neo4j.cypher.internal.frontend.phases.FieldSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureAccessMode
import org.neo4j.cypher.internal.frontend.phases.ProcedureDbmsAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSchemaWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDate
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGeometry
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTLocalDateTime
import org.neo4j.cypher.internal.util.symbols.CTLocalTime
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTPoint
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CTTime
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.internal.kernel.api.procs.DefaultParameterValue
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.Neo4jTypes.AnyType
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue

import java.util.Optional

import scala.jdk.CollectionConverters.ListHasAsScala

object procsHelpers {

  def asOption[T](optional: Optional[T]): Option[T] = if (optional.isPresent) Some(optional.get()) else None

  def asCypherProcMode(mode: Mode): ProcedureAccessMode = mode match {
    case Mode.READ    => ProcedureReadOnlyAccess
    case Mode.DEFAULT => ProcedureReadOnlyAccess
    case Mode.WRITE   => ProcedureReadWriteAccess
    case Mode.SCHEMA  => ProcedureSchemaWriteAccess
    case Mode.DBMS    => ProcedureDbmsAccess

    case _ => throw new CypherExecutionException(
        "Unable to execute procedure, because it requires an unrecognized execution mode: " + mode.name(),
        null
      )
  }

  def asCypherValue(neo4jValue: DefaultParameterValue): AnyValue = ValueUtils.of(neo4jValue.value())

  def asCypherType(neoType: AnyType): CypherType = neoType match {
    case Neo4jTypes.NTString        => CTString
    case Neo4jTypes.NTInteger       => CTInteger
    case Neo4jTypes.NTFloat         => CTFloat
    case Neo4jTypes.NTNumber        => CTNumber
    case Neo4jTypes.NTBoolean       => CTBoolean
    case l: Neo4jTypes.ListType     => CTList(asCypherType(l.innerType()))
    case Neo4jTypes.NTByteArray     => CTList(CTAny)
    case Neo4jTypes.NTDateTime      => CTDateTime
    case Neo4jTypes.NTLocalDateTime => CTLocalDateTime
    case Neo4jTypes.NTDate          => CTDate
    case Neo4jTypes.NTTime          => CTTime
    case Neo4jTypes.NTLocalTime     => CTLocalTime
    case Neo4jTypes.NTDuration      => CTDuration
    case Neo4jTypes.NTPoint         => CTPoint
    case Neo4jTypes.NTNode          => CTNode
    case Neo4jTypes.NTRelationship  => CTRelationship
    case Neo4jTypes.NTPath          => CTPath
    case Neo4jTypes.NTGeometry      => CTGeometry
    case Neo4jTypes.NTMap           => CTMap
    case Neo4jTypes.NTAny           => CTAny
    case t => throw new IllegalArgumentException(s"Could not find a valid mapping for the type $t")
  }

  def asCypherProcedureSignature(
    name: QualifiedName,
    id: Int,
    signature: org.neo4j.internal.kernel.api.procs.ProcedureSignature
  ): ProcedureSignature = {
    val input = signature.inputSignature().asScala
      .map(s =>
        FieldSignature(
          s.name(),
          asCypherType(s.neo4jType()),
          asOption(s.defaultValue()).map(asCypherValue),
          deprecated = s.isDeprecated,
          sensitive = s.isSensitive
        )
      )
      .toIndexedSeq
    val output =
      if (signature.isVoid) None
      else Some(
        signature.outputSignature().asScala.map(s =>
          FieldSignature(s.name(), asCypherType(s.neo4jType()), deprecated = s.isDeprecated, sensitive = s.isSensitive)
        ).toIndexedSeq
      )
    val deprecatedBy = asOption(signature.deprecated())
    val mode = asCypherProcMode(signature.mode())
    val description = asOption(signature.description())
    val warning = asOption(signature.warning())
    val threadSafe = signature.threadSafe()

    ProcedureSignature(
      name,
      input,
      output,
      Some(DeprecationInfo(signature.isDeprecated, deprecatedBy)),
      mode,
      description,
      warning,
      signature.eager(),
      id,
      signature.systemProcedure(),
      signature.allowedExpiredCredentials(),
      threadSafe
    )
  }
}
