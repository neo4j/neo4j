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
package org.neo4j.cypher.internal.logical.plans

import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

case class ProcedureSignature(
  name: QualifiedName,
  inputSignature: IndexedSeq[FieldSignature],
  outputSignature: Option[IndexedSeq[FieldSignature]],
  deprecationInfo: Option[String],
  accessMode: ProcedureAccessMode,
  description: Option[String] = None,
  warning: Option[String] = None,
  eager: Boolean = false,
  id: Int,
  systemProcedure: Boolean = false,
  allowExpiredCredentials: Boolean = false,
  threadSafe: Boolean = true
) {

  def outputFields: Seq[FieldSignature] = outputSignature.getOrElse(Seq.empty)

  def isVoid: Boolean = outputSignature.isEmpty

  override def toString: String = {
    val sig = inputSignature.mkString(", ")
    outputSignature.map(out => s"$name($sig) :: ${out.mkString(", ")}").getOrElse(s"$name($sig)")
  }
}

case class UserFunctionSignature(
  name: QualifiedName,
  inputSignature: IndexedSeq[FieldSignature],
  outputType: CypherType,
  deprecationInfo: Option[String],
  description: Option[String],
  isAggregate: Boolean,
  id: Int,
  builtIn: Boolean,
  threadSafe: Boolean = false
) {

  override def toString =
    s"$name(${inputSignature.mkString(", ")}) :: ${outputType.normalizedCypherTypeString()}"
}

object QualifiedName {

  def apply(unresolved: UnresolvedCall): QualifiedName =
    QualifiedName(unresolved.procedureNamespace.parts, unresolved.procedureName.name)

  def apply(unresolved: FunctionInvocation): QualifiedName =
    QualifiedName(unresolved.namespace.parts, unresolved.functionName.name)
}

case class QualifiedName(namespace: Seq[String], name: String) {
  override def toString: String = (namespace :+ name).mkString(".")
}

case class FieldSignature(
  name: String,
  typ: CypherType,
  default: Option[AnyValue] = None,
  deprecated: Boolean = false,
  sensitive: Boolean = false
) {

  override def toString: String = {
    val nameValue = default.map(d => s"$name  =  ${stringOf(d)}").getOrElse(name)
    s"$nameValue :: ${typ.normalizedCypherTypeString()}"
  }

  private def stringOf(any: AnyValue) = any match {
    case v: Value => v.prettyPrint()
    case _        => any.toString
  }
}

sealed trait ProcedureAccessMode
case object ProcedureReadOnlyAccess extends ProcedureAccessMode
case object ProcedureReadWriteAccess extends ProcedureAccessMode
case object ProcedureSchemaWriteAccess extends ProcedureAccessMode
case object ProcedureDbmsAccess extends ProcedureAccessMode
