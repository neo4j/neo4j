/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

case class ProcedureSignature(
  name: QualifiedName,
  inputSignature: IndexedSeq[FieldSignature],
  outputSignature: Option[IndexedSeq[FieldSignature]],
  deprecationInfo: Option[DeprecationInfo],
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
  deprecationInfo: Option[DeprecationInfo],
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
    QualifiedName(unresolved.functionName.namespace.parts, unresolved.functionName.name)
}

case class QualifiedName(namespace: Seq[String], name: String) {
  override def toString: String = (namespace :+ name).mkString(".")
}

// Should have one to one mapping with org.neo4j.kernel.api.CypherScope
sealed trait CypherScope

object CypherScope {
  case object Cypher5 extends CypherScope
  case object CypherFuture extends CypherScope
  val All: Set[CypherScope] = Set(Cypher5, CypherFuture)

  def from(version: CypherVersion): CypherScope = version match {
    case CypherVersion.Cypher5 => CypherScope.Cypher5
    case CypherVersion.Cypher6 => CypherScope.CypherFuture
  }

  def toKernelScope(scope: CypherScope): org.neo4j.kernel.api.CypherScope = scope match {
    case CypherScope.Cypher5      => org.neo4j.kernel.api.CypherScope.CYPHER_5
    case CypherScope.CypherFuture => org.neo4j.kernel.api.CypherScope.CYPHER_FUTURE
  }
  def toKernelScope(version: CypherVersion): org.neo4j.kernel.api.CypherScope = toKernelScope(from(version))
}

case class FieldSignature(
  name: String,
  typ: CypherType,
  default: Option[AnyValue] = None,
  deprecated: Boolean = false,
  sensitive: Boolean = false,
  description: String = null
) {

  override def toString: String = {
    if (description == null || description.isEmpty) {
      val nameValue = default.map(d => s"$name  =  ${stringOf(d)}").getOrElse(name)
      s"$nameValue :: ${typ.normalizedCypherTypeString()}"
    } else description
  }

  private def stringOf(any: AnyValue) = any match {
    case v: Value => v.prettyPrint()
    case _        => any.toString
  }
}

case class DeprecationInfo(
  isDeprecated: Boolean = false,
  deprecatedBy: Option[String] = None
)

sealed trait ProcedureAccessMode
case object ProcedureReadOnlyAccess extends ProcedureAccessMode
case object ProcedureReadWriteAccess extends ProcedureAccessMode
case object ProcedureSchemaWriteAccess extends ProcedureAccessMode
case object ProcedureDbmsAccess extends ProcedureAccessMode
