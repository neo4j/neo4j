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
import org.neo4j.cypher.internal.ast.AbstractFieldSignature
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Value

case class ProcedureSignature(
  name: ProcedureName,
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
    outputSignature.map(out => s"${name.fullName}($sig) :: ${out.mkString(", ")}").getOrElse(s"${name.fullName}($sig)")
  }
}

case class UserFunctionSignature(
  name: FunctionName,
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
    s"${name.fullName}(${inputSignature.mkString(", ")}) :: ${outputType.normalizedCypherTypeString()}"
}

// Should have one to one mapping with org.neo4j.kernel.api.QueryLanguage
sealed trait QueryLanguage

object QueryLanguage {
  case object Cypher5 extends QueryLanguage
  case object Cypher25 extends QueryLanguage
  val All: Set[QueryLanguage] = Set(Cypher5, Cypher25)

  def from(version: CypherVersion): QueryLanguage = version match {
    case CypherVersion.Cypher5  => QueryLanguage.Cypher5
    case CypherVersion.Cypher25 => QueryLanguage.Cypher25
  }

  def toCypherVersion(scope: QueryLanguage): CypherVersion = scope match {
    case QueryLanguage.Cypher5  => CypherVersion.Cypher5
    case QueryLanguage.Cypher25 => CypherVersion.Cypher25
  }

  def otherVersion(scope: QueryLanguage): QueryLanguage = scope match {
    case QueryLanguage.Cypher5  => QueryLanguage.Cypher25
    case QueryLanguage.Cypher25 => QueryLanguage.Cypher5
  }

  def toKernelScope(scope: QueryLanguage): org.neo4j.kernel.api.QueryLanguage = scope match {
    case QueryLanguage.Cypher5  => org.neo4j.kernel.api.QueryLanguage.CYPHER_5
    case QueryLanguage.Cypher25 => org.neo4j.kernel.api.QueryLanguage.CYPHER_25
  }
  def toKernelScope(version: CypherVersion): org.neo4j.kernel.api.QueryLanguage = toKernelScope(from(version))
}

case class FieldSignature(
  override val name: String,
  typ: CypherType,
  default: Option[AnyValue] = None,
  deprecated: Boolean = false,
  sensitive: Boolean = false,
  description: String = null
) extends AbstractFieldSignature {

  /**
   * Returns value of `typ`.
   */
  override def getType: CypherType = typ

  def hasDefault: Boolean = default.nonEmpty

  override def toString: String = {
    val nameValue = default.map(d => s"$name  =  ${stringOf(d)}").getOrElse(name)
    s"$nameValue :: ${typ.normalizedCypherTypeString()}"
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
