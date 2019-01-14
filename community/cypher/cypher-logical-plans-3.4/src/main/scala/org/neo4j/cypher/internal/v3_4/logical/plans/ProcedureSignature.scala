/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v3_4.logical.plans

import org.neo4j.cypher.internal.frontend.v3_4.ast.UnresolvedCall
import org.neo4j.cypher.internal.util.v3_4.symbols.CypherType
import org.neo4j.cypher.internal.v3_4.expressions.FunctionInvocation

case class ProcedureSignature(name: QualifiedName,
                              inputSignature: IndexedSeq[FieldSignature],
                              outputSignature: Option[IndexedSeq[FieldSignature]],
                              deprecationInfo: Option[String],
                              accessMode: ProcedureAccessMode,
                              description: Option[String] = None,
                              warning: Option[String] = None,
                              eager: Boolean = false,
                              id: Option[Int] = None) {

  def outputFields = outputSignature.getOrElse(Seq.empty)

  def isVoid = outputSignature.isEmpty

  override def toString = {
    val sig = inputSignature.mkString(", ")
    outputSignature.map(out => s"$name($sig) :: ${out.mkString(", ")}").getOrElse(s"$name($sig) :: VOID")
  }
}

case class UserFunctionSignature(name: QualifiedName,
                                 inputSignature: IndexedSeq[FieldSignature],
                                 outputType: CypherType,
                                 deprecationInfo: Option[String],
                                 allowed: Array[String],
                                 description: Option[String],
                                 isAggregate: Boolean,
                                 id: Option[Int] = None) {
  override def toString = s"$name(${inputSignature.mkString(", ")}) :: ${outputType.toNeoTypeString}"
}

object QualifiedName {
  def apply(unresolved: UnresolvedCall): QualifiedName =
    QualifiedName(unresolved.procedureNamespace.parts, unresolved.procedureName.name)

  def apply(unresolved: FunctionInvocation): QualifiedName =
    QualifiedName(unresolved.namespace.parts, unresolved.functionName.name)
}

case class QualifiedName(namespace: Seq[String], name: String) {
  override def toString = (namespace :+ name).mkString(".")
}

case class CypherValue(value: AnyRef, cypherType: CypherType)
case class FieldSignature(name: String, typ: CypherType, default: Option[CypherValue] = None, deprecated: Boolean = false) {
  override def toString = {
    val nameValue = default.map( d => s"$name  =  ${d.value}").getOrElse(name)
    s"$nameValue :: ${typ.toNeoTypeString}"
  }
}

sealed trait ProcedureAccessMode {
  def allowed: Array[String]

  override def hashCode() = this.allowed.toSet.hashCode()

  override def equals(obj: scala.Any) = {
    if(obj.getClass != this.getClass) {
      false
    } else {
      val other = obj.asInstanceOf[ProcedureAccessMode]
      this.allowed.toSet.equals(other.allowed.toSet)
    }
  }
}

case class ProcedureReadOnlyAccess(allowed: Array[String]) extends ProcedureAccessMode
case class ProcedureReadWriteAccess(allowed: Array[String]) extends ProcedureAccessMode
case class ProcedureSchemaWriteAccess(allowed: Array[String]) extends ProcedureAccessMode
case class ProcedureDbmsAccess(allowed: Array[String]) extends ProcedureAccessMode
