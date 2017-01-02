/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_0.spi

import org.neo4j.cypher.internal.frontend.v3_0.ast.UnresolvedCall
import org.neo4j.cypher.internal.frontend.v3_0.symbols.CypherType

case class ProcedureSignature(name: QualifiedProcedureName,
                              inputSignature: Seq[FieldSignature],
                              outputSignature: Option[Seq[FieldSignature]],
                              accessMode: ProcedureAccessMode = ProcedureReadOnlyAccess) {

  def outputFields = outputSignature.getOrElse(Seq.empty)

  def isVoid = outputSignature.isEmpty
}

object QualifiedProcedureName {
  def apply(unresolved: UnresolvedCall): QualifiedProcedureName =
    QualifiedProcedureName(unresolved.procedureNamespace.parts, unresolved.procedureName.name)
}

case class QualifiedProcedureName(namespace: Seq[String], name: String) {
  override def toString = s"""${namespace.mkString(".")}.$name"""
}

case class FieldSignature(name: String, typ: CypherType)

sealed trait ProcedureAccessMode

case object ProcedureReadOnlyAccess extends ProcedureAccessMode
case object ProcedureReadWriteAccess extends ProcedureAccessMode
case object ProcedureDbmsAccess extends ProcedureAccessMode
