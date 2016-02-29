/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.codegen.ir.functions

import org.neo4j.cypher.internal.compiler.v3_0.codegen.ir.expressions._
import org.neo4j.cypher.internal.frontend.v3_0.InternalException

sealed trait CodeGenFunction1 extends ((CodeGenExpression) => CodeGenExpression)

case object IdCodeGenFunction extends CodeGenFunction1 {

  override def apply(arg: CodeGenExpression): CodeGenExpression = arg match {
    case n: NodeExpression => LoadVariable(n.nodeIdVar)
    case n: NodeProjection => LoadVariable(n.nodeIdVar)
    case r: RelationshipExpression => LoadVariable(r.relId)
    case r: RelationshipProjection => LoadVariable(r.relId)
    case e => throw new InternalException(s"id function only accepts nodes or relationships not $e")
  }
}

case object TypeCodeGenFunction extends CodeGenFunction1 {
  override def apply(arg: CodeGenExpression): CodeGenExpression = arg match {
    case r: RelationshipExpression => TypeOf(r.relId)
    case r: RelationshipProjection => TypeOf(r.relId)
    case e => throw new InternalException(s"type function only accepts relationships $e")
  }
}
