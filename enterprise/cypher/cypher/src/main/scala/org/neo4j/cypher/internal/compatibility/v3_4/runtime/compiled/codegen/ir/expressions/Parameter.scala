/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure

case class Parameter(key: String, variableName: String, cType: CypherCodeGenType = CodeGenType.Any) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.expectParameter(key, variableName, cType)

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E =
    structure.loadVariable(variableName)

  override def nullable(implicit context: CodeGenContext) = cType.canBeNullable

  override def codeGenType(implicit context: CodeGenContext) = cType
}
