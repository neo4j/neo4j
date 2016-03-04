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
package org.neo4j.cypher.internal.compiler.v3_0.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_0.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.frontend.v3_0.symbols

case class Not(inner: CodeGenExpression) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    inner.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    if (!nullable && inner.cypherType == symbols.CTBoolean) structure.not(inner.generateExpression(structure))
    else structure.threeValuedNot(inner.generateExpression(structure))

  override def nullable(implicit context: CodeGenContext) = inner.nullable

  override def cypherType(implicit context: CodeGenContext) = symbols.CTBoolean
}
