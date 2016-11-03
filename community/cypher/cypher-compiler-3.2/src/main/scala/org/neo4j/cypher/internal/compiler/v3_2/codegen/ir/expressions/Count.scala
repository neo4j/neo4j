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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.frontend.v3_2.symbols

trait AggregateExpression extends CodeGenExpression {
  def initialValue[E](generator: MethodStructure[E])(implicit context: CodeGenContext): E
}

case class Count(expression: CodeGenExpression) extends AggregateExpression {

  private var name: String = null
  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    name = context.namer.newVarName()
    generator.assign(name, CodeGenType.primitiveInt, generator.constantExpression(Long.box(0L)))
  }

  def initialValue[E](generator: MethodStructure[E])(implicit context: CodeGenContext): E =
      generator.constantExpression(Long.box(0L))

  def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    structure.ifNonNullStatement(expression.generateExpression(structure)) { body =>
      body.incrementInteger(name)
    }
    structure.loadVariable(name)
  }

  override def nullable(implicit context: CodeGenContext) = false

  override def codeGenType(implicit context: CodeGenContext) = CodeGenType(symbols.CTInteger, IntType)
}
