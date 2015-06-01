/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v2_3.codegen.MethodStructure

trait CodeGenExpression {
  def init[E](generator: MethodStructure[E]): Unit

  def generateExpression[E](structure: MethodStructure[E]): E
}

object CodeGenExpression {

  def literal(value: Long): CodeGenExpression = Literal(java.lang.Long.valueOf(value))

  def literal(value: Double): CodeGenExpression = Literal(java.lang.Double.valueOf(value))

  def literal(value: String): CodeGenExpression = Literal(value)

  def parameter(key: String): CodeGenExpression = Parameter(key)

  def add(lhs: CodeGenExpression, rhs: CodeGenExpression): CodeGenExpression = Addition(lhs, rhs)

  def sub(lhs: CodeGenExpression, rhs: CodeGenExpression): CodeGenExpression = Subtraction(lhs, rhs)
}
