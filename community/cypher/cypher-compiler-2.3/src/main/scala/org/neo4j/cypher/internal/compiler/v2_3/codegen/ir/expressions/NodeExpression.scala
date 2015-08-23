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

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{CodeGenContext, MethodStructure, Variable}
import org.neo4j.cypher.internal.semantics.v2_3.symbols
import org.neo4j.cypher.internal.semantics.v2_3.symbols._

case class NodeExpression(nodeIdVar: Variable) extends CodeGenExpression {
  assert(nodeIdVar.cypherType == symbols.CTNode)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    if (nodeIdVar.nullable)
      structure.nullable(nodeIdVar.name, nodeIdVar.cypherType, structure.node(nodeIdVar.name))
    else
      structure.node(nodeIdVar.name)
  }

  override def nullable(implicit context: CodeGenContext) = nodeIdVar.nullable

  override def cypherType(implicit context: CodeGenContext) = CTNode
}
