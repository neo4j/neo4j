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

import org.neo4j.cypher.internal.compiler.v3_2.codegen.CodeGenContext
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CTBoolean

case class Or(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression {

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override def codeGenType(implicit context: CodeGenContext) =
    if (!nullable) CodeGenType.primitiveBool
    else CodeGenType(CTBoolean, ReferenceType)

  override final def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    lhs.init(generator)
    rhs.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E =
    if (!nullable) (lhs.codeGenType, rhs.codeGenType) match {
      case (t1, t2) if t1.isPrimitive && t2.isPrimitive =>
        structure.orExpression(lhs.generateExpression(structure), rhs.generateExpression(structure))
      case (t1, t2) if t1.isPrimitive =>
        structure.orExpression(lhs.generateExpression(structure), structure.unbox(rhs.generateExpression(structure), t2))
      case (t1, t2) if t2.isPrimitive =>
        structure.orExpression(structure.unbox(lhs.generateExpression(structure), t1), rhs.generateExpression(structure))
      case _ =>
        structure.unbox(
          structure.threeValuedOrExpression(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                                  structure.box(rhs.generateExpression(structure), rhs.codeGenType)),
          CodeGenType(CTBoolean, ReferenceType))
    }
    else structure.threeValuedOrExpression(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                           structure.box(rhs.generateExpression(structure), rhs.codeGenType))
}
