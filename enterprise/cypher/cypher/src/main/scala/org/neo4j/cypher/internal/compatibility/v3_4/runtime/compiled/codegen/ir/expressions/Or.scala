/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.util.v3_4.symbols.CTBoolean

case class Or(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression {

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override def codeGenType(implicit context: CodeGenContext) =
    if (!nullable) CodeGenType.primitiveBool
    else CypherCodeGenType(CTBoolean, ReferenceType)

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
          structure.threeValuedOrExpression(structure.box(lhs.generateExpression(structure), lhs.codeGenType), structure.box(rhs.generateExpression(structure), rhs.codeGenType)),
          CypherCodeGenType(CTBoolean, ReferenceType))
    }
    else structure.threeValuedOrExpression(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                           structure.box(rhs.generateExpression(structure), rhs.codeGenType))
}
