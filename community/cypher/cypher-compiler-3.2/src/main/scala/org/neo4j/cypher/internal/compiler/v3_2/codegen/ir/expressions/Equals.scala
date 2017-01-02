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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_2.codegen.CodeGenContext
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.frontend.v3_2.symbols.CTBoolean
import org.neo4j.cypher.internal.frontend.v3_2.{IncomparableValuesException, symbols}

case class Equals(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression {

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override def codeGenType(implicit context: CodeGenContext) =
    if (nullable) CodeGenType(CTBoolean, ReferenceType)
    else CodeGenType.primitiveBool

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    lhs.init(generator)
    rhs.init(generator)
  }

  /*
   * This looks somewhat complicated since it is doing a couple of optimizations:
   * - If we are comparing (non-nullable) nodes and relationships we do it by comparing ids directly
   * - If we are comparing two primitives we use: `a == b`
   * - If we are comparing one primitive with one reference type we unbox the reference, e.g.
   *   `a == b.longValue`
   * - For (non-nullable) strings we directly use `a.equals(b)`
   * - For other cases we resort to `threeValuedEqualsExpression` which has the correct `null`, `list/array`
   *   semantics
   */
  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    (lhs, rhs, nullable) match {
      case (NodeExpression(v1), NodeExpression(v2), false) =>
        structure.equalityExpression(structure.loadVariable(v1.name), structure.loadVariable(v2.name), CodeGenType.primitiveNode)
      case (NodeExpression(v1), NodeExpression(v2), true) =>
        structure.threeValuedPrimitiveEqualsExpression(structure.loadVariable(v1.name), structure.loadVariable(v2.name),
                                                       CodeGenType.primitiveNode)
      case (RelationshipExpression(v1), RelationshipExpression(v2), false) =>
        structure.equalityExpression(structure.loadVariable(v1.name), structure.loadVariable(v2.name),
                                     CodeGenType.primitiveRel)
      case (RelationshipExpression(v1), RelationshipExpression(v2), true) =>
        structure.threeValuedPrimitiveEqualsExpression(structure.loadVariable(v1.name), structure.loadVariable(v2.name),
                                                       CodeGenType.primitiveRel)
      case (NodeExpression(_), RelationshipExpression(_), _) => throw new
          IncomparableValuesException(symbols.CTNode.toString, symbols.CTRelationship.toString)
      case (RelationshipExpression(_), NodeExpression(_), _) => throw new
          IncomparableValuesException(symbols.CTNode.toString, symbols.CTRelationship.toString)

      // nullable case
      case (_, _, true) =>
        structure.threeValuedEqualsExpression(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                              structure.box(rhs.generateExpression(structure), rhs.codeGenType))
      // not nullable cases below

      //both are primitive
      case (t1, t2, false) if t1.codeGenType == t2.codeGenType && t1.codeGenType.isPrimitive =>
        structure.equalityExpression(lhs.generateExpression(structure), rhs.generateExpression(structure), t1.codeGenType)
      //t1 is primitive
      case (t1, t2, false) if t1.codeGenType.ct == t2.codeGenType.ct && t1.codeGenType.isPrimitive =>
        structure.equalityExpression(
          lhs.generateExpression(structure), structure.unbox(rhs.generateExpression(structure), t2.codeGenType),
          t1.codeGenType)
      //t2 is primitive
      case (t1, t2, false) if t1.codeGenType.ct == t2.codeGenType.ct && t2.codeGenType.isPrimitive =>
        structure.equalityExpression(
          structure.unbox(lhs.generateExpression(structure), t1.codeGenType), rhs.generateExpression(structure), t1.codeGenType)

      //both are strings
      case (t1, t2, false) if t1.codeGenType.ct == t2.codeGenType.ct && t1.codeGenType.ct == symbols.CTString =>
        structure.equalityExpression(lhs.generateExpression(structure), rhs.generateExpression(structure), t1.codeGenType)

      case _ =>
        structure.unbox(
          structure.threeValuedEqualsExpression(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                                structure.box(rhs.generateExpression(structure), rhs.codeGenType)),
          CodeGenType(CTBoolean, ReferenceType))
      }
  }
}
