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

import org.neo4j.cypher.internal.util.v3_4.IncomparableValuesException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.util.v3_4.symbols.{CTBoolean, CTMap, ListType}
import org.neo4j.cypher.internal.util.v3_4.symbols

case class Equals(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression {

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable ||
    isCollectionOfNonPrimitives(lhs) || isCollectionOfNonPrimitives(rhs)

  override def codeGenType(implicit context: CodeGenContext) =
    if (nullable) CypherCodeGenType(CTBoolean, ReferenceType)
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
          CypherCodeGenType(CTBoolean, ReferenceType))
      }
  }

  private def isCollectionOfNonPrimitives(e: CodeGenExpression)(implicit context: CodeGenContext) =
    e.codeGenType match {
      case CypherCodeGenType(ListType(_), ListReferenceType(inner)) if !RepresentationType.isPrimitive(inner) =>
        true
      case CypherCodeGenType(CTMap, _) =>
        true
      case _ =>
        false
    }
}
