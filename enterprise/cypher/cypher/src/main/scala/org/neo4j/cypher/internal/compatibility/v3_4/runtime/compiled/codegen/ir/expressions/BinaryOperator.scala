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

import org.neo4j.cypher.internal.util.v3_4.CypherTypeException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.util.v3_4.symbols._

trait BinaryOperator {
  self: CodeGenExpression =>

  def lhs: CodeGenExpression
  def rhs: CodeGenExpression

  def name: String

  override final def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    lhs.init(generator)
    rhs.init(generator)
  }

  override final def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    def isListOf(codeGenType: CodeGenType, cType: CypherType) = codeGenType match {
      case CypherCodeGenType(ListType(inner),_) if inner == cType => true
      case _ => false
    }
    (lhs.codeGenType, rhs.codeGenType) match {
      case (CypherCodeGenType(CTBoolean, _), t) if !(isListOf(t, CTAny) || isListOf(t, CTBoolean)) =>
        throw new CypherTypeException(s"Cannot $name a boolean and ${rhs.codeGenType.ct}")

      case (t, CypherCodeGenType(CTBoolean, _)) if !(isListOf(t, CTAny) || isListOf(t, CTBoolean)) =>
        throw new CypherTypeException(s"Cannot $name a ${rhs.codeGenType.ct} and a boolean")

      case (t1, t2) if t1.isPrimitive && t2.isPrimitive =>
        generator(structure)(context)(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                      structure.box(rhs.generateExpression(structure), rhs.codeGenType))

      case (t, _) if t.isPrimitive =>
        generator(structure)(context)(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                      rhs.generateExpression(structure))
      case (_, t) if t.isPrimitive =>
        generator(structure)(context)(lhs.generateExpression(structure),
                                      structure.box(rhs.generateExpression(structure), rhs.codeGenType))

      case _ => generator(structure)(context)(lhs.generateExpression(structure),
                                              rhs.generateExpression(structure))
    }
  }

  override def codeGenType(implicit context: CodeGenContext) =
    (lhs.codeGenType.ct, rhs.codeGenType.ct) match {
      case (CTInteger, CTInteger) => CypherCodeGenType(CTInteger, ReferenceType)
      case (Number(_), Number(_)) => CypherCodeGenType(CTFloat, ReferenceType)
      // Runtime we'll figure it out - can't store it in a primitive field unless we are 100% of the type
      case _ => CodeGenType.Any
    }

  protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext): (E, E) => E
}

object Number {
  def unapply(x: CypherType): Option[CypherType] = if (CTNumber.isAssignableFrom(x)) Some(x) else None
}
