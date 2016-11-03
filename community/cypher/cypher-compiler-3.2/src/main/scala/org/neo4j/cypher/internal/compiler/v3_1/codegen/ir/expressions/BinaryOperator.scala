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
package org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_1.codegen.{CodeGenContext, MethodStructure}
import org.neo4j.cypher.internal.frontend.v3_1.CypherTypeException
import org.neo4j.cypher.internal.frontend.v3_1.symbols._

trait BinaryOperator {
  self: CodeGenExpression =>

  def lhs: CodeGenExpression
  def rhs: CodeGenExpression

  def name: String

  override final def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    lhs.init(generator)
    rhs.init(generator)
  }

  override final def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    (lhs.codeGenType, rhs.codeGenType) match {
      case (CodeGenType(CTBoolean, _), _) => throw new CypherTypeException(s"Cannot $name a boolean and ${rhs.codeGenType.ct}")
      case (_, CodeGenType(CTBoolean, _)) => throw new CypherTypeException(s"Cannot $name a ${rhs.codeGenType.ct} and a boolean")

      case (t1, t2) if t1.isPrimitive && t2.isPrimitive =>
        generator(structure)(context)(structure.box(lhs.generateExpression(structure), t1),
                                      structure.box(rhs.generateExpression(structure), t2))

      case (t, _) if t.isPrimitive =>
        generator(structure)(context)(structure.box(lhs.generateExpression(structure), t),
                                      rhs.generateExpression(structure))
      case (_, t) if t.isPrimitive =>
        generator(structure)(context)(lhs.generateExpression(structure),
                                                           structure.box(rhs.generateExpression(structure), t))

      case _ => generator(structure)(context)(lhs.generateExpression(structure),
                                              rhs.generateExpression(structure))
    }

  override def codeGenType(implicit context: CodeGenContext) =
    (lhs.codeGenType.ct, rhs.codeGenType.ct) match {
      case (CTInteger, CTInteger) => CodeGenType(CTInteger, ReferenceType)
      case (Number(_), Number(_)) => CodeGenType(CTFloat, ReferenceType)
      // Runtime we'll figure it out - can't store it in a primitive field unless we are 100% of the type
      case _ => CodeGenType.Any
    }

  protected def generator[E](structure: MethodStructure[E])(implicit context: CodeGenContext): (E, E) => E
}

object Number {
  def unapply(x: CypherType): Option[CypherType] = if (CTNumber.isAssignableFrom(x)) Some(x) else None
}
