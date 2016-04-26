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
import org.neo4j.cypher.internal.frontend.v3_1.symbols.CTBoolean
import org.neo4j.cypher.internal.frontend.v3_1.{IncomparableValuesException, symbols}

case class Equals(lhs: CodeGenExpression, rhs: CodeGenExpression) extends CodeGenExpression {

  override def nullable(implicit context: CodeGenContext) = lhs.nullable || rhs.nullable

  override def codeGenType(implicit context: CodeGenContext) =
    if (nullable) CodeGenType(CTBoolean, ReferenceType)
    else (lhs, rhs) match {
    case (_:NodeExpression, _:NodeExpression) => CodeGenType(CTBoolean, BoolType)
    case (_:RelationshipExpression, _:RelationshipExpression) => CodeGenType(CTBoolean, BoolType)
    case _ => CodeGenType(CTBoolean, ReferenceType)
  }

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    lhs.init(generator)
    rhs.init(generator)
  }

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    if (nullable) structure.threeValuedEquals(structure.box(lhs.generateExpression(structure), lhs.codeGenType),
                                              structure.box(rhs.generateExpression(structure), rhs.codeGenType))
    else (lhs, rhs) match {
      case (NodeExpression(v1), NodeExpression(v2)) => structure.eq(structure.load(v1.name), structure.load(v2.name), CodeGenType.primitiveNode)
      case (RelationshipExpression(v1), RelationshipExpression(v2)) => structure.eq(structure.load(v1.name), structure.load(v2.name),
                                                                                    CodeGenType.primitiveRel)
      case (NodeExpression(_), RelationshipExpression(_)) => throw new
          IncomparableValuesException(symbols.CTNode.toString, symbols.CTRelationship.toString)
      case (RelationshipExpression(_), NodeExpression(_)) => throw new
          IncomparableValuesException(symbols.CTNode.toString, symbols.CTRelationship.toString)
      case _ => structure.threeValuedEquals(structure.box(lhs.generateExpression(structure), lhs. codeGenType),
                                            structure.box(rhs.generateExpression(structure), rhs.codeGenType))
    }
  }
}
