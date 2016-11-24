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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.Instruction
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, MethodStructure, Variable}

/**
  * Base class for aggregate expressions
  * @param expression the expression to aggregate
  * @param distinct is the aggregation distinct or not
  */
abstract class AggregateExpression(expression: CodeGenExpression, distinct: Boolean) {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def continuation(instruction: Instruction): Instruction = instruction

  def distinctCondition[E](value: E, valueType: CodeGenType, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext)

  protected def ifNotNull[E](structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext) = {
    expression match {
      case NodeExpression(v) => primitiveIfNot(v, structure)(block(_))
      case NodeProjection(v) => primitiveIfNot(v, structure)(block(_))
      case RelationshipExpression(v) => primitiveIfNot(v, structure)(block(_))
      case RelationshipProjection(v) => primitiveIfNot(v, structure)(block(_))
      case _ =>
        val tmpName = context.namer.newVarName()
        structure.assign(tmpName, expression.codeGenType, expression.generateExpression(structure))
        structure.ifNonNullStatement(structure.loadVariable(tmpName)) { body =>
          if (distinct) {
            distinctCondition(structure.loadVariable(tmpName),expression.codeGenType, body) { inner =>
              block(inner)
            }
          }
          else block(body)
        }
    }
  }

  protected def internalExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E = {
    expression match {
      case NodeExpression(v) => structure.loadVariable(v.name)
      case NodeProjection(v) => structure.loadVariable(v.name)
      case RelationshipExpression(v) => structure.loadVariable(v.name)
      case RelationshipProjection(v) => structure.loadVariable(v.name)
      case _ => expression.generateExpression(structure)
    }
  }

  protected def internalExpressionType(implicit context: CodeGenContext) = expression match {
    case NodeExpression(v) => CodeGenType.primitiveNode
    case NodeProjection(v) => CodeGenType.primitiveNode
    case RelationshipExpression(v) => CodeGenType.primitiveRel
    case RelationshipProjection(v) => CodeGenType.primitiveRel
    case _ => expression.codeGenType
  }

  private def primitiveIfNot[E](v: Variable, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                               (implicit context: CodeGenContext) = {
    structure.ifNotStatement(structure.equalityExpression(structure.loadVariable(v.name),
                                                          structure.constantExpression(Long.box(-1)),
                                                          CodeGenType.primitiveInt)) { body =>
      if (distinct) {
        distinctCondition(structure.loadVariable(v.name), CodeGenType.primitiveInt, body) { inner =>
          block(inner)
        }
      }
      else block(body)
    }
  }
}