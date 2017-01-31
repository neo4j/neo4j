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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.Instruction
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, Variable}


trait AggregateExpression {
  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def continuation(instruction: Instruction): Instruction = instruction
}
/**
  * Base class for aggregate expressions
  * @param expression the expression to aggregate
  * @param distinct is the aggregation distinct or not
  */
abstract class BaseAggregateExpression(expression: CodeGenExpression, distinct: Boolean) extends AggregateExpression {


  def distinctCondition[E](value: E, valueType: CodeGenType, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext)

  protected def ifNotNull[E](structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext) = {
    expression match {
      case NodeExpression(v) => primitiveIfNot(v, structure)(block(_))
      case RelationshipExpression(v) => primitiveIfNot(v, structure)(block(_))
      case expr =>
        val tmpName = context.namer.newVarName()
        structure.assign(tmpName, expression.codeGenType, expression.generateExpression(structure))
        val perhapsCheckForNotNullStatement: ((MethodStructure[E]) => Unit) => Unit = if (expr.nullable)
          structure.ifNonNullStatement(structure.loadVariable(tmpName))
        else
          _(structure)

        perhapsCheckForNotNullStatement { body =>
          if (distinct) {
            distinctCondition(structure.loadVariable(tmpName), expression.codeGenType, body) { inner =>
              block(inner)
            }
          }
          else block(body)
        }
    }
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
