/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}


trait AggregateExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def opName: String

  def continuation(instruction: Instruction): Instruction = new Instruction {

    override protected def children: Seq[Instruction] = Seq(instruction)

    override protected def operatorId: Set[String] = Set(opName)

    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
      generator.trace(opName) { inner =>
          inner.incrementRows()
          instruction.body(inner)
      }
    }
  }
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
          structure.ifNonNullStatement(structure.loadVariable(tmpName), expression.codeGenType)
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
