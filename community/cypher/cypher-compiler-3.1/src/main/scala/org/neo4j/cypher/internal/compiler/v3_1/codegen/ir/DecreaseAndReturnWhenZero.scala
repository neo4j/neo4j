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
package org.neo4j.cypher.internal.compiler.v3_1.codegen.ir

import org.neo4j.cypher.internal.compiler.v3_1.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compiler.v3_1.codegen.{CodeGenContext, MethodStructure}

case class DecreaseAndReturnWhenZero(opName: String, variableName: String, action: Instruction, startValue: CodeGenExpression)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    startValue.init(generator)
    val expression = generator.box(startValue.generateExpression(generator), startValue.codeGenType)
    generator.declareCounter(variableName, expression)
    generator.ifStatement(generator.counterEqualsZero(variableName)) { onTrue =>
      onTrue.returnSuccessfully()
    }
    action.init(generator)
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    action.body(generator)

    generator.trace(opName) { l1 =>
      l1.incrementRows()
      l1.ifStatement(l1.decreaseCounterAndCheckForZero(variableName)) { l2 =>
        l2.returnSuccessfully()
      }
    }
  }

  override protected def children: Seq[Instruction] = Seq(action)

  override protected def operatorId = Set(opName)
}
