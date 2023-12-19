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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.{LessThanEqual, MethodStructure}

case class DecreaseAndReturnWhenZero(opName: String, variableName: String, action: Instruction, startValue: CodeGenExpression)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    startValue.init(generator)
    val expression = generator.box(startValue.generateExpression(generator), startValue.codeGenType)
    generator.declareCounter(variableName, expression)
    generator.ifStatement(generator.checkInteger(variableName, LessThanEqual, 0L)) { onTrue =>
      onTrue.returnSuccessfully()
    }
    action.init(generator)
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    action.body(generator)

    generator.trace(opName) { l1 =>
      l1.incrementRows()
      l1.decrementInteger(variableName)
      l1.ifStatement(l1.checkInteger(variableName, LessThanEqual, 0L)) { l2 =>
        l2.returnSuccessfully()
      }
    }
  }

  override protected def children: Seq[Instruction] = Seq(action)

  override protected def operatorId = Set(opName)
}
