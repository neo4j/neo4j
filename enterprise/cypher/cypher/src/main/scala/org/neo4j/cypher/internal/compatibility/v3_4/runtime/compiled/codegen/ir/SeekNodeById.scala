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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

case class SeekNodeById(opName: String, nodeVar: Variable, expression: CodeGenExpression, action: Instruction) extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(opName) { body =>
      body.incrementDbHits()
      body.nodeIdSeek(nodeVar.name, expression.generateExpression(body), expression.codeGenType) { seekBody =>
        seekBody.incrementRows()
        action.body(seekBody)
      }
    }
  }

  override protected def children: Seq[Instruction] = Seq(action)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    super.init(generator)
    expression.init(generator)
  }
}
