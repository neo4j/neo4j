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

case class IndexSeek(opName: String, labelName: String, propNames: Seq[String], descriptorVar: String,
                     expression: CodeGenExpression) extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    assert(propNames.length == 1)
    expression.init(generator)
    val labelVar = context.namer.newVarName()
    val propKeyVar = context.namer.newVarName()
    generator.lookupLabelId(labelVar, labelName)
    generator.lookupPropertyKey(propNames.head, propKeyVar)
    generator.newIndexReference(descriptorVar, labelVar, propKeyVar)
  }

  override def produceLoopData[E](cursorName: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
      generator.indexSeek(cursorName, descriptorVar, expression.generateExpression(generator), expression.codeGenType)
      generator.incrementDbHits()
  }

  override def getNext[E](nextVar: Variable, cursorName: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext) = {
    generator.incrementDbHits()
    generator.nodeFromNodeValueIndexCursor(nextVar.name, cursorName)
  }

  override def checkNext[E](generator: MethodStructure[E], cursorName: String): E = generator.advanceNodeValueIndexCursor(cursorName)

  override def close[E](cursorName: String, generator: MethodStructure[E]): Unit = generator.closeNodeValueIndexCursor(cursorName)
}
