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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

case class ScanForLabel(opName: String, labelName: String, labelVar: String) extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.lookupLabelId(labelVar, labelName)

  override def produceLoopData[E](cursorName: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.labelScan(cursorName, labelVar)
    generator.incrementDbHits()
  }

  override def getNext[E](nextVar: Variable, cursorName: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext) = {
    generator.incrementDbHits()
    generator.nodeFromNodeLabelIndexCursor(nextVar.name, cursorName)
  }

  override def checkNext[E](generator: MethodStructure[E], cursorName: String): E = generator.advanceNodeLabelIndexCursor(cursorName)

  override def close[E](cursorName: String, generator: MethodStructure[E]): Unit = generator.closeNodeLabelIndexCursor(cursorName)
}
