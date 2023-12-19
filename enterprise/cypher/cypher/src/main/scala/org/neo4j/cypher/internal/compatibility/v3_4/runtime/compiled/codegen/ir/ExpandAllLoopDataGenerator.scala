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
import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection

case class ExpandAllLoopDataGenerator(opName: String, fromVar: Variable, dir: SemanticDirection,
                   types: Map[String, String], toVar: Variable, relVar: Variable)
  extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    types.foreach {
      case (typeVar,relType) => generator.lookupRelationshipTypeId(typeVar, relType)
    }
  }

  override def produceLoopData[E](cursorName: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    if(types.isEmpty)
      generator.nodeGetRelationshipsWithDirection(cursorName, fromVar.name, fromVar.codeGenType, dir)
    else
      generator.nodeGetRelationshipsWithDirectionAndTypes(cursorName, fromVar.name, fromVar.codeGenType, dir, types.keys.toIndexedSeq)
    generator.incrementDbHits()
  }

  override def getNext[E](nextVar: Variable, cursorName: String, generator: MethodStructure[E])
                         (implicit context: CodeGenContext) = {
    generator.incrementDbHits()
    generator.nextRelationshipAndNode(toVar.name, cursorName, dir, fromVar.name, relVar.name)
  }

  override def checkNext[E](generator: MethodStructure[E], cursorName: String): E =  generator.advanceRelationshipSelectionCursor(cursorName)

  override def close[E](cursorName: String,
                        generator: MethodStructure[E]): Unit = generator.closeRelationshipSelectionCursor(cursorName)
}
