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

/**
 * Generates code that runs and afterwards checks if the provided variable has been set,
 * if not it sets all provided variables to null and runs the alternativeAction
 */
case class NullingInstruction(loop: Instruction, yieldedFlagVar: String, alternativeAction: Instruction,
                            nullableVars: Variable*)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    loop.init(generator)

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.declareFlag(yieldedFlagVar, initialValue = false)
    loop.body(generator)
    generator.ifNotStatement(generator.loadVariable(yieldedFlagVar)){ ifBody =>
      //mark variables as null
      nullableVars.foreach(v => ifBody.markAsNull(v.name, v.codeGenType))
      alternativeAction.body(ifBody)
    }
  }

  override def children = Seq(loop)
}
