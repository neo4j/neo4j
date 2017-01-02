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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir

import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, Variable}

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
