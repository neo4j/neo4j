/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.codegen.ir

import org.neo4j.cypher.internal.compiler.v2_3.codegen.{Variable, CodeGenContext, MethodStructure}

/**
 * Generates instructions for running a loop data generator that sets the flag with the provided name
 * to true if the loop data generator produces results
 * @param loop the loop to wrap
 * @param yieldedFlagVar the name of the flag to update
 */
case class CheckingLoopDataGenerator(loop: LoopDataGenerator, yieldedFlagVar: String)
  extends LoopDataGenerator {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    loop.init(generator)
  }

  override def produceIterator[E](iterVar: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    loop.produceIterator(iterVar, generator)
  }

  override def produceNext[E](nextVar: Variable, iterVar: String, generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    loop.produceNext(nextVar, iterVar, generator)
    generator.updateFlag(yieldedFlagVar, newValue = true)
  }

  override def children = Seq(loop)

  override def id = loop.id
}
