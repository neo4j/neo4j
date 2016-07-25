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

import org.neo4j.cypher.internal.compiler.v3_1.codegen.{Variable, CodeGenContext, MethodStructure}

case class WhileLoop(variable: Variable, producer: LoopDataGenerator, action: Instruction) extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    val iterator = s"${variable.name}Iter"
    generator.trace(producer.opName) { body =>
      producer.produceIterator(iterator, body)
      body.whileLoop(producer.hasNext(body, iterator)) { loopBody =>
        loopBody.incrementDbHits()
        loopBody.incrementRows()
        producer.produceNext(variable, iterator, loopBody)
        action.body(loopBody)
      }
    }
  }

  override def operatorId: Set[String] = Set(producer.opName)

  override def children = Seq(action)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    producer.init(generator)
  }
}
