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

case class WhileLoop(variable: Variable, producer: LoopDataGenerator, action: Instruction) extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    val iterator = s"${variable.name}Iter"
    generator.trace(producer.opName) { body =>
      producer.produceLoopData(iterator, body)
      body.whileLoop(producer.checkNext(body, iterator)) { loopBody =>
        loopBody.incrementRows()
        producer.getNext(variable, iterator, loopBody)
        action.body(loopBody)
      }
      producer.close(iterator, generator)
    }
  }

  override def operatorId: Set[String] = Set(producer.opName)

  override def children = Seq(action)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    producer.init(generator)
  }
}
