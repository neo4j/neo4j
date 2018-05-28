/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir.Instruction
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.{HashableTupleDescriptor, MethodStructure}
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.{CodeGenContext, Variable}

case class Distinct(opName: String, setName: String, vars: Iterable[Variable]) extends AggregateExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.newDistinctSet(setName, vars.map(_.codeGenType))


  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.distinctSetIfNotContains(setName,
                                       vars.map(v => v.name -> (v.codeGenType ->
                                         generator.loadVariable(v.name))).toMap)((_) => {})
  }

  override def continuation(instruction: Instruction): Instruction = new Instruction {

    override protected def children: Seq[Instruction] = Seq(instruction)

    override protected def operatorId = Set(opName)

    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
      generator.trace(opName) { body =>
        val keyArg = vars.map(k => k.name -> k.codeGenType).toMap
        body.distinctSetIterate(setName, HashableTupleDescriptor(keyArg)) { inner =>
          inner.incrementRows()
          instruction.body(inner)
        }
      }
    }
  }
}


