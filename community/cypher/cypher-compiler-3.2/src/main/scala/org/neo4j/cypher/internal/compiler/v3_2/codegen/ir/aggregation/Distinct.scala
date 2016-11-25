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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.Instruction
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, MethodStructure, Variable}

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
        body.distinctSetIterate(setName, keyArg) { inner =>
          inner.incrementRows()
          instruction.body(inner)
        }
      }
    }
  }
}


