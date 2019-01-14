/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.{HashableTupleDescriptor, MethodStructure}

case class Distinct(opName: String, setName: String, vars: Iterable[(String, CodeGenExpression)])
  extends AggregateExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    vars.foreach {
      case (_, e) => e.init(generator)
    }
    generator.newDistinctSet(setName, vars.map(_._2.codeGenType))
  }


  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    vars.foreach {
      case (variable, expr) =>
        generator.declare(variable, expr.codeGenType)
        val generatedExpression =
          if (expr.needsJavaNullCheck) {
            // If we get null in an ReferenceType we convert it to NO_VALUE to avoid keeping both null and NO_VALUE in the set
            generator.ifNullThenNoValue(expr.generateExpression(generator))
          } else {
            expr.generateExpression(generator)
          }
        // Only materialize in produce results
        generator.assign(variable, expr.codeGenType, generatedExpression)
    }
    generator.distinctSetIfNotContains(setName,
                                       vars.map(v => v._1 -> (v._2.codeGenType ->
                                         generator.loadVariable(v._1))).toMap)((_) => {})
  }

  override def continuation(instruction: Instruction): Instruction = new Instruction {

    override protected def children: Seq[Instruction] = Seq(instruction)

    override protected def operatorId = Set(opName)

    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
      generator.trace(opName) { body =>
        val keyArg = vars.map(k => k._1 -> k._2.codeGenType).toMap
        body.distinctSetIterate(setName, HashableTupleDescriptor(keyArg)) { inner =>
          inner.incrementRows()
          instruction.body(inner)
        }
      }
    }
  }
}


