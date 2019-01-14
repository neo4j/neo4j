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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure

case class AcceptVisitor(produceResultOpName: String, columns: Map[String, CodeGenExpression])
  extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(produceResultOpName) { body =>
      body.incrementRows()
      columns.foreach { case (k, v: CodeGenExpression) =>
        body.setInRow(context.nameToIndex(k), anyValue(body, v))
      }
      body.visitorAccept()
    }
  }

  private def anyValue[E](generator: MethodStructure[E], v: CodeGenExpression)(implicit context: CodeGenContext) = {
    if (v.needsJavaNullCheck) {
      val variable = context.namer.newVarName()
      generator.localVariable(variable, v.generateExpression(generator), v.codeGenType)
      generator.ternaryOperator(generator.isNull(generator.loadVariable(variable), v.codeGenType),
                                generator.noValue(),
                                generator.toMaterializedAnyValue(generator.loadVariable(variable), v.codeGenType))
    }
    else generator.toMaterializedAnyValue(v.generateExpression(generator), v.codeGenType)
  }

  override protected def operatorId = Set(produceResultOpName)

  override protected def children = Seq.empty

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) {
    columns.values.foreach(_.init(generator))
    super.init(generator)
  }
}
