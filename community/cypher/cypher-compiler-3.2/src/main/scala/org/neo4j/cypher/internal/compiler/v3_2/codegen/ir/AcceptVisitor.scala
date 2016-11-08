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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir

import org.neo4j.cypher.internal.compiler.v3_2.codegen.CodeGenContext
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure

case class AcceptVisitor(produceResultOpName: String, columns: Map[String, CodeGenExpression])
  extends Instruction {

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(produceResultOpName) { body =>
      body.incrementRows()
      columns.foreach { case (k, v) =>
        body.setInRow(k, generator.box(v.generateExpression(body), v.codeGenType))
      }
      body.visitorAccept()
    }
  }

  override protected def operatorId = Set(produceResultOpName)

  override protected def children = Seq.empty

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) {
    columns.values.foreach(_.init(generator))
    super.init(generator)
  }
}
