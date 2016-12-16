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

import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, Variable}

case class NodeCountFromCountStoreInstruction(opName: String, variable: Variable, label: Option[(Option[Int], String)],
                                              inner: Instruction) extends Instruction {
  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(opName) { body =>
      body.incrementDbHits()
      if (label.nonEmpty) {
        val (token, _) = label.get
        val expression = token.map(t => body.token(Int.box(t))).getOrElse(body.loadVariable(tokenVar))
        body.assign(variable.name, variable.codeGenType,
                    generator.nodeCountFromCountStore(expression))
      } else {
        body.assign(variable.name, variable.codeGenType,
                    generator.nodeCountFromCountStore(generator.wildCardToken))
      }
      inner.body(body)
    }
  }

  override def operatorId: Set[String] = Set(opName)

  override def children = Seq(inner)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    label.foreach {
      case (token, name) if token.isEmpty =>
        generator.lookupLabelId(tokenVar, name)
      case _ => ()
    }
  }

  private def tokenVar = s"${variable.name}LabelToken"
}
