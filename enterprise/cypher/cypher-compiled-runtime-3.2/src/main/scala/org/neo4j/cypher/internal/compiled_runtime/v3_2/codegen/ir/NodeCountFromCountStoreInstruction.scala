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
package org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.ir

import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiled_runtime.v3_2.codegen.{CodeGenContext, Variable}

case class NodeCountFromCountStoreInstruction(opName: String, variable: Variable, label: Option[(Option[Int], String)],
                                              inner: Instruction) extends Instruction {
  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    generator.trace(opName) { body =>
      body.incrementDbHits()

      label match {
        //no label specified by the user
        case None =>
          body.assign(variable, body.nodeCountFromCountStore(generator.wildCardToken))

        // label specified and token known
        case Some((Some(token), _)) =>
          val tokenConstant = body.token(Int.box(token))
          body.assign(variable, generator.nodeCountFromCountStore(tokenConstant))

        // label specified, but token did not exists at compile time
        case Some((None, labelName)) =>
          val tokenConstant: E = generator.lookupLabelIdE(labelName)
          val isMissing = generator.primitiveEquals(tokenConstant, generator.wildCardToken)
          val zero = body.constantExpression(0L.asInstanceOf[AnyRef])
          val getFromCountStore = body.nodeCountFromCountStore(tokenConstant)
          body.assign(variable, body.ternaryOperator(isMissing, zero, getFromCountStore))
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
