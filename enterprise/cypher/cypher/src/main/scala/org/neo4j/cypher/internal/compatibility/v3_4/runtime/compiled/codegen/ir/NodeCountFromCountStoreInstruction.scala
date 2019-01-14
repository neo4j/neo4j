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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

case class NodeCountFromCountStoreInstruction(opName: String, variable: Variable, labels: List[Option[(Option[Int], String)]],
                                              inner: Instruction) extends Instruction {
  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    val ops: List[(MethodStructure[E]) => E] = labels.map(findOps[E])
    generator.trace(opName) { body =>
      ops.foreach( b => {
        body.incrementDbHits()
      })
      body.assign(variable, multiplyAll[E](ops, body))
      inner.body(body)
    }
  }

  private def multiplyAll[E](ops: List[(MethodStructure[E]) => E], body: MethodStructure[E]): E = ops match {
    case Nil => throw new IllegalStateException("At least one operation must be present at this stage")
    case f :: Nil => f(body)
    case a :: b :: Nil =>
      body.multiplyPrimitive(a(body), b(body))
    case a :: b :: tl =>
      //body.multiplyPrimitive(a(body), b(body))
      multiplyAll( ((bb: MethodStructure[E]) => bb.multiplyPrimitive(a(bb), b(bb))) :: tl , body)
  }

  private def findOps[E](label: Option[(Option[Int], String)]): MethodStructure[E] => E = label match {
      //no label specified by the user
      case None => (body: MethodStructure[E]) => body.nodeCountFromCountStore(body.wildCardToken)

      // label specified and token known
      case Some((Some(token), _)) => (body: MethodStructure[E]) => {
        val tokenConstant = body.token(Int.box(token))
        body.nodeCountFromCountStore(tokenConstant)
      }

      // label specified, but token did not exists at compile time
      case Some((None, labelName)) => (body: MethodStructure[E]) => {
        val isMissing = body.primitiveEquals(body.loadVariable(tokenVar(labelName)), body.wildCardToken)
        val zero = body.constantExpression(0L.asInstanceOf[AnyRef])
        val getFromCountStore = body.nodeCountFromCountStore(body.loadVariable(tokenVar(labelName)))
        body.ternaryOperator(isMissing, zero, getFromCountStore)
      }
    }

  override def operatorId: Set[String] = Set(opName)

  override def children = Seq(inner)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    labelNames.foreach {
      case (token, name) if token.isEmpty =>
        generator.lookupLabelId(tokenVar(name), name)
      case _ => ()
    }
  }

  private def labelNames = labels.filter(_.nonEmpty).flatten.toSet

  private def tokenVar(label: String) = s"${label}Token"
}
