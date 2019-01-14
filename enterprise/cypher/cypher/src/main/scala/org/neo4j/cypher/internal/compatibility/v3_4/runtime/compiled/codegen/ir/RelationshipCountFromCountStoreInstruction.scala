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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenType
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

case class RelationshipCountFromCountStoreInstruction(opName: String, variable: Variable, startLabel: Option[(Option[Int],String)],
                                                      relTypes: Seq[(Option[Int], String)], endLabel: Option[(Option[Int],String)],
                                                      inner: Instruction) extends Instruction {
  private val hasTokens = opName + "hasTokens"

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    generator.assign(variable, generator.constantPrimitiveExpression(0L))
    generator.trace(opName) { body =>

      body.ifStatement(body.loadVariable(hasTokens)) { ifBody =>

        def labelToken(lbl: Option[(Option[Int], String)], varName: String): E = lbl match {
          // label specified, and token known at compile time
          case Some((Some(token), _)) =>
            ifBody.constantExpression(Int.box(token))

          // label specified, but no token available at compile time
          case Some((None, labelName)) =>
            val variableName = s"${variable.name}$varName$labelName"
            ifBody.loadVariable(variableName)

          // no label specified
          case _ => ifBody.wildCardToken
        }

        val start = labelToken(startLabel, "StartOf")
        val end = labelToken(endLabel, "EndOf")

        if (relTypes.isEmpty)
          ifBody.incrementInteger(variable.name, ifBody.relCountFromCountStore(start, end, ifBody.wildCardToken))
        else
          relTypes.foreach {
            case (Some(token), _) =>
              val relType = ifBody.constantPrimitiveExpression(token)
              ifBody.incrementDbHits()
              ifBody.incrementInteger(variable.name, ifBody.relCountFromCountStore(start, end, relType))

            case (None, name) =>
              val relTypeToken = ifBody.loadVariable(s"${variable.name}TypeOf$name")
              val ifValidToken = ifBody.notExpression(ifBody.equalityExpression(relTypeToken, ifBody.wildCardToken, CodeGenType.javaInt))
              ifBody.ifStatement(ifValidToken) { inner =>
                inner.incrementDbHits()
                inner.incrementInteger(variable.name, inner.relCountFromCountStore(start, end, relTypeToken))
              }
          }
      }
    }
    inner.body(generator)
  }

  override def operatorId: Set[String] = Set(opName)

  override def children = Seq(inner)


  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)

    /*
    When initialising, we check that all label tokens that we need are available. If any are missing,
    we can simply return 0.
     */
    generator.assign(hasTokens, CodeGenType.primitiveBool, generator.constantPrimitiveExpression(true))

    def loadLabelToken(labelName: String, varName: String) = {
      val variableName = s"${variable.name}$varName$labelName"
      generator.assign(variableName, CodeGenType.javaInt, generator.lookupLabelIdE(labelName))
      val isTokenMissing = generator.equalityExpression(generator.loadVariable(variableName), generator.wildCardToken, CodeGenType.primitiveBool)
      generator.ifStatement(isTokenMissing) { block =>
        block.assign(hasTokens, CodeGenType.primitiveBool, block.constantPrimitiveExpression(false))
      }
    }

    startLabel.foreach {
      case (token, name) if token.isEmpty => loadLabelToken(name, "StartOf")
      case _ => ()
    }
    endLabel.foreach {
      case (token, name) if token.isEmpty => loadLabelToken(name, "EndOf")
      case _ => ()
    }
    relTypes.foreach {
      case (token, name) if token.isEmpty =>
        generator.assign(s"${variable.name}TypeOf$name", CodeGenType.javaInt, generator.lookupRelationshipTypeIdE(name))
      case _ => ()
    }
  }

  private def tokenVar = s"${variable.name}LabelToken"
}
