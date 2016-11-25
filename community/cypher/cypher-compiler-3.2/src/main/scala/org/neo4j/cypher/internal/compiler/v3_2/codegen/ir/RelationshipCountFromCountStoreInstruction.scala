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

import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, MethodStructure, Variable}

case class RelationshipCountFromCountStoreInstruction(opName: String, variable: Variable, startLabel: Option[(Option[Int],String)],
                                                      relTypes: Seq[(Option[Int], String)], endLabel: Option[(Option[Int],String)],
                                                      inner: Instruction) extends Instruction {
  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(opName) { body =>

      val start = startLabel match {
        case Some((Some(token), _)) =>  body.constantExpression(Int.box(token))
        case Some((None, name)) => body.loadVariable(s"${variable.name}StartOf$name")
        case _ => body.wildCardToken
      }

      val end = endLabel match {
        case Some((Some(token), _)) =>  body.constantExpression(Int.box(token))
        case Some((None, name)) => body.loadVariable(s"${variable.name}EndOf$name")
        case _ => body.wildCardToken
      }

      val rels = relTypes.map {
        case (Some(token), _) =>  body.constantExpression(Int.box(token))
        case (None, name) => body.loadVariable(s"${variable.name}TypeOf$name")
      }


      if (rels.isEmpty) {
        body.incrementDbHits()
        body.assign(variable.name, variable.codeGenType,
                    generator.relCountFromCountStore(start, end, body.wildCardToken))
      } else {
        for (i <- 1 to rels.size) body.incrementDbHits()
        body.assign(variable.name, variable.codeGenType,
                    generator.relCountFromCountStore(start, end, rels:_*))
      }
      inner.body(body)
    }
  }

  override def operatorId: Set[String] = Set(opName)

  override def children = Seq(inner)

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
    super.init(generator)
    startLabel.foreach {
      case (token, name) if token.isEmpty =>
        generator.lookupLabelId(s"${variable.name}StartOf$name", name)
    }
    endLabel.foreach {
      case (token, name) if token.isEmpty =>
        generator.lookupLabelId(s"${variable.name}EndOf$name", name)
    }
    relTypes.foreach {
      case (token, name) if token.isEmpty =>
        generator.lookupRelationshipTypeId(s"${variable.name}TypeOf$name", name)
    }
  }

  private def tokenVar = s"${variable.name}LabelToken"
}
