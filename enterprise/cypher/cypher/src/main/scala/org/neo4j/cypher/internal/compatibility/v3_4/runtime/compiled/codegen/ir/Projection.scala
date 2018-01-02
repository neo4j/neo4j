/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.{CodeGenExpression, CodeGenType}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

case class Projection(projectionOpName: String, variables: Map[Variable, CodeGenExpression], action: Instruction)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    super.init(generator)
    variables.foreach {
      case (_, expr) => expr.init(generator)
    }
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(projectionOpName) { body =>
      body.incrementRows()
      variables.foreach {
        case (variable, expr) =>
          body.declare(variable.name, variable.codeGenType)
          if (variable.codeGenType == CodeGenType.Any) body.assign(variable.name, variable.codeGenType, body.materializeAny(expr.generateExpression(body)))
          else body.assign(variable.name, variable.codeGenType, expr.generateExpression(body))
      }
      action.body(body)
    }
  }

  override protected def operatorId = Set(projectionOpName)

  override protected def children = Seq(action)
}
