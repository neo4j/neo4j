/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
          // Only materialize in produce results
          body.assign(variable.name, variable.codeGenType, expr.generateExpression(body))
      }
      action.body(body)
    }
  }

  override protected def operatorId = Set(projectionOpName)

  override protected def children = Seq(action)
}
