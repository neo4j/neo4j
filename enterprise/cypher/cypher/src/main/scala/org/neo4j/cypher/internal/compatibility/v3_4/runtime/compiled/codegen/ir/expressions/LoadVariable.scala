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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions

import org.neo4j.cypher.internal.util.v3_4.InternalException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

case class LoadVariable(variable: Variable) extends CodeGenExpression {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext) =
    structure.loadVariable(variable.name)

  override def nullable(implicit context: CodeGenContext): Boolean = variable.nullable

  override def codeGenType(implicit context: CodeGenContext) = variable.codeGenType match {
    case x: CypherCodeGenType => x
    case _ => throw new InternalException("Tried to create a Cypher value from a non-cypher-value variable")
  }
}
