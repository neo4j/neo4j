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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}
import org.neo4j.cypher.internal.util.v3_4.symbols

case class IdOf(variable: Variable) extends CodeGenExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {}

  def generateExpression[E](structure: MethodStructure[E])(implicit context: CodeGenContext): E =
    if (nullable) structure.nullableReference(variable.name, variable.codeGenType,
                                              structure.box(structure.loadVariable(variable.name),
                                                            CypherCodeGenType(symbols.CTInteger, ReferenceType)))
    else structure.loadVariable(variable.name)

  override def nullable(implicit context: CodeGenContext): Boolean = variable.nullable

  override def codeGenType(implicit context: CodeGenContext): CypherCodeGenType =
    if (nullable) CypherCodeGenType(symbols.CTInteger, ReferenceType) else CodeGenType.primitiveInt
}
