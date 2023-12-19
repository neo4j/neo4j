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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure

case class MethodInvocation(override val operatorId: Set[String],
                            symbol:JoinTableMethod,
                            methodName: String,
                            statements: Seq[Instruction]) extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {}

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.invokeMethod(symbol.tableType, symbol.name, methodName) { body =>
      statements.foreach(_.init(body))
      statements.foreach(_.body(body))
    }
  }

  override def children = statements
}
