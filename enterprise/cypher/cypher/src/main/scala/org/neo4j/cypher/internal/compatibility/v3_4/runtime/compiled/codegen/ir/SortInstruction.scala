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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.CodeGenContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure

case class SortInstruction(opName: String,
                           sortTableInfo: SortTableInfo)
  extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
  }

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    generator.trace(opName, Some(this.getClass.getSimpleName)) { body =>
      body.sortTableSort(sortTableInfo.tableName, sortTableInfo.tableDescriptor)
    }
  }

  override protected def children: Seq[Instruction] = Seq.empty

  override protected def operatorId = Set(opName)
}
