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

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

case class GetSortedResult(opName: String,
                           variablesToKeep: Map[String, Variable],
                           sortTableInfo: SortTableInfo,
                           action: Instruction) extends Instruction {

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    action.init(generator)

  override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext) =
    generator.trace(opName, Some(this.getClass.getSimpleName)) { l1 =>
      val variablesToGetFromFields = sortTableInfo.outgoingVariableNameToVariableInfo.collect {
        case (_, FieldAndVariableInfo(fieldName, queryVariableName, incoming, outgoing))
          if variablesToKeep.isDefinedAt(queryVariableName) => (outgoing.name, fieldName)
      }
      l1.sortTableIterate(sortTableInfo.tableName, sortTableInfo.tableDescriptor, variablesToGetFromFields) { l2 =>
        l2.incrementRows()
        action.body(l2)
      }
    }

  override def children = Seq(action)

  override protected def operatorId = Set(opName)
}
