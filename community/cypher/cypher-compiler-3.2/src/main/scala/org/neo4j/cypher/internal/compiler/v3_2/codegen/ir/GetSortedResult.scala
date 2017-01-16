/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, Variable}

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
      l1.sortTableIterate(sortTableInfo.tableName, sortTableInfo.valueStructure, variablesToGetFromFields) { l2 =>
        l2.incrementRows()
        action.body(l2)
      }
    }

  override def children = Seq(action)

  override protected def operatorId = Set(opName)
}
