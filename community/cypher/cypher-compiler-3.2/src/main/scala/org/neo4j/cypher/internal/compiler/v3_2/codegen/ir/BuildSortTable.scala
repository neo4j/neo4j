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

import org.neo4j.cypher.internal.compiler.v3_2.codegen._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi._

case class BuildSortTable(opName: String, tableName: String, columnVariables: Map[String, Variable],
                          sortItems: Iterable[SortItem])
                         (implicit context: CodeGenContext)
  extends Instruction
{

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    val initialCapacity = 128 // TODO: Use value from cardinality estimation if possible
    generator.allocateSortTable(tableName, initialCapacity, tupleDescriptor)
  }

  override def body[E](generator: MethodStructure[E])(implicit ignored: CodeGenContext): Unit = {
    generator.trace(opName, Some(this.getClass.getSimpleName)) { body =>
      val tuple = body.newTableValue(context.namer.newVarName(), tupleDescriptor)
      fieldToVariableInfo.foreach {
        case (fieldName: String, info: FieldAndVariableInfo) =>
          body.putField(tupleDescriptor, tuple, fieldName, info.incomingVariable.name)
      }
      body.sortTableAdd(tableName, tupleDescriptor, tuple)
    }
  }

  override protected def children = Seq.empty

  override protected def operatorId = Set(opName)

  private val fieldToVariableInfo: Map[String, FieldAndVariableInfo] = columnVariables.map {
    case (queryVariableName: String, incoming: Variable) =>
      val fieldName = CodeGenContext.sanitizedName(queryVariableName) // < Name the field after the query variable
      (fieldName,
        FieldAndVariableInfo(
          fieldName = fieldName,
          queryVariableName = queryVariableName,
          incomingVariable = incoming,
          outgoingVariable = incoming.copy(name = context.namer.newVarName())))
  }

  private val outgoingVariableNameToVariableInfo: Map[String, FieldAndVariableInfo] =
    fieldToVariableInfo.map {
      case (fieldName, info) => info.outgoingVariable.name -> info
    }

  private val tupleDescriptor = OrderableTupleDescriptor(
      structure = fieldToVariableInfo.mapValues(c => c.outgoingVariable.codeGenType),
      sortItems
  )

  val sortTableInfo: SortTableInfo = SortTableInfo(
    tableName,
    fieldToVariableInfo,
    outgoingVariableNameToVariableInfo,
    tupleDescriptor
  )
}

case class SortTableInfo(tableName: String,
                         fieldToVariableInfo: Map[String, FieldAndVariableInfo],
                         outgoingVariableNameToVariableInfo: Map[String, FieldAndVariableInfo],
                         tupleDescriptor: OrderableTupleDescriptor)

case class FieldAndVariableInfo(fieldName: String,
                                queryVariableName: String,
                                incomingVariable: Variable,
                                outgoingVariable: Variable)