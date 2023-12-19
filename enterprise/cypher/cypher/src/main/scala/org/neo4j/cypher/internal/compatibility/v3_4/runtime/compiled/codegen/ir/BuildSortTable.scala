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
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions.CodeGenExpression
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi._

case class BuildSortTable(opName: String, tableName: String, columnVariables: Map[String, Variable],
                          sortItems: Iterable[SortItem], estimateCardinality: Double)
                         (implicit context: CodeGenContext)
  extends BuildSortTableBase(opName, tableName, columnVariables, sortItems)
{

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    // Use estimated cardinality to decide initial capacity
    // Since we cannot trust this value we cap it within reasonable limits
    val initialCapacity = Math.max(128.0, Math.min(estimateCardinality, (Int.MaxValue / 2).toDouble)).toInt

    generator.allocateSortTable(tableName, tableDescriptor, generator.constantExpression(Int.box(initialCapacity)))
  }

  override protected def tableDescriptor = FullSortTableDescriptor(tupleDescriptor)
}

case class BuildTopTable(opName: String, tableName: String, countExpression: CodeGenExpression,
                         columnVariables: Map[String, Variable], sortItems: Iterable[SortItem])
                        (implicit context: CodeGenContext)
  extends BuildSortTableBase(opName, tableName, columnVariables, sortItems)
{
  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    countExpression.init(generator)

    // Return early on limit <= 0 (this unifies the behaviour with the normal limit implementation)
    val variableName = context.namer.newVarName()
    generator.declareCounter(variableName, generator.box(countExpression.generateExpression(generator), countExpression.codeGenType))
    generator.ifStatement(generator.checkInteger(variableName, LessThanEqual, 0L)) { onTrue =>
      onTrue.returnSuccessfully()
    }
    generator.allocateSortTable(tableName, tableDescriptor, generator.loadVariable(variableName))
  }

  override protected def tableDescriptor = TopTableDescriptor(tupleDescriptor)
}

abstract class BuildSortTableBase(opName: String, tableName: String, columnVariables: Map[String, Variable],
                                  sortItems: Iterable[SortItem])
                                 (implicit context: CodeGenContext)
  extends Instruction
{
  override def body[E](generator: MethodStructure[E])(implicit ignored: CodeGenContext): Unit = {
    generator.trace(opName, Some(this.getClass.getSimpleName)) { body =>
      val tuple = body.newTableValue(context.namer.newVarName(), tupleDescriptor)
      fieldToVariableInfo.foreach {
        case (fieldName: String, info: FieldAndVariableInfo) =>
          body.putField(tupleDescriptor, tuple, fieldName, info.incomingVariable.name)
      }
      body.sortTableAdd(tableName, tableDescriptor, tuple)
    }
  }

  override protected def children = Seq.empty

  override protected def operatorId = Set(opName)

  protected def tableDescriptor: SortTableDescriptor

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

  protected val tupleDescriptor = OrderableTupleDescriptor(
    structure = fieldToVariableInfo.mapValues(c => c.outgoingVariable.codeGenType),
    sortItems
  )

  val sortTableInfo: SortTableInfo = SortTableInfo(
    tableName,
    fieldToVariableInfo,
    outgoingVariableNameToVariableInfo,
    tableDescriptor
  )
}

case class SortTableInfo(tableName: String,
                         fieldToVariableInfo: Map[String, FieldAndVariableInfo],
                         outgoingVariableNameToVariableInfo: Map[String, FieldAndVariableInfo],
                         tableDescriptor: SortTableDescriptor)

case class FieldAndVariableInfo(fieldName: String,
                                queryVariableName: String,
                                incomingVariable: Variable,
                                outgoingVariable: Variable)