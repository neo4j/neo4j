/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.Instruction
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, MethodStructure, Variable}

abstract class AggregateExpression(expression: CodeGenExpression, distinct: Boolean) {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def continuation(instruction: Instruction): Instruction = instruction

  def distinctCondition[E](value: E, valueType: CodeGenType, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext)

  protected def ifNotNull[E](structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext) = {
    expression match {
      case NodeExpression(v) => primitiveIfNot(v, structure)(block(_))
      case NodeProjection(v) => primitiveIfNot(v, structure)(block(_))
      case RelationshipExpression(v) => primitiveIfNot(v, structure)(block(_))
      case RelationshipProjection(v) => primitiveIfNot(v, structure)(block(_))
      case _ =>
        val tmpName = context.namer.newVarName()
        structure.assign(tmpName, expression.codeGenType, expression.generateExpression(structure))
        structure.ifNonNullStatement(structure.loadVariable(tmpName)) { body =>
          if (distinct) {
            distinctCondition(structure.loadVariable(tmpName),expression.codeGenType, body) { inner =>
              block(inner)
            }
          }
          else block(body)
        }
    }
  }

  private def primitiveIfNot[E](v: Variable, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                               (implicit context: CodeGenContext) = {
    structure.ifNotStatement(structure.equalityExpression(structure.loadVariable(v.name),
                                                          structure.constantExpression(Long.box(-1)),
                                                          CodeGenType.primitiveInt)) { body =>
      if (distinct) {
        distinctCondition(structure.loadVariable(v.name), CodeGenType.primitiveInt, body) { inner =>
          block(inner)
        }
      }
      else block(body)
    }
  }
}

case class SimpleCount(variable: Variable, expression: CodeGenExpression, distinct: Boolean)
  extends AggregateExpression(expression, distinct) {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    generator.assign(variable.name, CodeGenType.primitiveInt, generator.constantExpression(Long.box(0L)))
    if (distinct) {
      generator.newSet(setName(variable))
    }
  }

  def update[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    ifNotNull(structure) { inner =>
      inner.incrementInteger(variable.name)
    }
  }

  def distinctCondition[E](value: E, valueType: CodeGenType, structure: MethodStructure[E])
                          (block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext) = {
    val tmpName = context.namer.newVarName()
    structure.newUniqueAggregationKey(tmpName, Map(typeName(variable) -> (valueType -> value)))
    structure.ifNotStatement(structure.setContains(setName(variable), structure.loadVariable(tmpName))) { inner =>
      inner.addToSet(setName(variable), inner.loadVariable(tmpName))
      block(inner)
    }
  }

  private def setName(variable: Variable) = variable.name + "Set"

  private def typeName(variable: Variable) = variable.name + "Type"
}

class DynamicCount(opName: String, variable: Variable, expression: CodeGenExpression,
                   groupingKey: Iterable[Variable], distinct: Boolean) extends AggregateExpression(expression, distinct) {

  private var mapName: String = null
  private var keyVar: String = null

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    mapName = context.namer.newVarName()
    generator.newAggregationMap(mapName, groupingKey.map(_.codeGenType).toIndexedSeq, distinct)
  }

  override def update[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    keyVar = context.namer.newVarName()
    val valueVar = context.namer.newVarName()
    structure.aggregationMapGet(mapName, valueVar, createKey(structure), keyVar)
    ifNotNull(structure) { inner =>
      inner.incrementInteger(valueVar)
    }
    structure.aggregationMapPut(mapName, createKey(structure), keyVar, structure.loadVariable(valueVar))
  }

  def distinctCondition[E](value: E, valueType: CodeGenType, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext) = {
    structure.checkDistinct(mapName, createKey(structure), keyVar, value, expression.codeGenType) { inner =>
      block(inner)
    }
  }

  private def createKey[E](body: MethodStructure[E])(implicit context: CodeGenContext) = {
    groupingKey.map(e => e.name -> (e.codeGenType -> body.loadVariable(e.name))).toMap
  }

  override def continuation(instruction: Instruction): Instruction = new Instruction {

    override protected def children: Seq[Instruction] = Seq(instruction)

    override protected def operatorId = Set(opName)

    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
      generator.trace(opName) { body =>
        val keyArg = groupingKey.map(k => k.name -> k.codeGenType).toMap
        body.aggregationMapIterate(mapName, keyArg, variable.name) { inner =>
          instruction.body(inner)
        }
      }
    }
  }
}
