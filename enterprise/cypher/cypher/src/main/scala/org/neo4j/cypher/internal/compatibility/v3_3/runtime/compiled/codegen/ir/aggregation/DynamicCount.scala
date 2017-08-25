/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.ir.Instruction
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.spi.{HashableTupleDescriptor, MethodStructure}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.compiled.codegen.{CodeGenContext, Variable}

/*
 * Dynamic count is used when a grouping key is defined. such as
 * `MATCH (n) RETURN n.prop1 count(n.prop2)`
 */
class DynamicCount(opName: String, variable: Variable, expression: CodeGenExpression,
                   groupingKey: Iterable[(String,CodeGenExpression)], distinct: Boolean) extends BaseAggregateExpression(expression, distinct) {

  private var mapName: String = null
  private var keyVar: String = null

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    mapName = context.namer.newVarName()
    val key = groupingKey.map(_._2.codeGenType).toIndexedSeq
    groupingKey.foreach {
      case (_, e) => e.init(generator)
    }
    generator.newAggregationMap(mapName, key)
    if (distinct) {
      generator.newMapOfSets(seenSet, key, expression.codeGenType)
    }
  }

  override def update[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    keyVar = context.namer.newVarName()
    val valueVar = context.namer.newVarName()
    groupingKey.foreach {
      case (v, expr) =>
        structure.declare(v, expr.codeGenType)
        if (expr.codeGenType == CodeGenType.Any) structure.assign(v, expr.codeGenType,
                                                                  structure.materializeAny(expr.generateExpression(structure)))
        else structure.assign(v, expr.codeGenType, expr.generateExpression(structure))
    }
    structure.aggregationMapGet(mapName, valueVar, createKey(structure), keyVar)
    ifNotNull(structure) { inner =>
      inner.incrementInteger(valueVar)
    }
    structure.aggregationMapPut(mapName, createKey(structure), keyVar, structure.loadVariable(valueVar))
  }

  def distinctCondition[E](value: E, valueType: CodeGenType, structure: MethodStructure[E])(block: MethodStructure[E] => Unit)
                          (implicit context: CodeGenContext) = {
    structure.checkDistinct(seenSet, createKey(structure), keyVar, value, valueType) { inner =>
      block(inner)
    }
  }

  private def seenSet = mapName + "Seen"

  private def createKey[E](body: MethodStructure[E])(implicit context: CodeGenContext) = {
    groupingKey.map(e => e._1 -> (e._2.codeGenType -> body.loadVariable(e._1))).toMap
  }

  override def continuation(instruction: Instruction): Instruction = new Instruction {

    override protected def children: Seq[Instruction] = Seq(instruction)

    override protected def operatorId = Set(opName)

    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
      generator.trace(opName) { body =>
        val keyArg = groupingKey.map(k => k._1 -> k._2.codeGenType).toMap
        body.aggregationMapIterate(mapName, HashableTupleDescriptor(keyArg), variable.name) { inner =>
          inner.incrementRows()
          instruction.body(inner)
        }
      }
    }
  }
}
