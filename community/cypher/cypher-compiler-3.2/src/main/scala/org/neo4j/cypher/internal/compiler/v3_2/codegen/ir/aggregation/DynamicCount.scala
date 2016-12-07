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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.Instruction
import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, Variable}

/*
 * Dynamic count is used when a grouping key is defined. such as
 * `MATCH (n) RETURN n.prop1 count(n.prop2)`
 */
class DynamicCount(opName: String, variable: Variable, expression: CodeGenExpression,
                   groupingKey: Iterable[Variable], distinct: Boolean) extends BaseAggregateExpression(expression, distinct) {

  private var mapName: String = null
  private var keyVar: String = null

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    mapName = context.namer.newVarName()
    val key = groupingKey.map(_.codeGenType).toIndexedSeq
    generator.newAggregationMap(mapName, key)
    if (distinct) {
      generator.newMapOfSets(seenSet, key, internalExpressionType)
    }
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
    structure.checkDistinct(seenSet, createKey(structure), keyVar, value, valueType) { inner =>
      block(inner)
    }
  }

  private def seenSet = mapName + "Seen"

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
          inner.incrementRows()
          instruction.body(inner)
        }
      }
    }
  }
}
