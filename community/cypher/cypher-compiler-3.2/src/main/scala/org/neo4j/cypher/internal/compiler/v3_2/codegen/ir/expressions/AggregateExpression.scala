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

trait AggregateExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def update[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit

  def continuation(instruction: Instruction): Instruction = instruction
}



case class SimpleCount(variable: Variable, expression: CodeGenExpression, distinct: Boolean) extends AggregateExpression {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    generator.assign(variable.name, CodeGenType.primitiveInt, generator.constantExpression(Long.box(0L)))
    if (distinct) generator.newSet(setName(variable))
  }

  def update[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    val tmpName = context.namer.newVarName()
    structure.assign(tmpName, CodeGenType.Any, expression.generateExpression(structure))
    structure.ifNonNullStatement(structure.loadVariable(tmpName)) { body =>
      condition(tmpName, body) { inner =>
        inner.incrementInteger(variable.name)
      }
    }
  }

  private def condition[E](name: String, structure: MethodStructure[E])(block: MethodStructure[E] => Unit) = {
    if (distinct) {
      structure.ifNotStatement(structure.setContains(setName(variable), structure.loadVariable(name))) { inner =>
        inner.addToSet(setName(variable), inner.loadVariable(name))
        block(inner)
      }
    } else block(structure)
  }

  private def setName(variable: Variable) = variable.name + "Set"
}

class DynamicCount(opName: String, variable: Variable, expression: CodeGenExpression,
                   groupingKey: Iterable[Variable], distinct: Boolean) extends AggregateExpression {

  private var mapName: String = null

  override def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    mapName = context.namer.newVarName()
    generator.newAggregationMap(mapName, groupingKey.map(_.codeGenType).toIndexedSeq, distinct)
  }

  override def update[E](structure: MethodStructure[E])(implicit context: CodeGenContext) = {
    val localVar = context.namer.newVarName()
    structure.aggregationMapGet(mapName, localVar, createKey(structure))
    condition(structure, expression.generateExpression(structure)) { inner =>
      inner.incrementInteger(localVar)
    }
    structure.aggregationMapPut(mapName, createKey(structure),
                                structure.loadVariable(localVar))
  }

  private def condition[E](structure: MethodStructure[E], value: E)(block: MethodStructure[E] => Unit)(implicit context: CodeGenContext) = {
    if (distinct) {
      structure.ifNonNullStatement(value) { i1 =>
        i1.checkDistinct(mapName, createKey(structure), value) { i2 =>
          block(i2)
        }
      }
    } else {
      structure.ifNonNullStatement(value) { inner =>
        block(inner)
      }
    }
  }

  private def createKey[E](body: MethodStructure[E])(implicit context: CodeGenContext): IndexedSeq[(CodeGenType, E)] = {
    groupingKey.map(e => (e.codeGenType, body.loadVariable(e.name))).toIndexedSeq
  }

  override def continuation(instruction: Instruction): Instruction = new Instruction {

    override protected def children: Seq[Instruction] = Seq(instruction)

    override protected def operatorId = Set(opName)

    override def body[E](generator: MethodStructure[E])(implicit context: CodeGenContext): Unit = {
      generator.trace(opName) { body =>
        val keyArg = groupingKey.map(k => k.name -> k.codeGenType).toIndexedSeq
        body.aggregationMapIterate(mapName, keyArg, variable.name) { inner =>
          instruction.body(inner)
        }
      }
    }
  }
}
