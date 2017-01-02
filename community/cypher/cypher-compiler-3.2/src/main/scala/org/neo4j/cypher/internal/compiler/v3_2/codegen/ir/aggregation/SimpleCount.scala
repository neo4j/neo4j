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
package org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.aggregation

import org.neo4j.cypher.internal.compiler.v3_2.codegen.ir.expressions._
import org.neo4j.cypher.internal.compiler.v3_2.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compiler.v3_2.codegen.{CodeGenContext, Variable}

/*
 * Simple count is used when no grouping key is defined such as
 * `MATCH (n) RETURN count(n.prop)`
 */
case class SimpleCount(variable: Variable, expression: CodeGenExpression, distinct: Boolean)
  extends BaseAggregateExpression(expression, distinct) {

  def init[E](generator: MethodStructure[E])(implicit context: CodeGenContext) = {
    expression.init(generator)
    generator.assign(variable.name, CodeGenType.primitiveInt, generator.constantExpression(Long.box(0L)))
    if (distinct) {
      generator.newDistinctSet(setName(variable), Seq(expression.codeGenType))
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

    structure.distinctSetIfNotContains(
      setName(variable), Map(typeName(variable) -> (expression.codeGenType -> expression.generateExpression(structure))))(block)
  }

  private def setName(variable: Variable) = variable.name + "Set"

  private def typeName(variable: Variable) = variable.name + "Type"
}


