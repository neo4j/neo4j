/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.aggregation

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.ir.expressions._
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.spi.MethodStructure
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.compiled.codegen.{CodeGenContext, Variable}

/*
 * Simple count is used when no grouping key is defined such as
 * `MATCH (n) RETURN count(n.prop)`
 */
case class SimpleCount(opName: String, variable: Variable, expression: CodeGenExpression, distinct: Boolean)
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


