/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.compatibility.v3_5.runtime

import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted.InterpretedPipeBuilder
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.interpreted.pipes.{OwningPipeAsserter, Pipe, PipeBuilderFactory, PipeExecutionBuilderContext}
import org.neo4j.cypher.internal.v3_5.logical.plans.{LogicalPlan, LogicalPlans}

class PipeExecutionPlanBuilder(pipeBuilderFactory: PipeBuilderFactory,
                               expressionConverters: ExpressionConverters) {
  def build(plan: LogicalPlan)
           (implicit context: PipeExecutionBuilderContext, tokenContext: TokenContext): Pipe = {

    val pipe = buildPipe(plan)
    OwningPipeAsserter.assertAllExpressionsHaveAnOwningPipe(pipe)
    pipe
  }

  private def buildPipe(plan: LogicalPlan)(implicit context: PipeExecutionBuilderContext, tokenContext: TokenContext): Pipe = {
    val pipeBuilder = pipeBuilderFactory(recurse = p => buildPipe(p),
                                         readOnly = context.readOnly,
                                         expressionConverters = expressionConverters)
    LogicalPlans.map(plan, pipeBuilder)
  }
}

object InterpretedPipeBuilderFactory extends PipeBuilderFactory {
  def apply(recurse: LogicalPlan => Pipe,
            readOnly: Boolean,
            expressionConverters: ExpressionConverters)
           (implicit context: PipeExecutionBuilderContext, tokenContext: TokenContext): InterpretedPipeBuilder = {
    InterpretedPipeBuilder(recurse, readOnly, expressionConverters, recursePipes(recurse), tokenContext)(context.semanticTable)
  }
}
