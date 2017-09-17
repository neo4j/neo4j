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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime

import org.neo4j.cypher.internal.apa.v3_4.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.{NestedPipeExpression, Pipe}
import org.neo4j.cypher.internal.compiler.v3_4.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v3_4.{ast => compilerAst}
import org.neo4j.cypher.internal.frontend.v3_4.phases.Monitors
import org.neo4j.cypher.internal.v3_4.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.v3_4.{expressions => frontEndAst}

trait PipeBuilderFactory {

  def apply(monitors: Monitors,
            recurse: LogicalPlan => Pipe,
            readOnly: Boolean,
            expressionConverters: ExpressionConverters)
           (implicit context: PipeExecutionBuilderContext, planContext: PlanContext): PipeBuilder

  protected def recursePipes(recurse: LogicalPlan => Pipe, planContext: PlanContext)
                            (in: frontEndAst.Expression): frontEndAst.Expression = {

    val buildPipeExpressions = new Rewriter {
      private val instance = bottomUp(Rewriter.lift {
        case expr@compilerAst.NestedPlanExpression(patternPlan, expression) =>
          val pipe = recurse(patternPlan)
          val result = NestedPipeExpression(pipe, expression)(expr.position)
          result
      })

      override def apply(that: AnyRef): AnyRef = instance.apply(that)
    }
    in.endoRewrite(buildPipeExpressions)

  }

}
