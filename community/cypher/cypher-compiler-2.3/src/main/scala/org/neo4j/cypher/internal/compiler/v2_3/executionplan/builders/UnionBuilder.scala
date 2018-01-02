/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_3.PlannerName
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression, Identifier}
import org.neo4j.cypher.internal.compiler.v2_3.commands.{Query, Union}
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.PipeInfo
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{DistinctPipe, PipeMonitor, UnionPipe}
import org.neo4j.cypher.internal.compiler.v2_3.spi.PlanContext


trait GraphQueryBuilder {
  def buildQuery(q: Query, context: PlanContext)(implicit pipeMonitor: PipeMonitor): PipeInfo
}

class UnionBuilder(queryBuilder: GraphQueryBuilder) {
  def buildUnionQuery(union: Union, context: PlanContext, plannerName: PlannerName)(implicit pipeMonitor: PipeMonitor): PipeInfo = {
    val combined = union.queries.map( q => queryBuilder.buildQuery(q, context))

    val pipes = combined.map(_.pipe)
    val updating = combined.map(_.updating).reduce(_ || _)

    val unionPipe = new UnionPipe(pipes.toList, union.queries.head.columns)
    val pipe = if (union.distinct) {
      val expressions: Map[String, Expression] = union.queries.head.columns.map(k => k -> Identifier(k)).toMap
      new DistinctPipe(unionPipe, expressions)()
    } else {
      unionPipe
    }

    PipeInfo(pipe, updating, plannerUsed = plannerName)
  }
}
