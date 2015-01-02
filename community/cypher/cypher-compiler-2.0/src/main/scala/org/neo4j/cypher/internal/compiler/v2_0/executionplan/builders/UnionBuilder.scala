/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.executionplan.builders

import org.neo4j.cypher.internal.compiler.v2_0.commands.{Query, Union}
import org.neo4j.cypher.internal.compiler.v2_0.pipes.{DistinctPipe, UnionPipe, Pipe}
import org.neo4j.cypher.internal.compiler.v2_0.commands.expressions.{Identifier, Expression}
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext


class UnionBuilder(queryBuilder: {def buildQuery(q: Query, context:PlanContext): (Pipe, Boolean)}) {
  def buildUnionQuery(union: Union, context:PlanContext): (Pipe, Boolean) = {
    checkQueriesHaveSameColumns(union)

    val combined = union.queries.map( q => queryBuilder.buildQuery(q, context))

    val pipes = combined.map(_._1)
    val updating = combined.map(_._2).reduce(_ || _)

    val unionPipe = new UnionPipe(pipes, union.queries.head.columns)
    val pipe = if (union.distinct) {
      val expressions: Map[String, Expression] = union.queries.head.columns.map(k => k -> Identifier(k)).toMap
      new DistinctPipe(unionPipe, expressions)
    } else {
      unionPipe
    }

    (pipe, updating)
  }

  private def checkQueriesHaveSameColumns(union: Union) {
    val allColumns: Seq[List[String]] = union.queries.map(_.columns)
    val first = allColumns.head
    val allTheSame = allColumns.forall(x => x == first)

    if (!allTheSame) {
      throw new SyntaxException("All sub queries in an UNION must have the same column names")
    }
  }
}