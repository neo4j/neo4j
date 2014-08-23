/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.convert.plannerQuery.ClauseConverters._
import org.neo4j.cypher.internal.compiler.v2_2.planner._

object StatementConverters {
  object SingleQueryPlanInput {
    val empty = new PlannerQueryBuilder(PlannerQuery.empty, Map.empty)
  }

  implicit class QueryConverter(val query: Query) {
    def asQueryPlanInput: QueryPlanInput = query match {
      case Query(None, SingleQuery(clauses)) =>
        val input = clauses.foldLeft(SingleQueryPlanInput.empty) {
          case (acc, clause) => clause.addToQueryPlanInput(acc)
        }

        val singeQueryPlanInput = input.build()

        QueryPlanInput(
          query = UnionQuery(Seq(singeQueryPlanInput), distinct = false),
          patternInExpression = input.patternExprTable
        )

      case _ =>
        throw new CantHandleQueryException
    }

  }
}
