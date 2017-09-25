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
package org.neo4j.cypher.internal.compiler.v3_4.phases

import org.neo4j.cypher.internal.compiler.v3_4.ast.convert.plannerQuery.StatementConverters._
import org.neo4j.cypher.internal.frontend.v3_4.InternalException
import org.neo4j.cypher.internal.frontend.v3_4.ast.Query
import org.neo4j.cypher.internal.frontend.v3_4.phases.CompilationPhaseTracer.CompilationPhase.LOGICAL_PLANNING
import org.neo4j.cypher.internal.frontend.v3_4.phases.{BaseContext, BaseState, Phase}
import org.neo4j.cypher.internal.ir.v3_4.UnionQuery

object CreatePlannerQuery extends Phase[BaseContext, BaseState, LogicalPlanState] {
  override def phase = LOGICAL_PLANNING

  override def description = "from the normalized ast, create the corresponding PlannerQuery"

  override def postConditions = Set(CompilationContains[UnionQuery])

  override def process(from: BaseState, context: BaseContext): LogicalPlanState = from.statement() match {
    case query: Query =>
      val unionQuery: UnionQuery = toUnionQuery(query, from.semanticTable())
      LogicalPlanState(from).copy(maybeUnionQuery = Some(unionQuery))

    case x => throw new InternalException(s"Expected a Query and not `$x`")
  }
}
