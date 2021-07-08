/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.planner.logical.Metrics.QueryGraphSolverInput
import org.neo4j.cypher.internal.compiler.planner.logical.steps.index.IndexCompatiblePredicatesProviderContext
import org.neo4j.cypher.internal.ir.PlannerQueryPart
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.UnionQuery
import org.neo4j.cypher.internal.ir.helpers.CachedFunction
import org.neo4j.cypher.internal.util.Cardinality

class CachedStatisticsBackedCardinalityModel(wrapped: StatisticsBackedCardinalityModel) extends CardinalityModel {

  private val singleCached = CachedFunction[SinglePlannerQuery, Metrics.QueryGraphSolverInput, SemanticTable, IndexCompatiblePredicatesProviderContext, Cardinality] { (a, b, c, d) => wrapped.singlePlannerQueryCardinality(a, b, c, d) }
  private val unionCached = CachedFunction[UnionQuery, Cardinality, Cardinality, Cardinality] { (a, b, c) => wrapped.combineUnion(a, b, c) }
  private val cached = CachedFunction[PlannerQueryPart, Metrics.QueryGraphSolverInput, SemanticTable, IndexCompatiblePredicatesProviderContext, Cardinality] { (a, b, c, d) => cachedCardinality(a, b, c, d) }

  private def cachedCardinality(queryPart: PlannerQueryPart,
                                input: QueryGraphSolverInput,
                                semanticTable: SemanticTable,
                                indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): Cardinality = queryPart match {
    case singlePlannerQuery: SinglePlannerQuery => singleCached(singlePlannerQuery, input, semanticTable, indexPredicateProviderContext)
    case uq@UnionQuery(part, query, _, _) =>
      unionCached(uq,
        apply(part, input, semanticTable, indexPredicateProviderContext),
        apply(query, input, semanticTable, indexPredicateProviderContext))
  }

  override def apply(queryPart: PlannerQueryPart,
                     input: QueryGraphSolverInput,
                     semanticTable: SemanticTable,
                     indexPredicateProviderContext: IndexCompatiblePredicatesProviderContext): Cardinality =
    cached(queryPart, input, semanticTable, indexPredicateProviderContext)
}


