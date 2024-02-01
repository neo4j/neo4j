/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.composite

import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.PlannerQueryBuilder
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.StatementConverters
import org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery.composite.CompositeQuery.Fragment
import org.neo4j.cypher.internal.compiler.helpers.SeqSupport.RichSeq
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.rendering.QueryRenderer

object CompositeQueryConverter {

  /**
   * Builds a planner query, the intermediate representation of a query, out of the fragments making up a composite query.
   */
  def convert(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    semanticTable: SemanticTable,
    query: CompositeQuery
  ): ir.PlannerQuery =
    query match {
      case foreign: CompositeQuery.Single.Foreign =>
        convertForeign(semanticTable, foreign)
      case fragments: CompositeQuery.Single.Fragments =>
        convertTopLevelFragments(cancellationChecker, nameGenerator, semanticTable, fragments)
      case union: CompositeQuery.Union =>
        convertUnion(cancellationChecker, nameGenerator, semanticTable, union)
    }

  private def convertForeign(
    semanticTable: SemanticTable,
    foreign: CompositeQuery.Single.Foreign
  ): ir.SinglePlannerQuery = {
    val builder = PlannerQueryBuilder(semanticTable, foreign.parameters.view.values.toSet)
    builder
      .withHorizon(ir.RunQueryAtProjection(
        graphReference = foreign.graphReference,
        queryString = QueryRenderer.render(foreign.clauses),
        parameters = foreign.parameters,
        columns = foreign.clauses.last.returnVariables.explicitVariables.toSet
      ))
      .build()
  }

  /**
   * Special handling for a top-level single query:
   *   - it does not have arguments (no importing with).
   *   - if it ends in standard clauses, the last horizon must be marked as being in final position.
   */
  private def convertTopLevelFragments(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    semanticTable: SemanticTable,
    fragments: CompositeQuery.Single.Fragments
  ): ir.SinglePlannerQuery = {
    // A top-level query does not have arguments.
    val builder = PlannerQueryBuilder(semanticTable, argumentIds = Set.empty)
    val withFragments = fragments.fragments.initAndLastOption match {
      // if the query ends in standard clauses
      case Some((initFragments, Fragment.Standard(lastClauses))) =>
        // we apply all the fragments before that
        val withInitFragments = addFragmentsToPlannerQueryBuilder(
          cancellationChecker,
          nameGenerator,
          semanticTable,
          builder,
          initFragments
        )

        // and add the last clauses, marked as being in final position. This is used for eagerness analysis.
        StatementConverters.addClausesToPlannerQueryBuilder(
          lastClauses,
          withInitFragments,
          nameGenerator,
          cancellationChecker,
          QueryProjection.Position.Final
        )

      // otherwise we simply add all the fragments.
      case _ => addFragmentsToPlannerQueryBuilder(
          cancellationChecker,
          nameGenerator,
          semanticTable,
          builder,
          fragments.fragments
        )
    }
    withFragments.build()
  }

  private def convertUnion(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    semanticTable: SemanticTable,
    union: CompositeQuery.Union
  ): ir.PlannerQuery =
    ir.UnionQuery(
      lhs = convertNestedQuery(cancellationChecker, nameGenerator, semanticTable, union.lhs),
      rhs = convertNestedSingle(cancellationChecker, nameGenerator, semanticTable, union.rhs),
      distinct = isDistinct(union.unionType),
      unionMappings = union.unionMappings
    )

  private def isDistinct(unionType: CompositeQuery.Union.Type): Boolean =
    unionType match {
      case CompositeQuery.Union.Type.All      => false
      case CompositeQuery.Union.Type.Distinct => true
    }

  /**
   * Converts a sub-query or the member of a union.
   */
  private def convertNestedQuery(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    semanticTable: SemanticTable,
    query: CompositeQuery
  ): ir.PlannerQuery =
    query match {
      case single: CompositeQuery.Single =>
        convertNestedSingle(cancellationChecker, nameGenerator, semanticTable, single)
      case union: CompositeQuery.Union =>
        convertUnion(cancellationChecker, nameGenerator, semanticTable, union)
    }

  private def convertNestedSingle(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    semanticTable: SemanticTable,
    single: CompositeQuery.Single
  ): ir.SinglePlannerQuery =
    single match {
      case foreign: CompositeQuery.Single.Foreign => convertForeign(semanticTable, foreign)
      case fragments: CompositeQuery.Single.Fragments =>
        val builder = PlannerQueryBuilder(semanticTable, fragments.arguments)
        val withFragments = addFragmentsToPlannerQueryBuilder(
          cancellationChecker,
          nameGenerator,
          semanticTable,
          builder,
          fragments.fragments
        )
        withFragments.build()
    }

  private def addFragmentsToPlannerQueryBuilder(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    semanticTable: SemanticTable,
    builder: PlannerQueryBuilder,
    fragments: Seq[CompositeQuery.Fragment]
  ): PlannerQueryBuilder = {
    // This function is the workhorse of the whole module, where the assembling of the planner query really happens.
    // We probe the cancellation checker to interrupt the conversion process if the query has been cancelled.
    cancellationChecker.throwIfCancelled()

    fragments.foldLeft(builder) { (builder, fragment) =>
      fragment match {
        case standard: Fragment.Standard =>
          StatementConverters.addClausesToPlannerQueryBuilder(
            standard.clauses,
            builder,
            nameGenerator,
            cancellationChecker,
            QueryProjection.Position.Intermediate
          )

        case subQuery: Fragment.SubQuery =>
          builder.withCallSubquery(
            subquery = convertNestedQuery(cancellationChecker, nameGenerator, semanticTable, subQuery.innerQuery),
            correlated = subQuery.isCorrelated,
            yielding = subQuery.isYielding,
            inTransactionsParameters = subQuery.inTransactionsParameters
          )
      }
    }
  }
}
