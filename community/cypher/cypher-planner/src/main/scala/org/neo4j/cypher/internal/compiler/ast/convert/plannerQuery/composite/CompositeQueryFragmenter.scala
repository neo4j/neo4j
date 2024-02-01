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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.compiler.helpers.SeqSupport.RichSeq
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.symbols.CTAny

object CompositeQueryFragmenter {

  /**
   * Recursively traverse an AST query and its sub-queries, grouping its clauses together into fragments based on which graph they need run against.
   * This is used in the process of turning an AST query into an IR planner query.
   */
  def fragment(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    query: ast.Query
  ): CompositeQuery = {
    // We must ensure that the parameters we generate do not clash with existing ones.
    // Lazily computed once to avoid duplicated effort.
    lazy val existingParameterNames = query.folder(cancellationChecker).findAllByClass[Parameter].map(_.name).toSet
    fragmentQuery(cancellationChecker, nameGenerator, existingParameterNames, query)
  }

  private def fragmentQuery(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    existingParameterNames: => Set[String],
    query: ast.Query
  ): CompositeQuery = {
    // This function gets called recursively to fragment the left-hand side of unions as well as sub-queries.
    // We probe the cancellation checker to interrupt the fragmentation process if the query has been cancelled.
    cancellationChecker.throwIfCancelled()

    query match {
      case singleQuery: ast.SingleQuery =>
        fragmentSingleQuery(cancellationChecker, nameGenerator, existingParameterNames, singleQuery)
      case ast.ProjectingUnionAll(lhs, rhs, mappings) =>
        CompositeQuery.Union(
          unionType = CompositeQuery.Union.Type.All,
          lhs = fragmentQuery(cancellationChecker, nameGenerator, existingParameterNames, lhs),
          rhs = fragmentSingleQuery(cancellationChecker, nameGenerator, existingParameterNames, rhs),
          unionMappings = mappings
        )
      case ast.ProjectingUnionDistinct(lhs, rhs, mappings) =>
        CompositeQuery.Union(
          unionType = CompositeQuery.Union.Type.Distinct,
          lhs = fragmentQuery(cancellationChecker, nameGenerator, existingParameterNames, lhs),
          rhs = fragmentSingleQuery(cancellationChecker, nameGenerator, existingParameterNames, rhs),
          unionMappings = mappings
        )
      case _: ast.UnmappedUnion =>
        throw new IllegalStateException(
          "Unmapped union should have been rewritten to projecting union by the namespacer."
        )
    }
  }

  private def fragmentSingleQuery(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    existingParameterNames: => Set[String],
    singleQuery: ast.SingleQuery
  ): CompositeQuery.Single =
    extractForeignClauses(singleQuery) match {
      case Some(foreignClauses) =>
        // If the clauses start with an import WITH clause, replace its import items with parameters.
        val rewrittenImportWith = foreignClauses.importWith.map { importWith =>
          parameteriseImportItems(nameGenerator, existingParameterNames, importWith)
        }

        CompositeQuery.Single.Foreign(
          graphReference = foreignClauses.useGraph.graphReference,
          clauses = List(
            rewrittenImportWith.map(_.withClause),
            foreignClauses.intermediateClauses,
            foreignClauses.returnClause
          ).flatten,
          parameters = rewrittenImportWith.map(_.parameters).getOrElse(Map.empty)
        )

      case None =>
        val arguments = extractArguments(singleQuery)
        val fragments = new FragmentsBuilder(arguments)
        singleQuery.clauses.foreach {
          case call @ ast.SubqueryCall(innerQuery, None) =>
            val subQuery = fragmentQuery(cancellationChecker, nameGenerator, existingParameterNames, innerQuery)
            fragments.addSubQuery(
              subQuery,
              innerQuery.isCorrelated,
              innerQuery.isReturning,
              call.inTransactionsParameters
            )
          case standardClause =>
            fragments.addStandardClause(standardClause)
        }
        fragments.result()
    }

  /**
   * The clauses making up a composite (sub-)query.
   *
   * @param useGraph the USE clause defining the graph on which the query is to be run
   * @param importWith the import WITH clause, optional
   * @param intermediateClauses all the clauses after the USE / import WITH clauses and before the RETURN clause
   * @param returnClause the RETURN clause, optional
   */
  private case class ForeignClauses(
    useGraph: ast.UseGraph,
    importWith: Option[ast.With],
    intermediateClauses: Seq[ast.Clause],
    returnClause: Option[ast.Return]
  )

  /**
   * Extracts a composite query fragment if the given single query qualifies.
   */
  private def extractForeignClauses(query: ast.SingleQuery): Option[ForeignClauses] = {
    // A composite query may start with an import WITH clause.
    val (headImportWith, clauses) = extractImportWith(query.clauses)
    clauses.headOption.collect {
      // The first clause after the optional import WITH must be the USE clause.
      case useGraph: ast.UseGraph =>
        val (importWith, otherClauses) = headImportWith match {
          // In a composite query, if there was no import WITH clause before the USE clause, it may be found directly after it.
          case None => extractImportWith(clauses.tail)
          // If there already was an import WITH clause at the start, then any subsequent WITH clause would be a standard clause.
          case Some(w) => (Some(w), clauses.tail)
        }
        val (intermediateClauses, returnClause) = otherClauses.initAndLastOption match {
          case Some((intermediate, r: ast.Return)) => (intermediate, Some(r))
          case _                                   => (otherClauses, None)
        }
        ForeignClauses(useGraph, importWith, intermediateClauses, returnClause)
    }
  }

  /**
   * Extracts the first clause if it is an importing WITH clause, returning the remaining clauses alongside it.
   */
  private def extractImportWith(clauses: Seq[ast.Clause]): (Option[ast.With], Seq[ast.Clause]) =
    clauses.headOption match {
      // A WITH clause is an import if it is the first clause, and if it only contains pass-through items.
      case Some(withClause @ ast.With(false, ri, None, None, None, None, _)) if ri.items.forall(_.isPassThrough) =>
        (Some(withClause), clauses.tail)
      case _ =>
        (None, clauses)
    }

  /**
   * @param parameters a map tying each new parameter to the alias of its original item
   * @param withClause the rewritten WITH clause containing the new parameters
   */
  private case class ParameterisedWithClause(
    parameters: Map[Parameter, LogicalVariable],
    withClause: ast.With
  )

  /**
   * Replaces items in the given import WITH clause with parameters.
   * The name of the parameter will be based on the item's alias, unless it clashes with an existing parameter, in which case it will get a new name.
   *
   * @param nameGenerator generator used to mint new parameter names in case of conflict with an existing parameter
   * @param existingParameterNames all the existing parameters in the query
   * @param importWith initial import WITH clause
   * @return the newly introduced parameters and the rewritten with clause
   */
  private def parameteriseImportItems(
    nameGenerator: AnonymousVariableNameGenerator,
    existingParameterNames: => Set[String],
    importWith: ast.With
  ): ParameterisedWithClause = {
    val (parameters, rewrittenItems) = importWith.returnItems.items.map {
      case _: ast.UnaliasedReturnItem =>
        throw new IllegalStateException("return items should have been aliased")

      case item @ ast.AliasedReturnItem(_, alias) =>
        val parameterName =
          // If there is an existing parameter with the same name as the alias,
          if (existingParameterNames.contains(alias.name))
            nameGenerator.nextName // then generate a parameter with a brand new name,
          else
            alias.name // otherwise name the parameter after the alias.
        val parameter = ExplicitParameter(parameterName, CTAny)(item.position)
        (parameter -> alias, ast.AliasedReturnItem(parameter, alias)(item.position))
    }.unzip
    // For convenience, return both a map of the new parameters and the rewritten WITH clause.
    ParameterisedWithClause(parameters.toMap, importWith.withReturnItems(rewrittenItems))
  }

  /**
   * Extracts the arguments of a single query i.e. the return items of its importing with, if any.
   */
  private def extractArguments(query: ast.SingleQuery): Set[LogicalVariable] =
    query
      .importWith
      .map(_.returnItems.items.map {
        case _: ast.UnaliasedReturnItem => throw new IllegalStateException("All return items should have been aliased.")
        case ast.AliasedReturnItem(_, variable) => variable
      }.toSet).getOrElse(Set.empty)
}
