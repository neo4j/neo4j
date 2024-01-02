/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.RunQueryAt
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.rewriters.normalizeWithAndReturnClauses
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.topDown

/**
 * Transforms composite queries, as identified by their USE clause, into [[RunQueryAt]] clauses.
 */
case class FragmentCompositeQueries(semanticFeatures: Set[SemanticFeature]) extends StatementRewriter {

  override def postConditions: Set[StepSequencer.Condition] = FragmentCompositeQueries.postConditions

  override def instance(from: BaseState, context: BaseContext): Rewriter =
    // If the "USE multiple graph selector" semantic feature is present,
    if (semanticFeatures.contains(SemanticFeature.UseAsMultipleGraphsSelector))
      // then we are dealing with a composite database, in which case we need to rewrite the relevant fragments into RunQueryAt clauses,
      rewriteFragments(context.cancellationChecker, from.anonymousVariableNameGenerator, from.statement())
    else
      // otherwise we can leave the query as is.
      Rewriter.noop

  private def rewriteFragments(
    cancellationChecker: CancellationChecker,
    nameGenerator: AnonymousVariableNameGenerator,
    statement: Statement
  ): Rewriter = {
    // We must ensure that the parameters we generate do not clash with existing ones.
    // Lazy computed once to avoid duplicated effort inside the rewriter.
    lazy val existingParameterNames = statement.folder(cancellationChecker).findAllByClass[Parameter].map(_.name).toSet
    topDown(
      rewriter = Rewriter.lift {
        // Composite queries only appear as single queries.
        // Note that union queries containing composite single queries are allowed.
        case singleQuery: SingleQuery =>
          // Check whether the query matches the shape of a composite query.
          extractQueryFragment(singleQuery) match {
            // If it does, rewrite it around a RunQueryAt clause.
            case Some(queryFragment) =>
              // If the fragment starts with an import WITH clause, replace its import items with parameters.
              val rewrittenImportWith = queryFragment.importWith.map { importWith =>
                parameteriseImportItems(nameGenerator, existingParameterNames, importWith)
              }

              // This is the query that will be serialised and sent to the relevant graph.
              val innerQuery = SingleQuery(List(
                rewrittenImportWith.map(_.withClause),
                queryFragment.intermediateClauses,
                queryFragment.returnClause
              ).flatten)(queryFragment.useGraph.position)

              // Package the query with the graph where it should run and what its parameters refer to in the outer query.
              val runQueryAt = RunQueryAt(
                graphReference = queryFragment.useGraph.graphReference,
                innerQuery = innerQuery,
                parameters = rewrittenImportWith.map(_.parameters).getOrElse(Map.empty)
              )(queryFragment.useGraph.position)

              // Finally, surface the values returned by the inner query in the outer query.
              val returnClause = queryFragment.returnClause.map { fragmentReturnClause =>
                fragmentReturnClause.copy(
                  distinct = false,
                  returnItems = fragmentReturnClause.returnItems.mapItems(_.map {
                    case UnaliasedReturnItem(_, _) =>
                      throw new IllegalStateException("return items should have been aliased")
                    case AliasedReturnItem(_, alias) =>
                      AliasedReturnItem(alias) // The underlying expression will have been interpreted by the inner query, we simply need to surface the aliases
                  }),
                  orderBy = None,
                  skip = None,
                  limit = None
                )(position = singleQuery.position)
              }

              SingleQuery(List(
                queryFragment.importWith, // The original import WITH clause remains as is in the outer query, the imported values are then passed to the inner query via its parameters.
                Some(runQueryAt),
                returnClause
              ).flatten)(singleQuery.position)

            // If it doesnt match the shape of a composite query, leave it as is.
            case None => singleQuery
          }
      },
      cancellation = cancellationChecker
    )
  }

  /**
   * The clauses making up a composite (sub-)query.
   *
   * @param useGraph the USE clause defining the graph on which the query is to be run
   * @param importWith the import WITH clause, optional
   * @param intermediateClauses all the clauses after the USE / import WITH clauses and before the RETURN clause
   * @param returnClause the RETURN clause, optional
   */
  private case class QueryFragment(
    useGraph: UseGraph,
    importWith: Option[With],
    intermediateClauses: Seq[Clause],
    returnClause: Option[Return]
  )

  /**
   * Extract a composite query fragment if the given single query qualifies.
   */
  private def extractQueryFragment(query: SingleQuery): Option[QueryFragment] = {
    // A composite query may start with an import WITH clause.
    val (headImportWith, clauses) = extractImportWith(query.clauses)
    clauses.headOption.collect {
      // The first clause after the optional import WITH must be the USE clause.
      case useGraph: UseGraph =>
        val (importWith, otherClauses) = headImportWith match {
          // In a composite query, if there was no import WITH clause before the USE clause, it may be found directly after it.
          case None => extractImportWith(clauses.tail)
          // If there already was an import WITH clause at the start, then any subsequent WITH clause would be a standard clause.
          case Some(w) => (Some(w), clauses.tail)
        }
        val (intermediateClauses, returnClause) = otherClauses.lastOption match {
          case Some(r: Return) => (otherClauses.init, Some(r))
          case _               => (otherClauses, None)
        }
        QueryFragment(useGraph, importWith, intermediateClauses, returnClause)
    }
  }

  /**
   * Extract the first clause if it is an import WITH clause, return the remaining clauses.
   */
  private def extractImportWith(clauses: Seq[Clause]): (Option[With], Seq[Clause]) =
    clauses.headOption match {
      // A WITH clause is an import if it is the first clause, and if it only contains pass-through items.
      case Some(withClause @ With(false, ri, None, None, None, None, _)) if ri.items.forall(_.isPassThrough) =>
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
    withClause: With
  )

  /**
   * Replace items in the given import WITH clause with parameters.
   * The name of the parameter will be based on the item's alias, unless it clashes with an existing parameter, in which case it will get a new name.
   *
   * @param nameGenerator generator used to mint new parameter names in case of conflict with an existing parameter
   * @param existingParameterNames all the existing parameters in the query
   * @param importWith initial import WITH clause
   * @return the newly introduced parameters and the rewritten with clause
   */
  private def parameteriseImportItems(
    nameGenerator: AnonymousVariableNameGenerator,
    existingParameterNames: Set[String],
    importWith: With
  ): ParameterisedWithClause = {
    val (parameters, rewrittenItems) = importWith.returnItems.items.map {
      case _: UnaliasedReturnItem =>
        throw new IllegalStateException("return items should have been aliased")

      case item @ AliasedReturnItem(_, alias) =>
        val parameterName =
          // If there is an existing parameter with the same name as the alias,
          if (existingParameterNames.contains(alias.name))
            nameGenerator.nextName // then generate a parameter with a brand new name,
          else
            alias.name // otherwise name the parameter after the alias.
        val parameter = ExplicitParameter(parameterName, CTAny)(item.position)
        (parameter -> alias, AliasedReturnItem(parameter, alias)(item.position))
    }.unzip
    // For convenience, return both a map of the new parameters and the rewritten WITH clause.
    ParameterisedWithClause(parameters.toMap, importWith.withReturnItems(rewrittenItems))
  }
}

object FragmentCompositeQueries
    extends StepSequencer.Step
    with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  override def preConditions: Set[StepSequencer.Condition] =
    normalizeWithAndReturnClauses.postConditions + // all return items are aliased
      Namespacer.completed // and variable names can be trusted

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[BaseContext, BaseState, BaseState] = FragmentCompositeQueries(semanticFeatures.toSet)
}
