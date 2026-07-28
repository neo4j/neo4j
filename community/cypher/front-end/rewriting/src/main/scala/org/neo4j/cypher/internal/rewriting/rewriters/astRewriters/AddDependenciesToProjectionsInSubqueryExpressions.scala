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
package org.neo4j.cypher.internal.rewriting.rewriters.astRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.ProjectingUnion
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.TopLevelBraces
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.SubqueryExpressionsHaveDependenciesInWithClauses
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ReturnItemsAreAliased
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo

/**
 * addDependenciesToProjectionsInSubqueryExpressions makes sure that any WITH clauses in the inner query
 * contain all imported scope dependencies from the outer scope, so they aren't overwritten.
 *
 * This makes sure that in a query such as:
 *
 *    WITH "Ada" AS name
 *    MATCH (n) WHERE n.name = name
 *    WHERE EXISTS {
 *       WITH "Lovelace" AS lastName
 *       MATCH (n)-[:HAS_FRIEND]-(m)
 *       WHERE m.name = name AND m.lastName = lastName
 *    }
 *    RETURN n
 *
 * The name from the outer scope is still available, WITH normally clears the scope, but for inner subqueries
 * we don't want that to happen, so this rewriter will add it to the WITH:
 *
 *  WITH "Lovelace" AS lastName, name as name
 */
case object AddDependenciesToProjectionsInSubqueryExpressions extends StepSequencer.Step
    with ASTRewriterFactory {

  private val subqueryExpressionMatcher: PartialFunction[AnyRef, AnyRef] = {
    case e: FullSubqueryExpression =>
      val scopeDependencies = e.scopeDependencies
      val newQuery = rewriteQuery(e.query, scopeDependencies, shouldSplitReturn = true)
      e.withQuery(newQuery)
  }

  val subqueryExpressionRewriter: Rewriter = bottomUp(Rewriter.lift(subqueryExpressionMatcher))

  private def rewriteQuery(query: Query, scopeDependencies: Set[LogicalVariable], shouldSplitReturn: Boolean): Query =
    query match {
      case sq: SingleQuery => rewritePartQuery(sq, scopeDependencies, shouldSplitReturn)
      case union @ UnionAll(lhs, rhs) =>
        union.copy(
          lhs = rewriteQuery(lhs, scopeDependencies, shouldSplitReturn),
          rhs = rewritePartQuery(rhs.singleQuery, scopeDependencies, shouldSplitReturn)
        )(union.position)
      case union @ UnionDistinct(lhs, rhs) =>
        union.copy(
          lhs = rewriteQuery(lhs, scopeDependencies, shouldSplitReturn),
          rhs = rewritePartQuery(rhs.singleQuery, scopeDependencies, shouldSplitReturn)
        )(union.position)
      case _: NextStatement =>
        throw new IllegalStateException("Didn't expect Next, only SingleQuery, UnionAll, or UnionDistinct.")
      case _: QueryWithLocalDefinitions =>
        throw new IllegalStateException(
          "Didn't expect QueryWithLocalDefinitions, only SingleQuery, UnionAll, or UnionDistinct."
        )
      case _: ConditionalQueryWhen =>
        throw new IllegalStateException("Didn't expect When, only SingleQuery, UnionAll, or UnionDistinct.")
      case _: TopLevelBraces =>
        throw new IllegalStateException("Didn't expect TopLevelBraces, only SingleQuery, UnionAll, or UnionDistinct.")
      case _: ProjectingUnion =>
        throw new IllegalStateException("Didn't expect ProjectingUnion, only SingleQuery, UnionAll, or UnionDistinct.")
    }

  def rewritePartQuery(
    query: PartQuery,
    scopeDependencies: Set[LogicalVariable],
    shouldSplitReturn: Boolean
  ): PartQuery = {
    val newClauses = query.clauses.flatMap {
      case w: With => Seq(addDependenciesToWithClause(w, scopeDependencies))
      case ret: Return if ret.orderBy.nonEmpty && shouldSplitReturn =>
        val (newWith, newReturn) = splitReturnClause(ret)
        Seq(addDependenciesToWithClause(newWith, scopeDependencies), newReturn)
      case x => Seq(x)
    }
    SingleQuery(clauses = newClauses)(query.position)
  }

  private def addDependenciesToWithClause(w: With, scopeDependencies: Set[LogicalVariable]): With = {
    val scopeDependenciesAsReturnItems = scopeDependencies.map { dependency =>
      AliasedReturnItem(dependency)
    }
    val newReturnItems = (w.returnItems.items ++ scopeDependenciesAsReturnItems).toSet
    w.copy(returnItems = w.returnItems.copy(items = newReturnItems.toSeq)(w.returnItems.position))(w.position)
  }

  /*
   * RETURN x + y ORDER BY x SKIP 10 LIMIT 5
   * =>
   * WITH x + y AS `x + y` ORDER BY x SKIP 10 LIMIT 5
   * RETURN `x + y`
   */
  private def splitReturnClause(r: Return): (With, Return) = {
    // TODO double check this
    val newWith = r.convertToWith(None)
    val newReturn =
      Return(r.returnItems.mapItems(items => items.map(ri => AliasedReturnItem(ri.alias.get))))(r.position)
    (newWith, newReturn)
  }

  override def preConditions: Set[Condition] = Set(
    ReturnItemsAreAliased,
    ExpressionsHaveComputedDependencies
  )

  override def postConditions: Set[Condition] = Set(SubqueryExpressionsHaveDependenciesInWithClauses)

  override def invalidatedConditions: Set[Condition] = SemanticInfoAvailable

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker,
    version: CypherVersion
  ): Rewriter = {
    if (version == CypherVersion.Cypher5) {
      subqueryExpressionRewriter
    } else {
      Rewriter.noop
    }
  }
}
