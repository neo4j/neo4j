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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.ProjectingUnion
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.SubqueryExpressionsHaveDependenciesInWithClauses
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
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
case object addDependenciesToProjectionsInSubqueryExpressions extends StepSequencer.Step
    with ASTRewriterFactory {

  val instance: Rewriter = bottomUp(Rewriter.lift {
    case s: ScopeClauseSubqueryCall =>
      val newQuery = rewriteQuery(s.innerQuery, s.importedVariables.toSet, shouldSplitReturn = false)
      s.copy(innerQuery = newQuery)(s.position)
    case e: FullSubqueryExpression =>
      val scopeDependencies = e.scopeDependencies
      val newQuery = rewriteQuery(e.query, scopeDependencies, shouldSplitReturn = true)
      e.withQuery(newQuery)
  })

  private def rewriteQuery(query: Query, scopeDependencies: Set[LogicalVariable], shouldSplitReturn: Boolean): Query =
    query match {
      case sq: SingleQuery => rewriteSingleQuery(sq, scopeDependencies, shouldSplitReturn)
      case union @ UnionAll(lhs, rhs, _) =>
        union.copy(
          lhs = rewriteQuery(lhs, scopeDependencies, shouldSplitReturn),
          rhs = rewriteSingleQuery(rhs, scopeDependencies, shouldSplitReturn)
        )(union.position)
      case union @ UnionDistinct(lhs, rhs, _) =>
        union.copy(
          lhs = rewriteQuery(lhs, scopeDependencies, shouldSplitReturn),
          rhs = rewriteSingleQuery(rhs, scopeDependencies, shouldSplitReturn)
        )(union.position)
      case _: ProjectingUnion =>
        throw new IllegalStateException("Didn't expect ProjectingUnion, only SingleQuery, UnionAll, or UnionDistinct.")
    }

  def rewriteSingleQuery(
    query: SingleQuery,
    scopeDependencies: Set[LogicalVariable],
    shouldSplitReturn: Boolean
  ): SingleQuery = {
    val newClauses = query.clauses.flatMap {
      case w: With => Seq(addDependenciesToWithClause(w, scopeDependencies))
      case ret: Return if ret.orderBy.nonEmpty && shouldSplitReturn =>
        val (newWith, newReturn) = splitReturnClause(ret)
        Seq(addDependenciesToWithClause(newWith, scopeDependencies), newReturn)
      case x => Seq(x)
    }
    query.copy(clauses = newClauses)(query.position)
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
    val newWith = With(r.distinct, r.returnItems, r.orderBy, r.skip, r.limit, r.where)(r.position)
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
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = instance

}
