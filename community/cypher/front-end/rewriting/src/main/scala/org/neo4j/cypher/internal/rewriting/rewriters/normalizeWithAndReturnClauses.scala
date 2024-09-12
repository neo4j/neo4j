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
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ProjectingUnion
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.UnionAll
import org.neo4j.cypher.internal.ast.UnionDistinct
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.factories.PreparatoryRewritingRewriterFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.topDown

case object ReturnItemsAreAliased extends Condition
case object ExpressionsInOrderByAndWhereUseAliases extends Condition

/**
 * This rewriter normalizes the scoping structure of a query, ensuring it is able to
 * be correctly processed for semantic checking. It makes sure that all return items
 * in WITH clauses are aliased.
 *
 * It also replaces expressions and subexpressions in ORDER BY and WHERE
 * to use aliases introduced by the WITH, where possible.
 *
 * Example:
 *
 * MATCH n
 * WITH n.prop AS prop ORDER BY n.prop DESC
 * RETURN prop
 *
 * This rewrite will change the query to:
 *
 * MATCH n
 * WITH n.prop AS prop ORDER BY prop DESC
 * RETURN prop AS prop
 */
case class normalizeWithAndReturnClauses(
  cypherExceptionFactory: CypherExceptionFactory
) extends Rewriter {

  def apply(that: AnyRef): AnyRef = that match {
    case q: Query => rewriteTopLevelQuery(q)

    case s @ ShowPrivileges(_, Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowPrivilegeCommands(_, _, Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowSupportedPrivilegeCommand(Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowDatabase(_, Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowAliases(_, Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowCurrentUser(Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowUsers(Some(Left((yields, returns))), _, _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowRoles(_, _, Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowServers(Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case x => x
  }

  /**
   * Rewrites all single queries in the top level query (which can be a single or a union query of single queries).
   * It does not rewrite query parts in subqueries.
   */
  private def rewriteTopLevelQuery(query: org.neo4j.cypher.internal.ast.Query): org.neo4j.cypher.internal.ast.Query =
    query match {
      case sq: SingleQuery => rewriteTopLevelSingleQuery(sq)
      case union @ UnionAll(lhs, rhs, _) =>
        union.copy(lhs = rewriteTopLevelQuery(lhs), rhs = rewriteTopLevelSingleQuery(rhs))(union.position)
      case union @ UnionDistinct(lhs, rhs, _) =>
        union.copy(lhs = rewriteTopLevelQuery(lhs), rhs = rewriteTopLevelSingleQuery(rhs))(union.position)
      case _: ProjectingUnion =>
        throw new IllegalStateException("Didn't expect ProjectingUnion, only SingleQuery, UnionAll, or UnionDistinct.")
    }

  /**
   * Adds aliases to all return items in Return clauses in the top level query.
   * Rewrites all projection clauses (also in subqueries) using [[rewriteProjectionsRecursively]].
   */
  private def rewriteTopLevelSingleQuery(singleQuery: SingleQuery): SingleQuery = {
    val newClauses = singleQuery.clauses.map {
      case r: Return => addAliasesToReturn(r)
      case x         => x
    }
    singleQuery.copy(clauses = newClauses)(singleQuery.position).endoRewrite(rewriteProjectionsRecursively)
  }

  private def addAliasesToReturn(r: Return): Return =
    r.copy(returnItems = aliasUnaliasedReturnItems(r.returnItems))(r.position)

  private def addAliasesToYield(y: Yield): Yield =
    y.copy(returnItems = aliasUnaliasedReturnItems(y.returnItems))(y.position)

  /**
   * Convert all UnaliasedReturnItems to AliasedReturnItems.
   */
  private def aliasUnaliasedReturnItems(ri: ReturnItems): ReturnItems = {
    val aliasedReturnItems =
      ri.items.map {
        case i: UnaliasedReturnItem =>
          val alias = i.alias.getOrElse(Variable(i.name)(i.expression.position))
          AliasedReturnItem(i.expression, alias)(i.position)
        case x => x
      }
    ri.copy(items = aliasedReturnItems)(ri.position)
  }

  /**
   * Convert those UnaliasedReturnItems to AliasedReturnItems which refer to a variable or a map projection.
   * Those can be deemed as implicitly aliased.
   */
  private def aliasImplicitlyAliasedReturnItems(ri: ReturnItems): ReturnItems = {
    val newItems =
      ri.items.map {
        case i: UnaliasedReturnItem if i.alias.isDefined =>
          AliasedReturnItem(i.expression, i.alias.get)(i.position)
        case x => x
      }
    ri.copy(items = newItems)(ri.position)
  }

  private val rewriteProjectionsRecursively: Rewriter = topDown(Rewriter.lift {
    // Only alias return items
    case clause @ ProjectionClause(_, ri: ReturnItems, None, _, _, None) =>
      clause.copyProjection(returnItems = aliasImplicitlyAliasedReturnItems(ri))

    case fullSubqueryExpression: FullSubqueryExpression =>
      fullSubqueryExpression.withQuery(rewriteTopLevelQuery(fullSubqueryExpression.query))

    // Alias return items and rewrite ORDER BY and WHERE
    case clause @ ProjectionClause(_, ri: ReturnItems, orderBy, _, _, where) =>
      clause.verifyOrderByAggregationUse((s, i) => throw cypherExceptionFactory.syntaxException(s, i))

      val existingAliases = ri.items.collect {
        case AliasedReturnItem(expression, variable) => expression -> variable
      }.toMap

      val updatedOrderBy = orderBy.map(aliasOrderBy(existingAliases, _))
      val updatedWhere = where.map(aliasWhere(existingAliases, _))

      clause.copyProjection(
        returnItems = aliasImplicitlyAliasedReturnItems(ri),
        orderBy = updatedOrderBy,
        where = updatedWhere
      )
  })

  /**
   * Given a list of existing aliases, this rewrites an OrderBy to use these where possible.
   */
  private def aliasOrderBy(existingAliases: Map[Expression, LogicalVariable], originalOrderBy: OrderBy): OrderBy = {
    val updatedSortItems = originalOrderBy.sortItems.map { aliasSortItem(existingAliases, _) }
    OrderBy(updatedSortItems)(originalOrderBy.position)
  }

  /**
   * Given a list of existing aliases, this rewrites a SortItem to use these where possible.
   */
  private def aliasSortItem(existingAliases: Map[Expression, LogicalVariable], sortItem: SortItem): SortItem = {
    sortItem match {
      case AscSortItem(expression) =>
        AscSortItem(aliasExpression(existingAliases, expression))(sortItem.position)
      case DescSortItem(expression) =>
        DescSortItem(aliasExpression(existingAliases, expression))(sortItem.position)
    }
  }

  /**
   * Given a list of existing aliases, this rewrites a where to use these where possible.
   */
  private def aliasWhere(existingAliases: Map[Expression, LogicalVariable], originalWhere: Where): Where = {
    Where(aliasExpression(existingAliases, originalWhere.expression))(originalWhere.position)
  }

  /**
   * Given a list of existing aliases, this rewrites expressions to use these where possible.
   */
  private def aliasExpression(existingAliases: Map[Expression, LogicalVariable], expression: Expression): Expression = {
    existingAliases.get(expression) match {
      case Some(alias) if !existingAliases.valuesIterator.contains(expression) =>
        alias.copyId.withPosition(expression.position)
      case _ =>
        val newExpression = expression.endoRewrite(topDown(
          Rewriter.lift {
            case subExpression: Expression =>
              existingAliases.get(subExpression) match {
                case Some(subAlias) if !potentiallyRedefined(subExpression, existingAliases) =>
                  subAlias.copyId.withPosition(subExpression.position)
                case _ => subExpression
              }
          },
          _.isInstanceOf[ScopeExpression]
        ))
        newExpression
    }
  }

  /**
   * Check that the alias is also not referenced a second time in the existing aliases which
   * would cause a potential error in re-referencing it.
   * e.g. WITH 2 AS a, WITH 4 as a, a AS b WHERE 4 = a ...
   * The `4 = a` should evaluate to true, but without this check, it would be rewritten to:
   * WITH 2 AS a, WITH 4 as a, a AS b WHERE 4 = b ... which is now false.
   * If the alias is not actually a redefinition, ignore it: e.g. n AS n is not a redefinition.
  */
  private def potentiallyRedefined(
    expression: Expression,
    existingAliases: Map[Expression, LogicalVariable]
  ): Boolean = {
    existingAliases.valuesIterator.contains(expression) ||
    existingAliases.filter {
      case (expression: Variable, variable: Variable) => expression.name != variable.name
      case _                                          => true
    }.valuesIterator.exists(alias =>
      expression.folder.findAllByClass[LogicalVariable].contains(alias)
    )
  }
}

case object normalizeWithAndReturnClauses extends Step with PreparatoryRewritingRewriterFactory {

  override def getRewriter(cypherExceptionFactory: CypherExceptionFactory): Rewriter = {
    normalizeWithAndReturnClauses(cypherExceptionFactory)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set(
    ReturnItemsAreAliased,
    ExpressionsInOrderByAndWhereUseAliases
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable
}
