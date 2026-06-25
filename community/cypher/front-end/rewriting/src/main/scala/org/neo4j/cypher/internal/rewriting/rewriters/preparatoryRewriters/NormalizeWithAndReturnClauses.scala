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
package org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.ExpressionBody
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.LocalFunctionDefinition
import org.neo4j.cypher.internal.ast.LocalProcedureDefinition
import org.neo4j.cypher.internal.ast.NextStatement
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.PartQuery
import org.neo4j.cypher.internal.ast.ProjectingUnion
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Query
import org.neo4j.cypher.internal.ast.QueryBody
import org.neo4j.cypher.internal.ast.QueryWithLocalDefinitions
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowAliases
import org.neo4j.cypher.internal.ast.ShowAuthRules
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowServers
import org.neo4j.cypher.internal.ast.ShowSupportedPrivilegeCommand
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SortItem
import org.neo4j.cypher.internal.ast.TopLevelBraces
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
case class NormalizeWithAndReturnClauses(
  cypherExceptionFactory: CypherExceptionFactory,
  versionOpt: Option[CypherVersion]
) extends Rewriter {

  private val isCypher5: Boolean = versionOpt.contains(CypherVersion.Cypher5)

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

    case s @ ShowAliases(_, Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowCurrentUser(Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowUsers(Some(Left((yields, returns))), _, _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowRoles(_, _, _, _, Some(Left((yields, returns))), _) =>
      s.copy(yieldOrWhere = Some(Left((addAliasesToYield(yields), returns.map(addAliasesToReturn)))))(s.position)
        .withGraph(s.useGraph)

    case s @ ShowAuthRules(Some(Left((yields, returns))), _, _) =>
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
      case pq: PartQuery => rewriteTopLevelPartQuery(pq)
      case union @ UnionAll(lhs, rhs) =>
        union.copy(lhs = rewriteTopLevelQuery(lhs), rhs = rewriteTopLevelPartQuery(rhs))(
          union.position
        )
      case union @ UnionDistinct(lhs, rhs) =>
        union.copy(lhs = rewriteTopLevelQuery(lhs), rhs = rewriteTopLevelPartQuery(rhs))(
          union.position
        )
      case when @ ConditionalQueryWhen(branches, default) =>
        when.copy(
          branches = branches.map {
            case b @ ConditionalQueryBranch(predicate, query) =>
              b.copy(
                predicate = predicate.endoRewrite(rewriteProjectionsRecursively),
                query.endoRewrite(rewriteProjectionsRecursively)
              )(b.position)
          },
          default = default.endoRewrite(rewriteProjectionsRecursively)
        )(when.position)
      case next @ NextStatement(queries) =>
        next.copy(
          queries.dropRight(1).endoRewrite(rewriteProjectionsRecursively) :+ rewriteTopLevelQuery(queries.last)
        )(next.position)
      case qwld @ QueryWithLocalDefinitions(definitions, query) =>
        // note that the inputSignature does not need to be rewritten here, because it is required to adhere to ASTSlicingPhrase.checkExpressionIsStatic
        qwld.copy(
          definitions = definitions.map {
            case lpd @ LocalProcedureDefinition(_, _, _, body) => lpd.copy(
                body = rewriteTopLevelQuery(body)
              )(lpd.position)
            case lfd @ LocalFunctionDefinition(_, _, _, qb @ QueryBody(query)) => lfd.copy(
                body = qb.copy(query = rewriteTopLevelQuery(query))(qb.position)
              )(lfd.position)
            case lfd @ LocalFunctionDefinition(_, _, _, eb @ ExpressionBody(expression)) => lfd.copy(
                body = eb.copy(expression = expression.endoRewrite(rewriteProjectionsRecursively))(eb.position)
              )(lfd.position)
          },
          query = rewriteTopLevelQuery(query)
        )(qwld.position)
      case _: ProjectingUnion =>
        throw new IllegalStateException("Didn't expect ProjectingUnion, only SingleQuery, UnionAll, or UnionDistinct.")
    }

  /**
   * Adds aliases to all return items in Return clauses in the top level query.
   * Rewrites all projection clauses (also in subqueries) using [[rewriteProjectionsRecursively]].
   */
  private def rewriteTopLevelPartQuery(partQuery: PartQuery): PartQuery = {
    partQuery match {
      case sq: SingleQuery =>
        val newClauses = sq.clauses.map {
          case r: Return => addAliasesToReturn(r)
          case x         => x
        }
        sq.copy(clauses = newClauses)(sq.position).endoRewrite(rewriteProjectionsRecursively)
      case tlb: TopLevelBraces =>
        tlb.endoRewrite(rewriteProjectionsRecursively)
    }
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
          val alias = i.alias.getOrElse(Variable(i.name)(i.expression.position, Variable.isIsolatedDefault))
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
    case clause @ ProjectionClause(_, ri: ReturnItems, _, None, _, _, None) =>
      clause.copyProjection(returnItems = aliasImplicitlyAliasedReturnItems(ri))

    case fullSubqueryExpression: FullSubqueryExpression =>
      fullSubqueryExpression.withQuery(rewriteTopLevelQuery(fullSubqueryExpression.query))

    // Alias return items and rewrite ORDER BY and WHERE
    case clause @ ProjectionClause(_, ri: ReturnItems, _, orderBy, _, _, where) =>
      if (isCypher5) {
        clause.verifyOrderByAggregationUse((s, i) =>
          throw cypherExceptionFactory.invalidUseOfAggregationInOrderBy(s, i)
        )

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
      } else {
        clause.copyProjection(returnItems = aliasImplicitlyAliasedReturnItems(clause.returnItems))
      }
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

case object NormalizeWithAndReturnClauses extends Step with PreparatoryRewritingRewriterFactory {

  override def getRewriter(
    cypherExceptionFactory: CypherExceptionFactory,
    versionOpt: Option[CypherVersion]
  ): Rewriter = {
    NormalizeWithAndReturnClauses(cypherExceptionFactory, versionOpt)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set()

  override def postConditions: Set[StepSequencer.Condition] = Set(
    ReturnItemsAreAliased,
    ExpressionsInOrderByAndWhereUseAliases
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable
}
