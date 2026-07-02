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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AddedInRewriteGeneral
import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.AscSortItem
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.DescSortItem
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.GroupingNone
import org.neo4j.cypher.internal.ast.OrderBy
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ProjectionClause.Elements
import org.neo4j.cypher.internal.ast.ProjectionClause.Subclauses
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.scoping.AggregatingItem
import org.neo4j.cypher.internal.ast.semantics.scoping.GroupingKey
import org.neo4j.cypher.internal.ast.semantics.scoping.NonAggregatingItem
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionSpecification
import org.neo4j.cypher.internal.ast.semantics.scoping.ScopeState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.StatementRewriter
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.FunctionInvocationsResolved
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.TreeAny
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewritable
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.RewriterStopper
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Condition
import org.neo4j.cypher.internal.util.topDown

/**
 * Expand and normalize subclauses. Assumes procedure resolution.
 */

case object SubclausesExpanded extends Condition

case object ExpandSubclauses extends StatementRewriter
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  private type Rewrites = Map[Ref[Clause], Seq[ProjectionClause]]

  override def instance(from: BaseState, context: BaseContext): Rewriter =
    context.cypherVersion match {
      case CypherVersion.Cypher5 => Rewriter.noop
      case _ => getRewriter(from.statement(), from.scopeState(), from.anonymousVariableNameGenerator)
    }

  override def preConditions: Set[Condition] = Set(
    UpToDateScopes,
    FunctionInvocationsResolved,
    AggregationsChecked
  )

  override def postConditions: Set[Condition] = Set(SubclausesExpanded)

  override def invalidatedConditions: Set[Condition] = Set(UpToDateScopes)

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] =
    ExpandSubclauses

  private def getRewrites(
    ast: Statement,
    scopeState: ScopeState,
    anonVarGen: AnonymousVariableNameGenerator
  ): Rewrites = {

    def subExpressionRewriter(spec: ProjectionSpecification): Rewriter =
      topDown(
        Rewriter.lift { case subExpr: Expression => spec.substituteSubExpression(subExpr, scopeState) },
        stopper = RewriterStopper.stopOn[ScopeExpression]
      )

    def shadowedByScope(e: Expression): Set[String] = e match {
      case _: FullSubqueryExpression =>
        scopeState.scopeOfOpt(e).map(_.collectAllDeclarations.iterator.map(_.value.name).toSet).getOrElse(Set.empty)
      case _: ScopeExpression =>
        scopeState.scopeOfOpt(e).map(_.declared.allSymbols.iterator.map(_.name).toSet).getOrElse(Set.empty)
      case _ => Set.empty
    }

    def groupingKeyRewriter(spec: ProjectionSpecification): Rewriter = {
      def rewrite(spec: ProjectionSpecification)(node: AnyRef): AnyRef = node match {
        case e: Expression =>
          spec.recognizeInNonAggregatingItem(e, isSubExpression = true).flatMap(_.alias) match {
            case Some(alias) => alias.withPosition(e.position)
            case None =>
              val innerSpec = spec.shadowGroupingKeys(shadowedByScope(e))
              Rewritable.dupAny(e, e.treeChildren.map(rewrite(innerSpec)).toSeq)
          }
        case other => Rewritable.dupAny(other, other.treeChildren.map(rewrite(spec)).toSeq)
      }
      Rewriter.fromFunction1(rewrite(spec))
    }

    def extractAndReplaceAggregatingExpressions(
      subclauses: Subclauses,
      spec: ProjectionSpecification
    ): (Seq[AliasedReturnItem], Option[OrderBy], Option[Where]) = {

      val substituteSubExpressions: Rewriter = subExpressionRewriter(spec)

      def extractSubclauseExpression(
        expr: Expression,
        useLegacySubstitution: Boolean
      ): (Option[Expression], Option[AliasedReturnItem]) = {
        spec.substituteFullExpression(expr, useLegacySubstitution, scopeState) match {
          case Some(itemAlias) => (Some(itemAlias), None)
          case None =>
            expr.endoRewrite(substituteSubExpressions) match {
              case rewritten if rewritten.containsAggregate =>
                val anonReference = Variable(anonVarGen.nextName, expr.position)
                (Some(anonReference), Some(AliasedReturnItem(expr, anonReference.copyId)(expr.position)))
              case rewritten if rewritten != expr => (Some(rewritten), None)
              case _                              => (None, None)
            }
        }
      }

      val Subclauses(groupBy, orderBy, _, _, where) = subclauses

      val hasGroupBy = groupBy.isDefined

      val sortItemsWithSubstitutions = orderBy.toSeq.flatMap(ob => {
        ob.sortItems.map {
          case si @ AscSortItem(expr) =>
            extractSubclauseExpression(expr, !hasGroupBy) match {
              case (Some(updatedExpr), itemOpt) =>
                (AscSortItem(updatedExpr)(expr.position), itemOpt)
              case (None, _) => (si, None)
            }
          case si @ DescSortItem(expr) =>
            extractSubclauseExpression(expr, !hasGroupBy) match {
              case (Some(updatedExpr), itemOpt) =>
                (DescSortItem(updatedExpr)(expr.position), itemOpt)
              case (None, _) => (si, None)
            }
        }
      })

      val updatedOrderBy = orderBy.flatMap(ob => {
        val sis = sortItemsWithSubstitutions.map(_._1)
        if (sis != ob.sortItems) Some(ob.copy(sis)(ob.position)) else None
      })

      val extractedPredicate = where.map(wh => {
        extractSubclauseExpression(wh.expression, !hasGroupBy) match {
          case (Some(updateExpr), itemOpt) => (Some(wh.copy(updateExpr)(wh.position)), itemOpt)
          case (None, _)                   => (None, None)
        }
      })

      val updatedWhere = extractedPredicate.flatMap(_._1)

      val extractedAggregatingItems = sortItemsWithSubstitutions.flatMap(_._2) ++ extractedPredicate.flatMap(_._2)

      (extractedAggregatingItems, updatedOrderBy, updatedWhere)

    }

    ast.folder.treeCollect[(ProjectionClause, Seq[ProjectionClause])] {
      case p @ ProjectionClause(distinct, items, _, _, _, _, _) if p.hasExpandableSubclause =>

        val scope = scopeState.recordedScopes(Ref(p))

        if (scope.children.nonEmpty) {
          val incoming = scope.children.head.incoming

          val (groupingKeys, nonAggregatingItems, aggregatingItems, hasInsetKeys) =
            incoming match {
              case ProjectionExpressionContext(_, _, _, spec, _) =>
                (
                  spec.groupingKeys.map(_.introduceAlias(anonVarGen)),
                  spec.nonAggregatingItems,
                  spec.aggregatingItems,
                  spec.hasInsetKeys
                )
              case _ =>
                val nonAggregatingItems: Set[NonAggregatingItem] =
                  items.items.map(ri => NonAggregatingItem(ri.expression, ri.alias.map(_.copyId))).toSet
                (Set.empty[GroupingKey], nonAggregatingItems, Set.empty[AggregatingItem], false)
            }

          val groupAndAggAliases = (groupingKeys.map(_.alias) ++ aggregatingItems.map(_.alias)).flatten

          val subclauses = Elements(p).subclauses
          val updatedSpec = ProjectionSpecification(
            groupingKeys,
            nonAggregatingItems,
            aggregatingItems,
            distinct,
            hasGroupBy = p.groupBy.isDefined
          )

          val (extractedAggregations, updatedOrderByOpt, updatedWhereOpt) =
            extractAndReplaceAggregatingExpressions(subclauses, updatedSpec)

          val hasSubclauseAggregations = extractedAggregations.nonEmpty

          val needsExplicitDistinct = (p.groupBy.isDefined || p.distinct) && aggregatingItems.isEmpty

          if (
            hasInsetKeys || hasSubclauseAggregations || (p.groupBy.exists(
              _.groupingElements.isInstanceOf[GroupingNone]
            ) && nonAggregatingItems.nonEmpty)
          ) {
            val pos = p.position
            val groupingAndAggregatingItems =
              groupingKeys.map(_.asReturnItem).toSeq ++
                aggregatingItems.map(_.asReturnItem).toSeq ++
                extractedAggregations
            val groupAndAggItems =
              ReturnItems(FreeProjection, groupingAndAggregatingItems)(pos)

            val groupingAndAggregatingClause =
              With(needsExplicitDistinct, groupAndAggItems, None, None, None, None, None, AddedInRewriteGeneral())(pos)

            val substituteGroupingKeys: Rewriter = groupingKeyRewriter(updatedSpec)
            val projectingItems = items.mapItems(_.map(ri => {
              val alias = ri.alias.get
              if (groupAndAggAliases contains alias) AliasedReturnItem(alias)
              else ri match {
                case ari: AliasedReturnItem =>
                  ari.copy(expression = ari.expression.endoRewrite(substituteGroupingKeys))(ari.position)
                case other => other
              }
            }))

            val projectingClause = p.copyProjection(
              distinct = false,
              returnItems = projectingItems,
              groupBy = None,
              orderBy = updatedOrderByOpt.orElse(p.orderBy),
              where = updatedWhereOpt.orElse(p.where)
            )

            p -> Seq(groupingAndAggregatingClause, projectingClause)
          } else if (updatedOrderByOpt.isDefined || updatedWhereOpt.isDefined || p.groupBy.isDefined) {
            p -> Seq(p.copyProjection(
              distinct = needsExplicitDistinct,
              groupBy = None,
              orderBy = updatedOrderByOpt.orElse(p.orderBy),
              where = updatedWhereOpt.orElse(p.where)
            ))
          } else p -> Seq()

        } else p -> Seq()

    }.filter(_._2.nonEmpty).map { case (clause, expanded) => Ref[Clause](clause) -> expanded }.toMap
  }

  def getRewriter(
    statement: Statement,
    scopeState: ScopeState,
    anonVarGen: AnonymousVariableNameGenerator
  ): Rewriter = {
    val rewrites = getRewrites(statement, scopeState, anonVarGen)
    if (rewrites.nonEmpty)
      topDown(Rewriter.lift {
        case singleQuery: SingleQuery =>
          singleQuery.copy(singleQuery.clauses.flatMap(c => rewrites.getOrElse(Ref(c), Seq(c))))(singleQuery.position)
      })
    else Rewriter.noop
  }
}
