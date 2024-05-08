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
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.UnaliasedReturnItem
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsAggregate
import org.neo4j.cypher.internal.expressions.IterablePredicateExpression
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ReduceExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.conditions.aggregationsAreIsolated
import org.neo4j.cypher.internal.rewriting.conditions.hasAggregateButIsNotAggregate
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.topDown

/**
 * This rewriter makes sure that aggregations are on their own in RETURN/WITH clauses, so
 * the planner can have an easy time
 *
 * Example:
 *
 * MATCH (n)
 * RETURN { name: n.name, count: count(*) }, n.foo
 *
 * This query has a RETURN clause where the single expression contains both the aggregate key and
 * the aggregation expression. To make the job easier on the planner, this rewrite will change the query to:
 *
 * MATCH (n)
 * WITH n.name AS x1, count(*) AS x2, n.foo as X3
 * RETURN { name: x1, count: x2 }
 */
case object isolateAggregation extends StatementRewriter with StepSequencer.Step with PlanPipelineTransformerFactory {

  override def instance(from: BaseState, context: BaseContext): Rewriter =
    bottomUp(rewriter(from)(context.cancellationChecker), cancellation = context.cancellationChecker)

  private def rewriter(from: BaseState)(cancellationChecker: CancellationChecker): Rewriter = Rewriter.lift {
    case q @ SingleQuery(clauses) =>
      val newClauses = clauses.flatMap {
        case clause: ProjectionClause if clauseNeedingWork(clause)(cancellationChecker) =>
          val clauseReturnItems = clause.returnItems.items
          val (returnsItemsWithAggregations, others) =
            clauseReturnItems.partition(r => hasAggregateButIsNotAggregate(r.expression)(cancellationChecker))

          val withAggregations = returnsItemsWithAggregations.map(_.expression).toSet

          val withReturnItems: Set[ReturnItem] =
            extractExpressionsToInclude(withAggregations)(cancellationChecker).map {
              e =>
                AliasedReturnItem(e, Variable(from.anonymousVariableNameGenerator.nextName)(e.position))(e.position)
            } ++ others
          val pos = clause.position
          val withClause = With(
            distinct = false,
            ReturnItems(includeExisting = false, withReturnItems.toIndexedSeq)(pos),
            None,
            None,
            None,
            None
          )(pos)

          val expressionRewriter = createRewriterFor(withReturnItems)
          val newReturnItems = clauseReturnItems.map {
            case ri @ AliasedReturnItem(expression, _) =>
              ri.copy(expression = expression.endoRewrite(expressionRewriter))(ri.position)
            case ri @ UnaliasedReturnItem(expression, _) =>
              ri.copy(expression = expression.endoRewrite(expressionRewriter))(ri.position)
          }
          val resultClause = clause.withReturnItems(newReturnItems)

          IndexedSeq(withClause, resultClause)

        case clause => IndexedSeq(clause)
      }

      q.copy(clauses = newClauses)(q.position)
  }

  private def createRewriterFor(withReturnItems: Set[ReturnItem]): Rewriter = {
    val aliasedExpressionRefs: Map[Ref[Expression], LogicalVariable] =
      withReturnItems.map(ri => Ref(ri.expression) -> ri.alias.get).toMap
    lazy val aliasedExpressions: Map[Expression, LogicalVariable] =
      withReturnItems.map(ri => ri.expression -> ri.alias.get).toMap

    def inner = Rewriter.lift {
      case original: Expression =>
        aliasedExpressionRefs.get(Ref(original)).orElse {
          // Don't rewrite constant expressions, unless they were explicitly aliased.
          Option.when(isNotConstantExpression(original)) {
            aliasedExpressions.get(original)
          }.flatten
        }.map(_.copyId).getOrElse(original)
    }
    topDown(inner)
  }

  private def extractExpressionsToInclude(originalExpressions: Set[Expression])(
    cancellationChecker: CancellationChecker
  ): Set[Expression] = {
    val expressionsToGoToWith: Set[Expression] = fixedPoint(cancellationChecker) {
      (expressions: Set[Expression]) =>
        expressions.flatMap {
          case e @ ReduceExpression(scope, init, coll) if hasAggregateButIsNotAggregate(e)(cancellationChecker) =>
            Seq(init, coll) ++ scope.expression.dependencies.diff(Set(e.accumulator) ++ Set(e.variable))

          case e @ ListComprehension(scope, expr) if hasAggregateButIsNotAggregate(e)(cancellationChecker) =>
            scope.extractExpression match {
              case None => Seq(expr)
              case Some(extract) =>
                Seq(expr) ++ extract.dependencies.diff(Set(e.variable))
            }

          case e @ DesugaredMapProjection(entity, items, _) if hasAggregateButIsNotAggregate(e)(cancellationChecker) =>
            items.map(_.exp) :+ entity

          case e: IterablePredicateExpression if hasAggregateButIsNotAggregate(e)(cancellationChecker) =>
            val predicate: Expression =
              e.innerPredicate.getOrElse(throw new IllegalStateException("Should never be empty"))
            // Weird way of doing it to make scalac happy
            Set(e.expression) ++ predicate.dependencies - e.variable

          case e if hasAggregateButIsNotAggregate(e)(cancellationChecker) =>
            e.arguments

          case e =>
            Seq(e)
        }
    }(originalExpressions).filter {
      // Constant expressions should never be isolated
      isNotConstantExpression
    }
    expressionsToGoToWith
  }

  private def isNotConstantExpression(expr: Expression): Boolean =
    IsAggregate(expr) || expr.dependencies.nonEmpty

  private def clauseNeedingWork(c: Clause)(cancellationChecker: CancellationChecker): Boolean = c.folder.treeExists {
    case e: Expression => hasAggregateButIsNotAggregate(e)(cancellationChecker)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // Otherwise it might rewrite ambiguous symbols incorrectly, e.g. when a grouping variable is shadowed in for-comprehension.
    Namespacer.completed
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(StatementCondition(aggregationsAreIsolated))

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // Can introduces new ambiguous variable names itself.
    Namespacer.completed
  ) ++ SemanticInfoAvailable // Adds a WITH clause with no SemanticInfo

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[BaseContext, BaseState, BaseState] = this

}
