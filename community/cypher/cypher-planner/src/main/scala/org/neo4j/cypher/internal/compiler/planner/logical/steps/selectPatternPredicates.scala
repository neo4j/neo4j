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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NodePatternExpression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections.containsPatternPredicates
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.asQueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator

case object selectPatternPredicates extends SelectionCandidateGenerator {

  override def apply(lhs: LogicalPlan,
                     unsolvedPredicates: Set[Expression],
                     queryGraph: QueryGraph,
                     interestingOrderConfig: InterestingOrderConfig,
                     context: LogicalPlanningContext): Iterator[SelectionCandidate] = {
    for {
      pattern <- unsolvedPredicates.iterator.filter(containsPatternPredicates)
      if queryGraph.argumentIds.subsetOf(lhs.availableSymbols)
    } yield {
        val plan = pattern match {
          case e:ExistsSubClause =>
            val innerPlan = planInnerOfSubquery(lhs, context, interestingOrderConfig, e)
            context.logicalPlanProducer.planSemiApply(lhs, innerPlan, e, context)
          case p@Not(e: ExistsSubClause) =>
            val innerPlan = planInnerOfSubquery(lhs, context, interestingOrderConfig, e)
            context.logicalPlanProducer.planAntiSemiApply(lhs, innerPlan, p, context)
          case p@Exists(patternExpression: PatternExpression) =>
            val rhs = rhsPlan(lhs, patternExpression, context)
            context.logicalPlanProducer.planSemiApply(lhs, rhs, p, context)
          case p@Not(Exists(patternExpression: PatternExpression)) =>
            val rhs = rhsPlan(lhs, patternExpression, context)
            context.logicalPlanProducer.planAntiSemiApply(lhs, rhs, p, context)
          case o@Ors(exprs) =>
            val (patternExpressions, expressions) = exprs.partition {
              case ExistsSubClause(_, _) => true
              case Not(ExistsSubClause(_, _)) => true
              case Exists(_: PatternExpression) => true
              case Not(Exists(_: PatternExpression)) => true
              case _ => false
            }
            val (plan, solvedPredicates) = planPredicates(lhs, patternExpressions.toSet, expressions.toSet, None, interestingOrderConfig, context)
            AssertMacros.checkOnlyWhenAssertionsAreEnabled(
              exprs.forall(solvedPredicates.contains),
              "planPredicates is supposed to solve all predicates in an OR clause."
            )
            context.logicalPlanProducer.solvePredicate(plan, o)
        }
        SelectionCandidate(plan, Set(pattern))
      }
  }

  def planInnerOfSubquery(lhs: LogicalPlan,
                          context: LogicalPlanningContext,
                          interestingOrderConfig: InterestingOrderConfig,
                          e: ExistsSubClause): LogicalPlan = {
    // Creating a query graph by combining all extracted query graphs created by each entry of the patternElements
    val qg = e.patternElements.foldLeft(QueryGraph.empty) { (acc, patternElement) =>
      patternElement match {
        case elem: RelationshipChain =>
          val variableToCollectName = context.anonymousVariableNameGenerator.nextName
          val collectionName = context.anonymousVariableNameGenerator.nextName
          val patternExpr = PatternExpression(RelationshipsPattern(elem)(elem.position))(e.outerScope, variableToCollectName, collectionName)
          val qg = asQueryGraph(patternExpr, lhs.availableSymbols, context.anonymousVariableNameGenerator)
          acc ++ qg

        case elem: NodePattern =>
          val patternExpr = NodePatternExpression(List(elem))(elem.position)
          val qg = asQueryGraph(patternExpr, lhs.availableSymbols, context.anonymousVariableNameGenerator)
          acc ++ qg
      }
    }

    // Adding the predicates and known outer variables to new query graph
    val new_qg = e.optionalWhereExpression.foldLeft(qg) {
      case (acc: QueryGraph, patternExpr: Expression) => {
        val outerVariableNames = e.outerScope.map(id => id.name)
        val usedVariables: Seq[String] = patternExpr.arguments
          .findByAllClass[Variable]
          .map(_.name)
          .distinct

        acc.addPredicates(outerVariableNames, patternExpr)
          .addArgumentIds(usedVariables.filter(v => outerVariableNames.contains(v)))
      }
    }

    context.strategy.plan(new_qg, interestingOrderConfig, context).result
  }

  def planPredicates(lhs: LogicalPlan,
                     patternExpressions: Set[Expression],
                     expressions: Set[Expression],
                     letExpression: Option[Expression],
                     interestingOrderConfig: InterestingOrderConfig,
                     context: LogicalPlanningContext): (LogicalPlan, Set[Expression]) = {

    def planSelect(predicate: Expression, source: LogicalPlan,
                   func: (LogicalPlan, LogicalPlan, Expression, LogicalPlanningContext) => LogicalPlan): (LogicalPlan, Set[Expression]) = {
      val plan = func(lhs, source, onePredicate(expressions ++ letExpression.toSet), context)
      (plan, expressions + predicate)
    }

    def planSemiApply(predicate: Expression, innerExpression: Expression, tail: List[Expression], source: LogicalPlan): (LogicalPlan, Set[Expression]) = {
       val (newLhs, newLetExpr) = predicate match {
        case Not(_) => createLetAntiSemiApply(lhs, source, innerExpression, expressions, letExpression, context)
        case _ => createLetSemiApply(lhs, source, innerExpression, expressions, letExpression, context)
      }
      val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), interestingOrderConfig, context)
      (plan, solvedPredicates ++ Set(predicate) ++ expressions)
    }

    patternExpressions.toList match {
      case (p@Exists(patternExpression: PatternExpression)) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, context)
        planSelect(p, rhs, context.logicalPlanProducer.planSelectOrSemiApply)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, context)
        planSelect(p, rhs, context.logicalPlanProducer.planSelectOrAntiSemiApply)

      case (e@ExistsSubClause(_, _)) :: Nil =>
        val innerPlan = planInnerOfSubquery(lhs, context, interestingOrderConfig, e)
        planSelect(e, innerPlan, context.logicalPlanProducer.planSelectOrSemiApply)

       case (p@Not(e@ExistsSubClause(_, _))) :: Nil =>
        val innerPlan = planInnerOfSubquery(lhs, context, interestingOrderConfig, e)
        planSelect(p, innerPlan, context.logicalPlanProducer.planSelectOrAntiSemiApply)

      case (p@Exists(patternExpression: PatternExpression)) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, context)
        planSemiApply(p, patternExpression, tail, rhs)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, context)
        planSemiApply(p, patternExpression, tail, rhs)

      case (e@ExistsSubClause(_, _)) :: tail =>
       val innerPlan = planInnerOfSubquery(lhs, context, interestingOrderConfig, e)
       planSemiApply(e, e, tail, innerPlan)

      case (p@Not(e@ExistsSubClause(_, _))) :: tail =>
        val innerPlan = planInnerOfSubquery(lhs, context, interestingOrderConfig, e)
        planSemiApply(p, e, tail, innerPlan)

      case _ =>
        throw new IllegalArgumentException("There should be at least one pattern expression")
    }
  }

  private def createLetSemiApply(lhs: LogicalPlan,
                                 rhs: LogicalPlan,
                                 existsExpression: Expression,
                                 expressions: Set[Expression],
                                 letExpression: Option[Expression],
                                 context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(existsExpression, context.anonymousVariableNameGenerator)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), context), ident)
  }

  private def createLetAntiSemiApply(lhs: LogicalPlan,
                                     rhs: LogicalPlan,
                                     existsExpression: Expression,
                                     expressions: Set[Expression],
                                     letExpression: Option[Expression],
                                     context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(existsExpression, context.anonymousVariableNameGenerator)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetAntiSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrAntiSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), context), ident)
  }

  private def rhsPlan(lhs: LogicalPlan, pattern: PatternExpression, ctx: LogicalPlanningContext) = {
    val context = ctx.withUpdatedLabelInfo(lhs)
    context.strategy.planPatternExpression(lhs.availableSymbols, pattern, context)
  }

  private def onePredicate(expressions: Set[Expression]): Expression = if (expressions.size == 1)
    expressions.head
  else
    Ors(expressions.toSeq)(expressions.head.position)

  private def freshId(existsExpression: Expression, anonymousVariableNameGenerator: AnonymousVariableNameGenerator) = {
    val name = anonymousVariableNameGenerator.nextName
    (name, Variable(name)(existsExpression.position))
  }
}
