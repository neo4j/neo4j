/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.compiler.planner.logical.CandidateGenerator
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.ir.InterestingOrder
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.Exists
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.PatternExpressionPatternElementNamer
import org.neo4j.cypher.internal.v4_0.util.FreshIdNameGenerator
import org.neo4j.cypher.internal.v4_0.util.UnNamedNameGenerator

case object selectPatternPredicates extends CandidateGenerator[LogicalPlan] {

  def apply(lhs: LogicalPlan, queryGraph: QueryGraph, interestingOrder: InterestingOrder, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    for (
      pattern <- queryGraph.selections.patternPredicatesGiven(lhs.availableSymbols)
      if applicable(lhs, queryGraph, pattern, context.planningAttributes.solveds))
      yield {
        pattern match {
          case e:ExistsSubClause =>
            val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
            context.logicalPlanProducer.planSemiApply(lhs, innerPlan, e, context)
          case p@Not(e: ExistsSubClause) =>
            val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
            context.logicalPlanProducer.planAntiSemiApply(lhs, innerPlan, p, context)
          case p@Exists(patternExpression: PatternExpression) =>
            val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
            context.logicalPlanProducer.planSemiApply(lhs, rhs, p, context)
          case p@Not(Exists(patternExpression: PatternExpression)) =>
            val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
            context.logicalPlanProducer.planAntiSemiApply(lhs, rhs, p, context)
          case Ors(exprs) =>
            val (patternExpressions, expressions) = exprs.partition {
              case ExistsSubClause(_, _) => true
              case Not(ExistsSubClause(_, _)) => true
              case Exists(_: PatternExpression) => true
              case Not(Exists(_: PatternExpression)) => true
              case _ => false
            }
            val (plan, solvedPredicates) = planPredicates(lhs, patternExpressions, expressions, None, interestingOrder, context)
            context.logicalPlanProducer.solvePredicate(plan, onePredicate(solvedPredicates), context)
        }
      }
  }

  def planInnerOfSubquery(lhs: LogicalPlan, context: LogicalPlanningContext, interestingOrder: InterestingOrder, e: ExistsSubClause): LogicalPlan = {
    // Creating a query graph by combining all extracted query graphs created by each entry of the patternElements
    val emptyTuple = (Map.empty[PatternElement, Variable], QueryGraph.empty)
    val(namedMap, qg) = e.patternElements.foldLeft(emptyTuple) { (acc, patternElement) =>
      patternElement match {
      case elem: RelationshipChain =>
        val patternExpr = PatternExpression(RelationshipsPattern(elem)(elem.position))
        val (namedExpr, namedMap) = PatternExpressionPatternElementNamer.apply(patternExpr)
        val qg = extractQG(lhs, namedExpr, context)

        (acc._1 ++ namedMap, acc._2 ++ qg)

      case elem: NodePattern =>
        val patternExpr = NodePatternExpression(List(elem))(elem.position)
        val (namedExpr, namedMap) = PatternExpressionPatternElementNamer.apply(patternExpr)
        val qg = extractQG(lhs, namedExpr, context)

        (acc._1 ++ namedMap, acc._2 ++ qg)
      }
    }

    // Adding the predicates to new query graph
    val new_qg = e.optionalWhereExpression.foldLeft(qg) {
      case (acc, p) => acc.addPredicates(e.outerScope.map(id => id.name), p)
    }

    val innerContext = createPlannerContext(context, namedMap)
    innerContext.strategy.plan(new_qg, interestingOrder, innerContext)
  }

  private def extractQG(source: LogicalPlan, namedExpr: NodePatternExpression, context: LogicalPlanningContext): QueryGraph = {
    import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._

    val qgArguments = getQueryGraphArguments(source, namedExpr)
    asQueryGraph(namedExpr, context.innerVariableNamer).withArgumentIds(qgArguments)
  }

  private def extractQG(source: LogicalPlan, namedExpr: PatternExpression, context: LogicalPlanningContext): QueryGraph = {
    import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._

    val qgArguments = getQueryGraphArguments(source, namedExpr)
    asQueryGraph(namedExpr, context.innerVariableNamer).withArgumentIds(qgArguments)
  }

  private def getQueryGraphArguments(source: LogicalPlan, namedExpr: Expression) = {
    val dependencies = namedExpr.
      dependencies.
      map(_.name).
      filter(id => UnNamedNameGenerator.isNamed(id))

    source.availableSymbols intersect dependencies
  }

  private def createPlannerContext(context: LogicalPlanningContext, namedMap: Map[PatternElement, Variable]): LogicalPlanningContext = {
    val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
    val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
    context.forExpressionPlanning(namedNodes, namedRels)
  }

  private def planPredicates(lhs: LogicalPlan,
                             patternExpressions: Set[Expression],
                             expressions: Set[Expression],
                             letExpression: Option[Expression],
                             interestingOrder: InterestingOrder,
                             context: LogicalPlanningContext): (LogicalPlan, Set[Expression]) = {

    def planSelect(predicate: Expression, source: LogicalPlan,
                   func: (LogicalPlan, LogicalPlan, Expression, InterestingOrder, LogicalPlanningContext) => LogicalPlan): (LogicalPlan, Set[Expression]) = {
      val plan = func(lhs, source, onePredicate(expressions ++ letExpression.toSet), interestingOrder, context)
      (plan, expressions + predicate)
    }

    def planSemiApply(predicate: Expression, innerExpression: Expression, tail: List[Expression], source: LogicalPlan): (LogicalPlan, Set[Expression]) = {
       val (newLhs, newLetExpr) = predicate match {
        case Not(_) => createLetAntiSemiApply(lhs, source, innerExpression, predicate, expressions, letExpression, interestingOrder, context)
        case _ => createLetSemiApply(lhs, source, innerExpression, expressions, letExpression, interestingOrder, context)
      }
      val (plan, solvedPredicates) = planPredicates(newLhs, tail.toSet, Set.empty, Some(newLetExpr), interestingOrder, context)
      (plan, solvedPredicates ++ Set(predicate) ++ expressions)
    }

    patternExpressions.toList match {
      case (p@Exists(patternExpression: PatternExpression)) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        planSelect(p, rhs, context.logicalPlanProducer.planSelectOrSemiApply)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: Nil =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        planSelect(p, rhs, context.logicalPlanProducer.planSelectOrAntiSemiApply)

      case (e@ExistsSubClause(_, _)) :: Nil =>
        val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
        planSelect(e, innerPlan, context.logicalPlanProducer.planSelectOrSemiApply)

       case (p@Not(e@ExistsSubClause(_, _))) :: Nil =>
        val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
        planSelect(p, innerPlan, context.logicalPlanProducer.planSelectOrAntiSemiApply)

      case (p@Exists(patternExpression: PatternExpression)) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        planSemiApply(p, patternExpression, tail, rhs)

      case (p@Not(Exists(patternExpression: PatternExpression))) :: tail =>
        val rhs = rhsPlan(lhs, patternExpression, interestingOrder, context)
        planSemiApply(p, patternExpression, tail, rhs)

      case (e@ExistsSubClause(_, _)) :: tail =>
       val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
       planSemiApply(e, e, tail, innerPlan)

      case (p@Not(e@ExistsSubClause(_, _))) :: tail =>
        val innerPlan = planInnerOfSubquery(lhs, context, interestingOrder, e)
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
                                 interestingOrder: InterestingOrder,
                                 context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(existsExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), interestingOrder, context), ident)
  }

  private def createLetAntiSemiApply(lhs: LogicalPlan,
                                     rhs: LogicalPlan,
                                     existsExpression: Expression,
                                     predicate: Expression,
                                     expressions: Set[Expression],
                                     letExpression: Option[Expression],
                                     interestingOrder: InterestingOrder,
                                     context: LogicalPlanningContext) = {
    val (idName, ident) = freshId(existsExpression)
    if (expressions.isEmpty && letExpression.isEmpty)
      (context.logicalPlanProducer.planLetAntiSemiApply(lhs, rhs, idName, context), ident)
    else
      (context.logicalPlanProducer.planLetSelectOrAntiSemiApply(lhs, rhs, idName, onePredicate(expressions ++ letExpression.toSet), interestingOrder, context), ident)
  }

  private def rhsPlan(lhs: LogicalPlan, pattern: PatternExpression, interestingOrder: InterestingOrder, ctx: LogicalPlanningContext) = {
    val context = ctx.withUpdatedCardinalityInformation(lhs)
    val (plan, _) = context.strategy.planPatternExpression(lhs.availableSymbols, pattern, context)
    plan
  }

  private def onePredicate(expressions: Set[Expression]): Expression = if (expressions.size == 1)
    expressions.head
  else
    Ors(expressions)(expressions.head.position)

  private def applicable(outerPlan: LogicalPlan, qg: QueryGraph, expression: Expression, solveds: Solveds) = {
    val symbolsAvailable = qg.argumentIds.subsetOf(outerPlan.availableSymbols)
    val isSolved = solveds.get(outerPlan.id).asSinglePlannerQuery.exists(_.queryGraph.selections.contains(expression))
    symbolsAvailable && !isSolved
  }


  private def freshId(existsExpression: Expression) = {
    val name = FreshIdNameGenerator.name(existsExpression.position)
    (name, Variable(name)(existsExpression.position))
  }
}
