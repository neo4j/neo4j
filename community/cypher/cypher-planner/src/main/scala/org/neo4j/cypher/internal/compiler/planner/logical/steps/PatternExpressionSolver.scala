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

import org.neo4j.cypher.internal.compiler.planner.logical.{LogicalPlanningContext, patternExpressionRewriter}
import org.neo4j.cypher.internal.ir.InterestingOrder
import org.neo4j.cypher.internal.ir.{HasMappableExpressions, QueryGraph}
import org.neo4j.cypher.internal.logical.plans.{Argument, LogicalPlan}
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.expressions.functions.{Coalesce, Exists, Head}
import org.neo4j.cypher.internal.v4_0.rewriting.rewriters.{PatternExpressionPatternElementNamer, projectNamedPaths}
import org.neo4j.cypher.internal.v4_0.util.{FreshIdNameGenerator, Rewriter, UnNamedNameGenerator, topDown}

import scala.collection.mutable
import scala.reflect.ClassTag

/*
Prepares expressions containing pattern expressions by solving them in a sub-query through RollUpApply and replacing
the original expression with an identifier, or preferably GetDegree when possible.

A query such as:
MATCH (n) RETURN (n)-->()

Would be solved with a plan such as

+Rollup (creates the collection with all the produced paths from RHS)
| \
| +(RHS) Projection (of path)
| |
| +Expand( (n)-->() )
| |
| +Argument
|
+(LHS) AllNodesScan(n)
*/
object PatternExpressionSolver {
  private val pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression

  /**
    * Get a Solver to solve multiple expressions and finally return a rewritten plan of the given source.
    *
    * The usage pattern is like this:
    *
    * {{{
    * val solver = PatternExpressionSolver.solverFor(source, context)
    * val rewrittenExpression = solver.solve(someExpressionForANewPlan)
    * val rewrittenSource = solver.rewrittenPlan()
    * // Proceed to plan a new operator using rewrittenExpression instead of someExpressionForANewPlan, and rewrittenSource instead of source
    * }}}
    *
    * @param source the LogicalPlan that a new operator will be put on top of.
    */
  def solverFor(source: LogicalPlan,
                context: LogicalPlanningContext): SolverForInnerPlan = new SolverForInnerPlan(source, context)

  /**
    * Get a Solver to solve multiple expressions and finally rewrite a planned leaf plan.
    *
    * The usage pattern is like this:
    *
    * {{{
    * val solver = PatternExpressionSolver.solverForLeafPlan(argumentIds, context)
    * val rewrittenExpression = solver.solve(someExpressionForANewPlan)
    * val newArguments = solver.newArguments
    * val plan = // plan leaf plan using `argumentIds ++ newArguments`
    * val rewrittenPlan = solver.rewriteLeafPlan(plan)
    * }}}
    *
    * @param argumentIds the argument IDs of the leaf plan that is about to be planned
    */
  def solverForLeafPlan(argumentIds: Set[String],
                        context: LogicalPlanningContext): SolverForLeafPlan = new SolverForLeafPlan(argumentIds, context)

  abstract class Solver(initialPlan: LogicalPlan,
                                context: LogicalPlanningContext) {
    private val patternExpressionSolver = solvePatternExpressions(initialPlan.availableSymbols, context)
    private val patternComprehensionSolver = solvePatternComprehensions(initialPlan.availableSymbols, context)
    protected var resultPlan: LogicalPlan = initialPlan
    protected var arguments: mutable.Builder[String, Set[String]] = Set.newBuilder[String]

    def solve(expression: Expression, maybeKey: Option[String] = None): Expression = {
      if (resultPlan == null) {
        throw new IllegalArgumentException("You cannot solve more expressions after obtaining the rewritten plan.")
      }

      val RewriteResult(plan, solvedExp, introducedVariables) = expression match {
        case expression: PatternExpression =>
          val (newPlan, newVar) = patternExpressionSolver.solveUsingRollUpApply(resultPlan, expression, maybeKey, context)
          RewriteResult(newPlan, newVar, Set(newVar.name))

        case expression: PatternComprehension =>
          val (newPlan, newVar) = patternComprehensionSolver.solveUsingRollUpApply(resultPlan, expression, maybeKey, context)
          RewriteResult(newPlan, newVar, Set(newVar.name))

        case inExpression =>
          val expression = solveUsingGetDegree(inExpression)
          val RewriteResult(firstStepPlan, firstStepExpression, firstStepIntroducedVariables) = patternComprehensionSolver.rewriteInnerExpressions(resultPlan, expression, context)
          val RewriteResult(secondStepPlan, secondStepExpression, secondStepintroducedVariables) = patternExpressionSolver.rewriteInnerExpressions(firstStepPlan, firstStepExpression, context)
          RewriteResult(secondStepPlan, secondStepExpression, firstStepIntroducedVariables ++ secondStepintroducedVariables)
      }
      resultPlan = plan
      arguments ++= introducedVariables
      solvedExp
    }

  }

  class SolverForInnerPlan(source: LogicalPlan,  context: LogicalPlanningContext)
    extends Solver(source, context) {

    def rewrittenPlan(): LogicalPlan = {
      val result = this.resultPlan
      this.resultPlan = null
      result
    }
  }

  class SolverForLeafPlan(argumentIds: Set[String],  context: LogicalPlanningContext)
      extends Solver(
        context.logicalPlanProducer.ForPatternExpressionSolver.planArgument(argumentIds, context), // When we have a leaf plan, we start with a single row on the LHS of the RollupApply
        context){

    def newArguments: Set[String] = {
      arguments.result()
    }

    def rewriteLeafPlan(leafPlan: LogicalPlan): LogicalPlan = {
      val lhsOfApply = this.resultPlan
      this.resultPlan = null
      lhsOfApply match {
        case _: Argument =>
          // We did not change anything. No need to wrap the leaf plan in an apply.
          leafPlan
        case _ =>
          context.logicalPlanProducer.ForPatternExpressionSolver.planApply(lhsOfApply, leafPlan, context)
      }
    }
  }

  private def solveUsingGetDegree(exp: Expression): Expression = exp.endoRewrite(getDegreeRewriter)

  private def solvePatternExpressions(availableSymbols: Set[String],
                                      context: LogicalPlanningContext): ListSubQueryExpressionSolver[PatternExpression] = {

    def extractQG(source: LogicalPlan, namedExpr: PatternExpression): QueryGraph = {
      import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._

      val dependencies = namedExpr.
        dependencies.
        map(_.name).
        filter(id => UnNamedNameGenerator.isNamed(id))

      val qgArguments = source.availableSymbols intersect dependencies
      asQueryGraph(namedExpr, context.innerVariableNamer).withArgumentIds(qgArguments)
    }

    def createPlannerContext(context: LogicalPlanningContext, namedMap: Map[PatternElement, Variable]): LogicalPlanningContext = {
      val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
      val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
      context.forExpressionPlanning(namedNodes, namedRels)
    }

    def createPathExpression(pattern: PatternExpression): PathExpression = {
      val pos = pattern.position
      val path = EveryPath(pattern.pattern.element)
      val step: PathStep = pathStepBuilder(path)
      PathExpression(step)(pos)
    }

    ListSubQueryExpressionSolver[PatternExpression](
      namer = PatternExpressionPatternElementNamer.apply,
      extractQG = extractQG,
      createPlannerContext = createPlannerContext,
      projectionCreator = createPathExpression,
      patternExpressionRewriter = patternExpressionRewriter(availableSymbols, context))
  }

  private def solvePatternComprehensions(availableSymbols: Set[String],
                                         context: LogicalPlanningContext): ListSubQueryExpressionSolver[PatternComprehension] = {
    def extractQG(source: LogicalPlan, namedExpr: PatternComprehension) = {
      import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters._

      val queryGraph = asQueryGraph(namedExpr, context.innerVariableNamer)
      val args = queryGraph.idsWithoutOptionalMatchesOrUpdates intersect availableSymbols
      queryGraph.withArgumentIds(args)
    }

    def createProjectionToCollect(pattern: PatternComprehension): Expression = pattern.projection

    def createPlannerContext(context: LogicalPlanningContext, namedMap: Map[PatternElement, Variable]): LogicalPlanningContext = {
      val namedNodes = namedMap.collect { case (elem: NodePattern, identifier) => identifier }
      val namedRels = namedMap.collect { case (elem: RelationshipChain, identifier) => identifier }
      context.forExpressionPlanning(namedNodes, namedRels)
    }

    ListSubQueryExpressionSolver[PatternComprehension](
      namer = PatternExpressionPatternElementNamer.apply,
      extractQG = extractQG,
      createPlannerContext = createPlannerContext,
      projectionCreator = createProjectionToCollect,
      patternExpressionRewriter = patternExpressionRewriter(availableSymbols, context))
  }

  private case class RewriteResult(currentPlan: LogicalPlan, currentExpression: Expression, introducedVariables: Set[String])

  private case class ListSubQueryExpressionSolver[T <: Expression](namer: T => (T, Map[PatternElement, Variable]),
                                                           extractQG: (LogicalPlan, T) => QueryGraph,
                                                           createPlannerContext: (LogicalPlanningContext, Map[PatternElement, Variable]) => LogicalPlanningContext,
                                                           projectionCreator: T => Expression,
                                                           patternExpressionRewriter: Rewriter,
                                                           pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression)
                                                          (implicit m: ClassTag[T]) {

    case class PlannedSubQuery(columnName: String, innerPlan: LogicalPlan, nullableIdentifiers: Set[String]) {
      def variableToCollect: String = columnName
    }

    def solveUsingRollUpApply(source: LogicalPlan, expr: T, maybeKey: Option[String], context: LogicalPlanningContext): (LogicalPlan, Variable) = {

      val key = maybeKey.getOrElse(FreshIdNameGenerator.name(expr.position.bumped()))
      val subQueryPlan = planSubQuery(source, expr, context)
      val producedPlan = context.logicalPlanProducer.ForPatternExpressionSolver.planRollup(source,
        subQueryPlan.innerPlan,
        key,
        subQueryPlan.variableToCollect,
        subQueryPlan.nullableIdentifiers,
        context)

      (producedPlan, Variable(key)(expr.position))
    }

    def rewriteInnerExpressions(plan: LogicalPlan, expression: Expression, context: LogicalPlanningContext): RewriteResult = {
      val patternExpressions: Seq[T] = expression.findByAllClass[T]

      patternExpressions.foldLeft(RewriteResult(plan, expression, Set.empty)) {
        case (RewriteResult(currentPlan, currentExpression, introducedVariables), patternExpression) =>
          val (newPlan, newVar) = solveUsingRollUpApply(currentPlan, patternExpression, None, context)

          val rewriter = rewriteButStopIfRollUpApplyForbidden(patternExpression, newVar)
          val rewrittenExpression = currentExpression.endoRewrite(rewriter)

          if (rewrittenExpression == currentExpression) {
            RewriteResult(currentPlan, currentExpression.endoRewrite(patternExpressionRewriter), introducedVariables)
          } else {
            RewriteResult(newPlan, rewrittenExpression, introducedVariables + newVar.name)
          }
      }
    }

    private def planSubQuery(source: LogicalPlan, expr: T, context: LogicalPlanningContext) = {
      val (namedExpr, namedMap) = namer(expr)

      val qg = extractQG(source, namedExpr)
      val innerContext = createPlannerContext(context, namedMap)

      //We don't retain the interesting order because
      //  i) There is no way of expressing order within a pattern comprehension
      //  ii) It can lead to endless recursion in case the sort expression contains the subquery we are solving
      //      e.g. `n{.name, Thing_Has_Thing2:[ (n)-[:Has]->(thing2:Thing2)| n.name ]} ORDER BY n.name`
      val innerPlan = innerContext.strategy.plan(qg, InterestingOrder.empty, innerContext)
      val collectionName = FreshIdNameGenerator.name(expr.position)
      val projectedPath = projectionCreator(namedExpr)
      val projectedInner = projection(innerPlan, Map(collectionName -> projectedPath), Map(collectionName -> projectedPath),
                                      InterestingOrder.empty, innerContext)
      PlannedSubQuery(columnName = collectionName, innerPlan = projectedInner, nullableIdentifiers = qg.argumentIds)
    }

    /*
    * It's important to not go use RollUpApply if the expression we are working with is:
    *
    * a) inside a loop. If that is not honored, it will produce the wrong results by not having the correct scope.
    * b) inside a conditional expression. Otherwise it can be executed even when not strictly needed.
    * c) inside an expression that accessed only part of the list. Otherwise we do too much work. To avoid that we inject a Limit into the
    *    NestedPlanExpression.
    *
    */
    private def rewriteButStopIfRollUpApplyForbidden(oldExp: Expression, newExp: Expression): Rewriter = {
      val inner = Rewriter.lift {
        case exp if exp == oldExp =>
          newExp
      }
      topDown(inner, stopper = {
        case _: PatternComprehension => false
        // Loops
        case _: ScopeExpression => true
        // Conditionals & List accesses
        case _: CaseExpression => true
        case _: ContainerIndex => true
        case _: ListSlice => true
        case f: FunctionInvocation => f.function == Exists || f.function == Coalesce || f.function == Head
        case _ => false
      })
    }
  }

  case class ForMappable[T]() {
    def solve(inner: LogicalPlan, mappable: HasMappableExpressions[T],  context: LogicalPlanningContext): (T, LogicalPlan) = {
      val solver = PatternExpressionSolver.solverFor(inner, context)
      val rewrittenExpression = mappable.mapExpressions(solver.solve(_))
      val rewrittenInner = solver.rewrittenPlan()
      (rewrittenExpression, rewrittenInner)
    }
  }

  object ForMulti {
    def solve(inner: LogicalPlan, expressions: Seq[Expression],  context: LogicalPlanningContext): (Seq[Expression], LogicalPlan) = {
      val solver = PatternExpressionSolver.solverFor(inner, context)
      val rewrittenExpressions: Seq[Expression] = expressions.map(solver.solve(_))
      val rewrittenInner = solver.rewrittenPlan()
      (rewrittenExpressions, rewrittenInner)
    }
  }

  object ForSingle  {
    def solve(inner: LogicalPlan, expression: Expression,  context: LogicalPlanningContext): (Expression, LogicalPlan) = {
      val solver = PatternExpressionSolver.solverFor(inner, context)
      val rewrittenExpression = solver.solve(expression)
      val rewrittenInner = solver.rewrittenPlan()
      (rewrittenExpression, rewrittenInner)
    }
  }

  object ForExistentialSubquery {
    def solve(source: LogicalPlan, expressions: Seq[Expression], interestingOrder: InterestingOrder, context: LogicalPlanningContext): (Seq[Expression], LogicalPlan) = {
      expressions.foldLeft((Seq.empty[Expression], source)) {
          case ((solvedExprs, plan), e: ExistsSubClause) =>
            val subQueryPlan = selectPatternPredicates.planInnerOfSubquery(plan, context, interestingOrder, e)
            val semiApplyPlan = context.logicalPlanProducer.planSemiApplyInHorizon(plan, subQueryPlan, e, context)
            (solvedExprs :+ e, semiApplyPlan)
          case (acc, _) => acc
      }
    }
  }
}
