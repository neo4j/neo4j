/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.CostModelMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanRestrictions
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerConfiguration
import org.neo4j.cypher.internal.compiler.planner.logical.QueryPlannerKit
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.SortPlanner.SatisfiedForPlan
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.BestPlans
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Cardinality.NumericCardinality
import org.neo4j.cypher.internal.util.CartesianOrdering
import org.neo4j.cypher.internal.util.Cost
import org.neo4j.exceptions.InternalException

import scala.annotation.tailrec

/**
 * Responsible for connecting disconnected components that have already been planned in isolation,
 * and also responsible for solving optional matches.
 */
trait JoinDisconnectedQueryGraphComponents {

  def connectComponentsAndSolveOptionalMatch(
    plans: Set[PlannedComponent],
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    singleComponentPlanner: SingleComponentPlannerTrait
  ): BestPlans
}

case class PlannedComponent(queryGraph: QueryGraph, plan: BestPlans)

case class Component(queryGraph: QueryGraph, plan: LogicalPlan)

/**
 * This class is responsible for connecting two disconnected logical plans, which can be
 * done with hash joins when an useful predicate connects the two plans, or with cartesian
 * product lacking that.
 *
 * The input is a set of disconnected patterns and this class will greedily find the
 * cheapest connection that can be done replace the two input plans with the connected
 * one. This process can then be repeated until a single plan remains.
 *
 * This class is being replaced by [[ComponentConnectorPlanner]].
 * It is still left in the code in case the replacement leads to unexpected regressions.
 * The plan is to remove this in the future, e.g. in the next major version.
 *
 * Compared with [[ComponentConnectorPlanner]], this does not always consider ordering
 * during planning, and might thus produce worse plans if there is an ORDER BY.
 */
case object cartesianProductsOrValueJoins extends JoinDisconnectedQueryGraphComponents {

  val COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT = 8

  override def connectComponentsAndSolveOptionalMatch(
    plans: Set[PlannedComponent],
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    singleComponentPlanner: SingleComponentPlannerTrait
  ): BestPlans = {

    @tailrec
    def recurse(
      plans: Set[PlannedComponent],
      optionalMatches: Seq[QueryGraph]
    ): (Set[PlannedComponent], Seq[QueryGraph]) = {
      if (optionalMatches.nonEmpty) {
        // If we have optional matches left to solve - start with that
        val firstOptionalMatch = optionalMatches.head
        val applicablePlan =
          plans.find(p => firstOptionalMatch.argumentIds subsetOf p.plan.bestResult.availableSymbols)

        applicablePlan match {
          case Some(t @ PlannedComponent(solvedQg, p)) =>
            val candidates = context.plannerState.config.optionalSolvers
              .flatMap(getSolver =>
                getSolver.solver(firstOptionalMatch, qg, interestingOrderConfig, context).connect(p.bestResult)
              )
            val best = kit.pickBest(candidates, s"best plan solving optional match: $firstOptionalMatch").get
            recurse(plans - t + PlannedComponent(solvedQg, BestResults(best, None)), optionalMatches.tail)

          case None =>
            // If we couldn't find any optional match we can take on, produce the best cartesian product possible
            recurse(
              connectComponentsStep(plans, qg, interestingOrderConfig, context, kit, singleComponentPlanner),
              optionalMatches
            )
        }
      } else if (plans.size > 1) {

        recurse(
          connectComponentsStep(plans, qg, interestingOrderConfig, context, kit, singleComponentPlanner),
          optionalMatches
        )
      } else (plans, optionalMatches)
    }

    val (resultingPlans, optionalMatches) = recurse(plans, qg.optionalMatches)
    require(resultingPlans.size == 1)
    require(optionalMatches.isEmpty)
    resultingPlans.head.plan
  }

  /**
   * Connects components step-wise, i.e. it will connect at least 2 components.
   * Greedily chooses the cheapest option to connect at each step.
   *
   * @param plans the so-far connected components
   * @param qg the whole query graph
   * @return a smaller set of connected components, by connecting 2 or more together.
   */
  private def connectComponentsStep(
    plans: Set[PlannedComponent],
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    singleComponentPlanner: SingleComponentPlannerTrait
  ): Set[PlannedComponent] = {
    require(plans.size > 1, "Can't connect less than 2 components.")

    /*
    To connect disconnected query parts, we have a couple of different ways. First we check if there are any joins that
    we could do. Joins are equal or better than cartesian products, so we always go for the joins when possible.

    Next we perform an exhaustive search for how to combine the remaining query parts together. In-between each step we
    check if any joins have been made available and if any predicates can be applied. This exhaustive search makes for
    better plans, but is exponentially expensive.

    So, when we have too many plans to combine, we fall back to the naive way of just building a left deep tree with
    all query parts cross joined together.
     */
    val joins =
      produceHashJoins(plans, qg, context, kit) ++
        produceNIJVariations(plans, qg, interestingOrderConfig, context, kit, singleComponentPlanner)

    val (joinsSatisfyingOrder, joinsOther) = joins.partition { case (comp, _) =>
      require(comp.plan.bestResultFulfillingReq.isEmpty, s"Expected only bestResult for component $comp")
      val plan = comp.plan.bestResult
      val asSortedAsPossible = SatisfiedForPlan(plan)
      val providedOrder = context.staticComponents.planningAttributes.providedOrders(plan.id)
      interestingOrderConfig.orderToSolve.satisfiedBy(providedOrder) match {
        case asSortedAsPossible() => true
        case _                    => false
      }
    }

    if (joinsSatisfyingOrder.nonEmpty) {
      pickTheBest(plans, kit, joinsSatisfyingOrder)
    } else if (joinsOther.nonEmpty) {
      pickTheBest(plans, kit, joinsOther)
    } else if (plans.size < COMPONENT_THRESHOLD_FOR_CARTESIAN_PRODUCT) {
      val cartesianProducts = produceCartesianProducts(plans, qg, context, kit)
      pickTheBest(plans, kit, cartesianProducts)
    } else {
      Set(planLotsOfCartesianProducts(plans, qg, interestingOrderConfig, context, kit, considerSelections = true))
    }
  }

  private def pickTheBest(
    plans: Set[PlannedComponent],
    kit: QueryPlannerKit,
    joins: Map[PlannedComponent, (PlannedComponent, PlannedComponent)]
  ): Set[PlannedComponent] = {
    val bestPlan = kit.pickBest.ofBestResults(
      joins.map(_._1.plan),
      s"best join plan",
      plan => {
        val solvedQg = joins.keys.collectFirst {
          case PlannedComponent(queryGraph, BestResults(bestResult, bestResultFulfillingReq))
            if bestResult == plan || bestResultFulfillingReq.contains(plan) => queryGraph
        }.get
        s"Solved: $solvedQg"
      }
    ).get
    val bestQG: QueryGraph = joins.collectFirst {
      case (PlannedComponent(fqg, pl), _) if bestPlan == pl => fqg
    }.get
    val (p1, p2) = joins(PlannedComponent(bestQG, bestPlan))

    plans - p1 - p2 + PlannedComponent(bestQG, bestPlan)
  }

  private def theSortedComponent(components: Set[PlannedComponent], kit: QueryPlannerKit): Option[PlannedComponent] = {
    val allSorted = components.collect {
      case pc @ PlannedComponent(_, BestResults(_, Some(_))) => pc
    }

    // we might get multiple sorted components when there is an order by literal for example
    kit.pickBest[PlannedComponent](_.plan.result, allSorted, "best sorted component")
  }

  /**
   * Plans a large amount of query parts together. Produces a left deep tree sorted by the cost/cardinality of the query parts.
   *
   * @param considerSelections whether to try and plan selections after each combining of two components.
   */
  private[idp] def planLotsOfCartesianProducts(
    plans: Set[PlannedComponent],
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    considerSelections: Boolean
  ): PlannedComponent = {
    val maybeSortedComponent = theSortedComponent(plans, kit)

    def sortCriteria(c: Component): (Cost, Cardinality) = {
      val cardinality = context.staticComponents.planningAttributes.cardinalities(c.plan.id)
      val cost = context.cost.costFor(
        c.plan,
        context.plannerState.input,
        context.semanticTable,
        context.staticComponents.planningAttributes.cardinalities,
        context.staticComponents.planningAttributes.providedOrders,
        context.plannerState.accessedAndAggregatingProperties,
        context.statistics,
        CostModelMonitor.DEFAULT
      )
      (cost, cardinality)
    }

    val components = plans.toList.map {
      case PlannedComponent(queryGraph, BestResults(bestResult, _)) => Component(queryGraph, bestResult)
    }

    val bestComponents: Seq[Component] =
      if (components.size < 2) {
        components
      } else {
        val maxCardinality = components
          .map(c => context.staticComponents.planningAttributes.cardinalities(c.plan.id))
          .filter(_ >= Cardinality.SINGLE)
          .product(NumericCardinality)
        val ordering: CartesianOrdering = context.settings.executionModel.cartesianOrdering(maxCardinality)
        components.map { c => (c, sortCriteria(c)) }.sortBy(_._2)(ordering).map(_._1)
      }

    val componentsWithSortedPlanFirst = maybeSortedComponent.map {
      // If we have a sorted component, that should go to the very left of the cartesian products to keep the sort order
      sortedComponent =>
        val c = Component(sortedComponent.queryGraph, sortedComponent.plan.bestResultFulfillingReq.get)
        c +: bestComponents.filterNot(comp => c.queryGraph == comp.queryGraph)
    }

    /**
     * This build a right-deep tree of Cartesian Products.
     *
     * In Volcano, the shape of the tree does not affect cost, as shown below.
     * C0 x (C1 x C2)              = (C0 x C1) x C2
     * c0 + s0 * (c1 + s1 * c2)    = c0 + s0 * c1 + s0 * s1 * c2
     * c0 + s0 * c1 + s0 * s1 * c2 = c0 + s0 * c1 + s0 * s1 * c2
     *
     * In Batched, we believe that a right-deep tree is cheaper or equally costly compared to other tree shapes.
     * To show this in the general case, we would need to prove the following:
     * C0 x (C1 x C2)                                 <=? (C0 x C1) x C2
     * c0 + ⌈s0 / B⌉ * (c1 + ⌈s1 / B⌉ * c2)           <=? c0 + ⌈s0 / B⌉ * c1 + ⌈s0 * s1 / B⌉ * c2
     * c0 + ⌈s0 / B⌉ * c1 + ⌈s0 / B⌉ * ⌈s1 / B⌉ * c2  <=? c0 + ⌈s0 / B⌉ * c1 + ⌈s0 * s1 / B⌉ * c2          | -c0 - ⌈s0 / B⌉ * c1
     *                      ⌈s0 / B⌉ * ⌈s1 / B⌉ * c2  <=?                      ⌈s0 * s1 / B⌉ * c2          | /c2
     *                      ⌈s0 / B⌉ * ⌈s1 / B⌉       <=?                      ⌈s0 * s1 / B⌉
     */
    def cross(allPlans: Seq[Component]): Component = allPlans.reduceRight[Component] {
      case (l, r) =>
        val cp = context.staticComponents.logicalPlanProducer.planCartesianProduct(l.plan, r.plan, context)
        val cpWithSelection = if (considerSelections) kit.select(cp, qg) else cp
        Component(l.queryGraph ++ r.queryGraph, cpWithSelection)
    }

    val bestPlan = cross(bestComponents)
    val bestSortedPlan = {
      // If there is a sort, there are 2 possible candidates for the best sorted plan.
      // The first in where we put the sorted component left-most, so that we don't have to plan an extra sort.
      val candidate1 = componentsWithSortedPlanFirst.map(cross).map(_.plan)
      // The second is taking the cheapest order of cartesian products, and planning an extra sort afterwards.
      val candidate2 = SortPlanner.maybeSortedPlan(
        bestPlan.plan,
        interestingOrderConfig,
        isPushDownSort = true,
        context,
        updateSolved = true
      )
      kit.pickBest(Set(candidate1, candidate2).flatten, s"best sorted plan for ${plans.map(_.queryGraph)}")
    }
    PlannedComponent(bestPlan.queryGraph, BestResults(bestPlan.plan, bestSortedPlan))
  }

  private def produceCartesianProducts(
    plans: Set[PlannedComponent],
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit
  ): Map[PlannedComponent, (PlannedComponent, PlannedComponent)] = {
    (for (t1 @ PlannedComponent(qg1, p1) <- plans; t2 @ PlannedComponent(qg2, p2) <- plans if p1 != p2) yield {
      val crossProduct =
        kit.select(
          context.staticComponents.logicalPlanProducer.planCartesianProduct(p1.bestResult, p2.bestResult, context),
          qg
        )
      (PlannedComponent(qg1 ++ qg2, BestResults(crossProduct, None)), (t1, t2))
    }).toMap
  }

  // Developers note: This method has been re-implemented in a very low-level imperative style, because
  // this code path caused a big SOAK regression for queries with 50-60 plans. The current implementation is
  // about 100x faster than the old one, please change functionality here with one eye on performance.
  private def produceNIJVariations(
    plans: Set[PlannedComponent],
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    singleComponentPlanner: SingleComponentPlannerTrait
  ): Map[PlannedComponent, (PlannedComponent, PlannedComponent)] = {
    val predicatesWithDependencies: Array[(Expression, Array[LogicalVariable])] =
      qg.selections.flatPredicates.toArray.map(pred => (pred, pred.dependencies.toArray))
    val planArray = plans.toArray
    val allCoveredIds: Array[Set[LogicalVariable]] = planArray.map(_.queryGraph.allCoveredIds)

    val result = Map.newBuilder[PlannedComponent, (PlannedComponent, PlannedComponent)]

    var a = 0
    while (a < planArray.length) {
      var b = a + 1
      while (b < planArray.length) {

        val planA = planArray(a).plan
        val planB = planArray(b).plan
        val qgA = planArray(a).queryGraph
        val qgB = planArray(b).queryGraph

        for (
          predicate <-
            this.predicatesDependendingOnBothSides(predicatesWithDependencies, allCoveredIds(a), allCoveredIds(b))
        ) {
          val nestedIndexJoinAB = planNIJIfApplicable(
            planA.bestResult,
            planB.bestResult,
            qgA,
            qgB,
            qg,
            interestingOrderConfig,
            predicate,
            context,
            kit,
            singleComponentPlanner
          )
          val nestedIndexJoinBA = planNIJIfApplicable(
            planB.bestResult,
            planA.bestResult,
            qgB,
            qgA,
            qg,
            interestingOrderConfig,
            predicate,
            context,
            kit,
            singleComponentPlanner
          )

          nestedIndexJoinAB.foreach(x => result += ((x, planArray(a) -> planArray(b))))
          nestedIndexJoinBA.foreach(x => result += ((x, planArray(a) -> planArray(b))))
        }
        b += 1
      }
      a += 1
    }

    result.result()
  }

  private def produceHashJoins(
    plans: Set[PlannedComponent],
    qg: QueryGraph,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit
  ): Map[PlannedComponent, (PlannedComponent, PlannedComponent)] = {
    (for {
      join <- joinPredicateCandidates(qg.selections.flatPredicates)
      t1 @ PlannedComponent(_, planA) <- plans if planA.bestResult.satisfiesExpressionDependencies(
        join.lhs
      ) && !planA.bestResult.satisfiesExpressionDependencies(join.rhs)
      t2 @ PlannedComponent(_, planB) <- plans if planB.bestResult.satisfiesExpressionDependencies(
        join.rhs
      ) && !planB.bestResult.satisfiesExpressionDependencies(join.lhs) && planA != planB
    } yield {
      val hashJoinAB = kit.select(
        context.staticComponents.logicalPlanProducer.planValueHashJoin(
          planA.bestResult,
          planB.bestResult,
          join.predicateToPlan,
          join.originalPredicate,
          context
        ),
        qg
      )
      val hashJoinBA = kit.select(
        context.staticComponents.logicalPlanProducer.planValueHashJoin(
          planB.bestResult,
          planA.bestResult,
          join.inversePredicateToPlan,
          join.originalPredicate,
          context
        ),
        qg
      )

      Set(
        (
          PlannedComponent(
            context.staticComponents.planningAttributes.solveds.get(hashJoinAB.id).asSinglePlannerQuery.lastQueryGraph,
            BestResults(hashJoinAB, None)
          ),
          t1 -> t2
        ),
        (
          PlannedComponent(
            context.staticComponents.planningAttributes.solveds.get(hashJoinBA.id).asSinglePlannerQuery.lastQueryGraph,
            BestResults(hashJoinBA, None)
          ),
          t1 -> t2
        )
      )

    }).flatten.toMap
  }

  private def planNIJIfApplicable(
    lhsPlan: LogicalPlan,
    rhsInputPlan: LogicalPlan,
    lhsQG: QueryGraph,
    rhsQG: QueryGraph,
    fullQG: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    predicate: Expression,
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    singleComponentPlanner: SingleComponentPlannerTrait
  ): Iterator[PlannedComponent] = {

    // We cannot plan NIJ if the RHS is more than one component or optional matches because that would require us to recurse into
    // JoinDisconnectedQueryGraphComponents instead of SingleComponentPlannerTrait.
    val notSingleComponent = rhsQG.connectedComponents.size > 1
    val containsOptionals = context.staticComponents.planningAttributes.solveds.get(
      rhsInputPlan.id
    ).asSinglePlannerQuery.lastQueryGraph.optionalMatches.nonEmpty

    if (notSingleComponent || containsOptionals) {
      Iterator.empty
    } else {
      planNIJ(
        lhsPlan,
        rhsInputPlan,
        lhsQG,
        rhsQG,
        interestingOrderConfig,
        Seq(predicate),
        context,
        kit,
        singleComponentPlanner
      ).map {
        result =>
          val resultWithSelection = kit.select(result, fullQG)
          PlannedComponent(
            context.staticComponents.planningAttributes.solveds.get(
              resultWithSelection.id
            ).asSinglePlannerQuery.lastQueryGraph,
            BestResults(resultWithSelection, None)
          )
      }
    }
  }

  /**
   * Index Nested Loop Joins -- if there is a value join connection between the LHS and RHS, and a useful index exists for
   * one of the sides, it can be used if the query is planned as an apply with the index seek on the RHS.
   *
   *   Apply
   * LHS  Index Seek
   */
  def planNIJ(
    lhsPlan: LogicalPlan,
    rhsInputPlan: LogicalPlan,
    lhsQG: QueryGraph,
    rhsQG: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    predicates: Seq[Expression],
    context: LogicalPlanningContext,
    kit: QueryPlannerKit,
    singleComponentPlanner: SingleComponentPlannerTrait
  ): Iterator[LogicalPlan] = {
    // Replan the RHS with the LHS arguments available. If good indexes exist, they can now be used
    // Also keep any hints we might have gotten in the rhsQG so they get considered during planning
    val rhsQgWithLhsArguments =
      context.staticComponents.planningAttributes.solveds.get(rhsInputPlan.id).asSinglePlannerQuery.lastQueryGraph
        .addArgumentIds(lhsQG.idsWithoutOptionalMatchesOrUpdates.toIndexedSeq)
        .addPredicates(predicates: _*)
        .addHints(rhsQG.hints)

    val (leftSymbols, rightSymbols) = predicates
      .view
      .flatMap(_.dependencies)
      .to(Set)
      .partition(lhsQG.idsWithoutOptionalMatchesOrUpdates.contains)

    rightSymbols match {
      case SetExtractor(rightSymbol) =>
        val contextForRhs = context.withModifiedPlannerState(_
          .withUpdatedLabelInfo(lhsPlan, context.staticComponents.planningAttributes.solveds))
        val leafPlanCandidates = {
          val contextForRhsLeaves =
            contextForRhs.withModifiedPlannerState(_.withConfig(context.plannerState.config.withLeafPlanners(
              QueryPlannerConfiguration.leafPlannersForNestedIndexJoins(LeafPlanRestrictions.OnlyIndexSeekPlansFor(
                rightSymbol,
                leftSymbols
              ))
            )))

          val componentInterestingOrderConfig = interestingOrderConfig.forQueryGraph(rhsQgWithLhsArguments)

          contextForRhsLeaves.plannerState.config.leafPlanners.candidates(
            rhsQgWithLhsArguments,
            interestingOrderConfig = componentInterestingOrderConfig,
            context = contextForRhsLeaves
          )
        }
        val rhsPlans =
          try {
            // planComponent throws if it can't find a solution, which is normally the expected behavior.
            // Here, however, restricting the leaf planners might lead to no solutions found and that is OK.
            Some(singleComponentPlanner.planComponent(
              leafPlanCandidates,
              rhsQgWithLhsArguments,
              contextForRhs,
              kit,
              interestingOrderConfig
            ))
          } catch {
            case _: InternalException =>
              None
          }

        // Keep only RHSs that actually leverage the data from the LHS to use an index.
        // The reason is that otherwise, we are producing a cartesian product disguising as an Apply, and
        // this confuses the cost model
        rhsPlans.fold[Iterator[LogicalPlan]](Iterator.empty)(_.allResults.iterator.collect {
          case rhsPlan if containsDependentIndexSeeks(rhsPlan) =>
            context.staticComponents.logicalPlanProducer.planApply(lhsPlan, rhsPlan, context)
        })
      case _ =>
        // If there are more than one dependency on RHS symbols, no index can solve the predicate
        Iterator.empty
    }
  }

  /**
   * Checks whether a plan contains an index seek that depends on a different variable than the one it is introducing.
   */
  def containsDependentIndexSeeks(plan: LogicalPlan): Boolean =
    plan.leaves.exists {
      case NodeIndexSeek(_, _, _, valueExpr, _, _, _, _) =>
        valueExpr.expressions.exists(_.dependencies.nonEmpty)
      case PartitionedNodeIndexSeek(_, _, _, valueExpr, _, _) =>
        valueExpr.expressions.exists(_.dependencies.nonEmpty)
      case NodeUniqueIndexSeek(_, _, _, valueExpr, _, _, _) =>
        valueExpr.expressions.exists(_.dependencies.nonEmpty)
      case DirectedRelationshipIndexSeek(_, _, _, _, _, valueExpr, _, _, _) =>
        valueExpr.expressions.exists(_.dependencies.nonEmpty)
      case UndirectedRelationshipIndexSeek(_, _, _, _, _, valueExpr, _, _, _) =>
        valueExpr.expressions.exists(_.dependencies.nonEmpty)
      case NodeIndexContainsScan(_, _, _, valueExpr, _, _, _) =>
        valueExpr.dependencies.nonEmpty
      case NodeIndexEndsWithScan(_, _, _, valueExpr, _, _, _) =>
        valueExpr.dependencies.nonEmpty
      case DirectedRelationshipIndexContainsScan(_, _, _, _, _, valueExpr, _, _, _) =>
        valueExpr.dependencies.nonEmpty
      case DirectedRelationshipIndexEndsWithScan(_, _, _, _, _, valueExpr, _, _, _) =>
        valueExpr.dependencies.nonEmpty
      case UndirectedRelationshipIndexContainsScan(_, _, _, _, _, valueExpr, _, _, _) =>
        valueExpr.dependencies.nonEmpty
      case UndirectedRelationshipIndexEndsWithScan(_, _, _, _, _, valueExpr, _, _, _) =>
        valueExpr.dependencies.nonEmpty
      case _ => false
    }

  case class JoinPredicate(
    // this is the predicate found in the query graph
    originalPredicate: Expression,
    // this is the equals predicate used for the join
    predicateToPlan: Equals
  ) {
    val lhs: Expression = predicateToPlan.lhs
    val rhs: Expression = predicateToPlan.rhs
    val inversePredicateToPlan: Equals = predicateToPlan.switchSides

    def inverse: JoinPredicate = copy(predicateToPlan = inversePredicateToPlan)
  }

  /**
   * Given all predicates, find the ones eligible for value hash joins.
   * Those are equality predicates where both sides have different non-empty dependencies.
   */
  def joinPredicateCandidates(flatPredicates: Seq[Expression]): Set[JoinPredicate] = flatPredicates.collect {
    case e @ Equals(l, r)
      if l.dependencies.nonEmpty &&
        r.dependencies.nonEmpty &&
        r.dependencies != l.dependencies => JoinPredicate(e, e)
    case in @ In(l, ListLiteral(Seq(r)))
      if l.dependencies.nonEmpty &&
        r.dependencies.nonEmpty &&
        r.dependencies != l.dependencies => JoinPredicate(in, Equals(l, r)(in.position))
  }.toSet

  /**
   * Find all the predicates that depend on both the RHS and the LHS.
   * Imperative implementation style for performance. See produceNIJVariations.
   */
  def predicatesDependendingOnBothSides(
    predicateDependencies: Array[(Expression, Array[LogicalVariable])],
    idsFromLeft: Set[LogicalVariable],
    idsFromRight: Set[LogicalVariable]
  ): Seq[Expression] =
    predicateDependencies.filter {
      case (_, deps) =>
        var i = 0
        var unfulfilledLhsDep = false
        var unfulfilledRhsDep = false
        var forAllLhsOrRhs = true

        while (i < deps.length) {
          val inLhs = idsFromLeft(deps(i))
          val inRhs = idsFromRight(deps(i))
          unfulfilledLhsDep = unfulfilledLhsDep || !inLhs
          unfulfilledRhsDep = unfulfilledRhsDep || !inRhs
          forAllLhsOrRhs = forAllLhsOrRhs && (inLhs || inRhs)
          i += 1
        }

        unfulfilledLhsDep && // The left plan is not enough
        unfulfilledRhsDep && // Neither is the right one
        forAllLhsOrRhs // But together we're good
    }.map(_._1)
}
