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
package org.neo4j.cypher.internal.compiler.planner.logical.steps

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsElementIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.DisjunctionForOneVariable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.InlinedRelationshipTypePredicateKind
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.WhereClausePredicateKind
import org.neo4j.cypher.internal.compiler.planner.logical.steps.leafPlanOptions.leafPlanHeuristic
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.logical
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

object OrLeafPlanner {

  /**
   * A kind of predicate and capabilities how to interact with the OrLeafPlanner at various stages in the computation.
   */
  sealed trait PredicateKind {

    /**
     * @return all disjunctions in the query graph of this kind.
     */
    def findDisjunctions(qg: QueryGraph): Seq[DisjunctionForOneVariable]

    /**
     * @return a query graph where all predicates of this kind have been removed
     */
    def stripAllFromQueryGraph(qg: QueryGraph): QueryGraph

    /**
     * @return all predicates of this kind in the query graph that use the same variable as in the given disjunction.
     */
    def collectRelatedPredicates(qg: QueryGraph, disjunction: DisjunctionForOneVariable): Seq[DistributablePredicate]

    /**
     * @param qg          a query graph to modify.
     * @param solvedQgs   for each predicate in the disjunction, the qg that the plan in the Union claims to solve.
     * @param disjunction the disjunction
     * @return a query graph where all predicates of this kind solved in solvedQgs are added to qg, as appropriate.
     */
    def addSolvedToQueryGraph(
      qg: QueryGraph,
      solvedQgs: Seq[QueryGraph],
      disjunction: DisjunctionForOneVariable,
      context: LogicalPlanningContext
    ): QueryGraph
  }

  /**
   * A disjunction of predicates that all use only one variable
   *
   * @param variable                   the variable
   * @param predicates                 the predicates.
   * @param interestingOrderCandidates if these candidates lead to different leaf plans, we can plan OrderedUnion instead of Union
   */
  case class DisjunctionForOneVariable(
    variable: LogicalVariable,
    predicates: Iterable[DistributablePredicate],
    interestingOrderCandidates: Seq[InterestingOrderCandidate]
  ) {
    override def toString: String = predicates.mkString(" OR ")

    def qgWithOnlyRelevantVariable(bareQg: QueryGraph): QueryGraph = {
      val solvedRel = bareQg.patternRelationships.find(_.variable == variable)
      QueryGraph(
        argumentIds = bareQg.argumentIds,
        patternNodes =
          solvedRel.fold(bareQg.patternNodes.filter(_ == variable))(r => Set(r.left, r.right)),
        patternRelationships = solvedRel.toSet,
        hints = bareQg.hints
      )
    }
  }

  /**
   * A predicate that can be distributed by the OrLeafPlanner
   */
  sealed trait DistributablePredicate {

    /**
     * Add this predicate to a query graph.
     */
    def addToQueryGraph(qg: QueryGraph): QueryGraph

    /**
     * Test whether this predicate is contained in a query graph.
     */
    def containedIn(qg: QueryGraph): Boolean
  }

  /**
   * Predicates which are expressed in the WHERE part, i.e. in queryGraph.selections.
   */
  final case object WhereClausePredicateKind extends PredicateKind {

    private def variableIfAllEqualHasLabelsOrRelTypes(expressions: Iterable[Expression]): Option[Expression] = {
      expressions.headOption
        .collect {
          case HasLabels(variable, _) => variable
          case HasTypes(variable, _)  => variable
        }
        .filter(variable =>
          expressions.tail.forall {
            case HasLabels(`variable`, _) => true
            case HasTypes(`variable`, _)  => true
            case _                        => false
          }
        )
    }

    override def findDisjunctions(qg: QueryGraph): Seq[DisjunctionForOneVariable] =
      qg.selections.flatPredicates.collect {
        case Ors(exprs) =>
          // All expressions in the OR must be for the same variable, otherwise we cannot solve it with Union of LeafPlans.
          variableUsedInExpression(exprs.head, qg.argumentIds) match {
            case Some(singleUsedVar)
              if exprs.tail.map(variableUsedInExpression(_, qg.argumentIds)).forall(_.contains(singleUsedVar)) =>
              val interestingOrderCandidates = for {
                v <- variableIfAllEqualHasLabelsOrRelTypes(exprs).toSeq
                // ASC before DESC because it is slightly cheaper
                indexOrder <- Seq(Asc(_, Map.empty), Desc(_, Map.empty))
              } yield InterestingOrderCandidate(Seq(indexOrder(v)))

              Some(DisjunctionForOneVariable(
                singleUsedVar,
                exprs.map(WhereClausePredicate),
                interestingOrderCandidates
              ))
            case _ => None
          }
      }.flatten

    override def stripAllFromQueryGraph(qg: QueryGraph): QueryGraph = qg.withSelections(Selections())

    override def collectRelatedPredicates(
      qg: QueryGraph,
      disjunction: DisjunctionForOneVariable
    ): Seq[DistributablePredicate] = {
      qg.selections.flatPredicates
        // IdSeekable predicates are never related
        .filter {
          case AsIdSeekable(_)        => false
          case AsElementIdSeekable(_) => false
          case _                      => true
        }
        .collect {
          // Those predicates which only use the variable that is used in the OR
          // Any Ors will not get added. Those can either be the disjunction itself, or any other OR which we can't solve with the leaf planners anyway.
          case e
            if variableUsedInExpression(e, qg.argumentIds).contains(disjunction.variable) &&
              !e.isInstanceOf[Ors] => WhereClausePredicate(e)
        }
    }

    override def addSolvedToQueryGraph(
      qg: QueryGraph,
      solvedQgs: Seq[QueryGraph],
      disjunction: DisjunctionForOneVariable,
      context: LogicalPlanningContext
    ): QueryGraph = {
      // Predicates solved by all plans can be added top-level, otherwise the planner will have to plan another Selection for them.
      val predicatesSolvedByAllPlans = solvedQgs.head.selections.flatPredicates.filter { predicate =>
        solvedQgs.tail.forall(_.selections.flatPredicates.contains(predicate))
      }

      // Predicates solved by only one plan each must be added inside an Ors
      val disjunctivePredicatesPerPlan = solvedQgs.map(_
        .selections
        .flatPredicates
        .filterNot(predicatesSolvedByAllPlans.contains))

      // We assume:
      // - disjunctivePredicatesPerPlan.flatten is a subset of disjunction.predicates
      //   - Either we have a rel-type disjunction, then disjunctivePredicatesPerPlan.flatten is empty
      //   - Or we have a where clause disjunction
      // - Each plan solves exactly one part from the disjunction.
      //   That can either be a single predicate or a conjunction or predicates.
      // - If one of the plan solves a predicate in the disjunction that is anyway solved by all other plans, then we will not get a predicate from this plan here.

      val qgWithPredicatesSolvedByAllPlans = qg.addPredicates(predicatesSolvedByAllPlans: _*)
      // If any of the plans does not provide a predicate to this, this amounts to providing `TRUE` which in turn makes the `Ors` to be created constant `TRUE`.
      // Thus, we leave it out.
      if (disjunctivePredicatesPerPlan.forall(_.nonEmpty)) {
        val ors = Ors(disjunctivePredicatesPerPlan.map(x => Ands.create(x.to(ListSet))))(InputPosition.NONE)
        qgWithPredicatesSolvedByAllPlans.addPredicates(ors)
      } else {
        qgWithPredicatesSolvedByAllPlans
      }
    }
  }

  final case class WhereClausePredicate(e: Expression) extends DistributablePredicate {

    override def addToQueryGraph(qg: QueryGraph): QueryGraph = e match {
      case HasTypes(variable: Variable, Seq(singleType)) =>
        InlinedRelationshipTypePredicate(variable, singleType).addToQueryGraph(qg)
      case _ => qg.addPredicates(e)
    }

    override def containedIn(qg: QueryGraph): Boolean = {
      val flatPredicates = qg.selections.flatPredicates
      e match {
        case Ands(compositePredicates) =>
          compositePredicates.forall(flatPredicates.contains)
        case _ => flatPredicates.contains(e)
      }
    }

    override def toString: String = ExpressionStringifier(e => e.asCanonicalStringVal)(e)
  }

  /**
   * Relationship type predicates which are inlined like (a)-[r:REL1|REL2]-()
   */
  final case object InlinedRelationshipTypePredicateKind extends PredicateKind {

    override def findDisjunctions(qg: QueryGraph): Seq[DisjunctionForOneVariable] = qg.patternRelationships.collect {
      case PatternRelationship(rel, _, _, types, SimplePatternLength) if types.distinct.length > 1 =>
        val interestingOrderCandidates = for {
          // ASC before DESC because it is slightly cheaper
          indexOrder <- Seq(Asc(_, Map.empty), Desc(_, Map.empty))
        } yield InterestingOrderCandidate(Seq(indexOrder(rel)))

        DisjunctionForOneVariable(
          rel,
          ListSet.from(types.map(InlinedRelationshipTypePredicate(rel, _))),
          interestingOrderCandidates
        )
    }.toSeq

    override def stripAllFromQueryGraph(qg: QueryGraph): QueryGraph =
      qg.withPatternRelationships(qg.patternRelationships.map(_.copy(types = Seq())))

    override def collectRelatedPredicates(
      qg: QueryGraph,
      disjunction: DisjunctionForOneVariable
    ): Seq[DistributablePredicate] = {
      def includesHasTypes(disjunction: DisjunctionForOneVariable) =
        disjunction.predicates.exists {
          case WhereClausePredicate(_: HasTypes) => true
          case _                                 => false
        }

      // We should only collect related inlined type predicates if there are no HasTypes in the disjunction,
      // otherwise we will have multiple type predicates for the same variable, which has no solution.
      if (!includesHasTypes(disjunction)) {
        qg.patternRelationships.toSeq.collect {
          // PatternRelationships that have inlined type predicates
          case rel @ PatternRelationship(disjunction.`variable`, _, _, types, SimplePatternLength) =>
            types.map(InlinedRelationshipTypePredicate(rel.variable, _))
        }.flatten
      } else {
        Seq.empty
      }
    }

    def addTypesToRelationship(qg: QueryGraph, variable: LogicalVariable, types: Seq[RelTypeName]): QueryGraph = {
      // Replace the rel without a predicate with a rel with a predicate
      val relWithoutInlinedTypePredicate = qg.patternRelationships.collectFirst {
        case pr @ PatternRelationship(`variable`, _, _, _, _) => pr
      }.head
      val relWithInlinedTypePredicate = relWithoutInlinedTypePredicate.copy(types = types)
      qg
        .removePatternRelationship(relWithoutInlinedTypePredicate)
        .addPatternRelationship(relWithInlinedTypePredicate)
    }

    override def addSolvedToQueryGraph(
      qg: QueryGraph,
      solvedQgs: Seq[QueryGraph],
      disjunction: DisjunctionForOneVariable,
      context: LogicalPlanningContext
    ): QueryGraph = {
      val relTypes = solvedQgs.map { solvedQG =>
        solvedQG.patternRelationships.collectFirst {
          case PatternRelationship(disjunction.`variable`, _, _, Seq(singleType), _) => singleType
        }
      }

      // If all plans solve the relationship, let's build the disjunction of solved types
      if (relTypes.forall(_.isDefined)) {
        val types = relTypes.flatten.distinct
        addTypesToRelationship(qg, disjunction.variable, types)
      } else {
        qg
      }
    }
  }

  final case class InlinedRelationshipTypePredicate(variable: LogicalVariable, typ: RelTypeName)
      extends DistributablePredicate {

    override def addToQueryGraph(qg: QueryGraph): QueryGraph =
      InlinedRelationshipTypePredicateKind.addTypesToRelationship(qg, variable, Seq(typ))

    override def containedIn(qg: QueryGraph): Boolean = qg.patternRelationships.exists {
      case PatternRelationship(`variable`, _, _, Seq(`typ`), _) => true
      case _                                                    => false
    }

    override def toString: String = ExpressionStringifier(e => e.asCanonicalStringVal)(
      HasTypes(variable, Seq(typ))(InputPosition.NONE)
    )
  }

  /**
   * If an expression uses exactly one non-argument variable, return it. Otherwise, return None.
   */
  private def variableUsedInExpression(e: Expression, argumentIds: Set[LogicalVariable]): Option[Variable] = {
    val nonArgVars = e.folder.findAllByClass[Variable].filterNot(v => argumentIds.contains(v))
    if (nonArgVars.distinct.size == 1) nonArgVars.headOption else None
  }
}

case class OrLeafPlanner(inner: Seq[LeafPlanner]) extends LeafPlanner {

  private val predicateKinds = Set(WhereClausePredicateKind, InlinedRelationshipTypePredicateKind)

  override def apply(
    qg: QueryGraph,
    interestingOrderConfig: InterestingOrderConfig,
    context: LogicalPlanningContext
  ): Set[LogicalPlan] = {
    val pickBest = context.plannerState.config.pickBestCandidate(context)
    val select = context.plannerState.config.applySelections

    // The queryGraph without any predicates
    val bareQg = predicateKinds.foldLeft(qg)((accQg, dp) => dp.stripAllFromQueryGraph(accQg))

    def solvedQueryGraph(plan: LogicalPlan): QueryGraph =
      context.staticComponents.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.tailOrSelf.queryGraph

    def findPlansPerPredicate(disjunction: DisjunctionForOneVariable): Array[Array[LogicalPlan]] = {
      // Collect any other top-level predicates that only use this variable
      val relatedPredicates = predicateKinds.flatMap(_.collectRelatedPredicates(qg, disjunction))

      // Keep only the node/rel variable around
      val qgWithOnlyRelevantVariable = disjunction.qgWithOnlyRelevantVariable(bareQg)

      // Add all related predicates to the queryGraph
      val qgWithRelatedPredicates =
        relatedPredicates.foldLeft(qgWithOnlyRelevantVariable)((accQg, dp) => dp.addToQueryGraph(accQg))

      // Add interesting order candidates to allow planning OrderedUnion
      val innerInterestingOrderConfig =
        disjunction.interestingOrderCandidates.foldLeft(interestingOrderConfig)(_.addInterestingOrderCandidate(_))

      // Add each expression in the OR separately
      disjunction.predicates.map { predicate =>
        val qgForExpression = predicate.addToQueryGraph(qgWithRelatedPredicates)

        // Obtain plans for the query graph with this expression added
        val innerLeafPlans = inner
          .flatMap(_(qgForExpression, innerInterestingOrderConfig, context)).distinct
          // Apply selections on top of the leaf plans.
          .map(select(_, qgForExpression, innerInterestingOrderConfig, context))
          // Only keep a plan if it actually solves the predicate from the disjunction
          .filter(plan => predicate.containedIn(solvedQueryGraph(plan)))

        // This is a Seq of possible solutions per expression
        // We really only want the best option
        pickBest(
          innerLeafPlans,
          leafPlanHeuristic(context),
          s"best plan for $predicate from disjunction $disjunction"
        ).toArray
      }.toArray
    }

    def computeJoinedSolvedQueryGraph(plans: Seq[LogicalPlan], disjunction: DisjunctionForOneVariable): QueryGraph = {
      // Start by creating a query graph containing only the variables that are involved by the disjunction, and the correct arguments.
      val queryGraph = disjunction.qgWithOnlyRelevantVariable(bareQg)

      val solvedQgs = plans.map(solvedQueryGraph)

      // Let the predicate kinds add the predicates that each plan claims to solve to the queryGraph
      predicateKinds.foldLeft(queryGraph)((accQg, dp) =>
        dp.addSolvedToQueryGraph(accQg, solvedQgs, disjunction, context)
      )
    }

    def mergePlansWithUnion(plans: Array[LogicalPlan], joinedSolvedQueryGraph: QueryGraph): LogicalPlan = {
      plans match {
        case Array(singlePlan) => singlePlan
        case _                 =>
          // Determines if we can plan OrderedUnion
          val maybeSortColumn = Option(context.staticComponents.planningAttributes.providedOrders(plans.head.id))
            // We only support a sorted union if the plans are sorted by a single column.
            .filter(_.columns.size == 1)
            // All plans must be ordered by the same thing.
            .filter(head =>
              plans.tail.map(p => context.staticComponents.planningAttributes.providedOrders(p.id)).forall(_ == head)
            )
            .flatMap(_.columns.headOption)
            // The only sort column must be by a variable. Convert to a logical plan ColumnOrder.
            .collect {
              case ordering.ColumnOrder.Asc(v: Variable, _)  => (v, logical.plans.Ascending(v))
              case ordering.ColumnOrder.Desc(v: Variable, _) => (v, logical.plans.Descending(v))
            }

          // Join the plans with Union
          val unionPlan = plans.reduce[LogicalPlan] {
            case (p1, p2) =>
              maybeSortColumn match {
                case Some((_, sortColumn)) =>
                  context.staticComponents.logicalPlanProducer.planOrderedUnion(
                    p1,
                    p2,
                    List(),
                    Seq(sortColumn),
                    context
                  )
                case None => context.staticComponents.logicalPlanProducer.planUnion(p1, p2, List(), context)
              }
          }

          // Plan a single Distinct on top
          val orPlan = maybeSortColumn match {
            // Parallel runtime does currently not support OrderedDistinct
            case Some((sortVariable, _)) if context.settings.executionModel.providedOrderPreserving =>
              context.staticComponents.logicalPlanProducer.planOrderedDistinctForUnion(
                unionPlan,
                Seq(sortVariable),
                context
              )
            case _ => context.staticComponents.logicalPlanProducer.planDistinctForUnion(unionPlan, context)
          }

          // Update solved with the joinedSolvedQueryGraph
          context.staticComponents.logicalPlanProducer.updateSolvedForOr(orPlan, joinedSolvedQueryGraph, context)
      }
    }

    for {
      predicateKind <- predicateKinds
      disjunction <- predicateKind.findDisjunctions(qg)
      // Maximum number of predicates on a single variable after which we give up trying to plan a distinct union to avoid stack overflow errors.
      // It was introduced after a query with > 7k types in a single relationship pattern landed us in trouble.
      if disjunction.predicates.size <= context.settings.predicatesAsUnionMaxSize
      // No point in doing OR leaf planning for less than 2 predicates
      if disjunction.predicates.size >= 2
      plansPerExpression = findPlansPerPredicate(disjunction)
      // We can only solve the whole OR. If one predicate didn't yield any plan, we have to give up.
      if plansPerExpression.forall(_.nonEmpty)
      // Find each combination of best plans, with one best plan for each predicate in the disjunction
      combinations = combine(plansPerExpression)
      plans <- combinations
      if plans.nonEmpty
      distinctPlans = plans.distinct
    } yield mergePlansWithUnion(distinctPlans, computeJoinedSolvedQueryGraph(distinctPlans, disjunction))
  }
}
