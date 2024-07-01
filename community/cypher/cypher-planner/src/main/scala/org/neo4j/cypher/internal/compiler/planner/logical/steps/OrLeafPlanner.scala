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
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.Ordering.orderedUnionColumns
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.Ordering.planDistinctOrOrderedDistinct
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.Ordering.planUnionOrOrderedUnion
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsElementIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.plans.AsIdSeekable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.DisjunctionWithRelatedPredicates
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
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Asc
import org.neo4j.cypher.internal.ir.ordering.ColumnOrder.Desc
import org.neo4j.cypher.internal.ir.ordering.InterestingOrderCandidate
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.collection.immutable.ListSet

import scala.collection.immutable.Set

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
      disjunction: DisjunctionWithRelatedPredicates,
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

  case class DisjunctionWithRelatedPredicates(
    disjunction: DisjunctionForOneVariable,
    relatedPredicates: Seq[DistributablePredicate]
  ) {

    override def toString: String =
      Seq(s"(${disjunction.toString})", s"${relatedPredicates.mkString(" AND ")}").mkString(" AND ")
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
      disjunctionWithRelatedPredicates: DisjunctionWithRelatedPredicates,
      context: LogicalPlanningContext
    ): QueryGraph = {
      val disjunction = disjunctionWithRelatedPredicates.disjunction
      val relatedPredicates = disjunctionWithRelatedPredicates.relatedPredicates
      lazy val predicatesInTheDisjunction = disjunction.predicates.collect {
        case w: WhereClausePredicate => w
      }.toSet
      // Each of the solved predicates may contain the relatedpredicates for the disjunction.
      lazy val predicatesRelatedToTheDisjunction = relatedPredicates.collect {
        case w: WhereClausePredicate => w.flattenConjunction
      }.flatten

      // Predicates that are not part of the original disjunction but part of the related predicates SHOULD be solved by all plans (since related predicates will add an AND relationship across the disjunction).
      // We will pick these up and add them to the new solved query graph.
      val relatedPredicatesSolvedByAllPlans = solvedQgs.head.selections.flatPredicatesSet.filter { predicate =>
        predicatesRelatedToTheDisjunction.contains(predicate) &&
        solvedQgs.tail.forall(_.selections.flatPredicatesSet.contains(predicate))
      }

      // Each plan should solve a part of the disjunction.
      // We identify all parts of the disjunction solved by each of the query plan.
      // Each query plan should solve (at least) one side of the disjunction + all the related plans
      val disjunctivePredicatesPerPlan = solvedQgs
        .map(_.selections.flatPredicatesSet)
        .collect({
          case solvedPredicateSet if solvedPredicateSet.nonEmpty =>
            val solvedDisjunctivePredicates = predicatesInTheDisjunction
              .collect({
                case predicateInDisjunction
                  if predicateInDisjunction.flattenConjunction.union(
                    relatedPredicatesSolvedByAllPlans
                  ) == solvedPredicateSet => predicateInDisjunction.e
              })
            if (solvedDisjunctivePredicates.isEmpty) {
              val predicatesSolvedByOnlyThisPlan = solvedPredicateSet.diff(relatedPredicatesSolvedByAllPlans)
              if (predicatesSolvedByOnlyThisPlan.nonEmpty)
                Set(Ands.create(
                  predicatesSolvedByOnlyThisPlan.to(ListSet)
                ))
              else
                Set.empty
            } else
              solvedDisjunctivePredicates
        }).flatten

      val qgWithPredicatesSolvedByAllPlans = qg.addPredicates(relatedPredicatesSolvedByAllPlans.to(Seq): _*)
      // If any of the plans does not provide a predicate to this, this amounts to providing `TRUE` which in turn makes the `Ors` to be created constant `TRUE`.
      // Thus, we leave it out.
      if (disjunctivePredicatesPerPlan.nonEmpty) {
        val ors = Ors.create(disjunctivePredicatesPerPlan.to(ListSet))
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

    def flattenConjunction: Set[Expression] = {
      e match {
        case Ands(compositePredicates) => compositePredicates
        case default                   => Set(default)
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
      disjunctionWithRelatedPredicates: DisjunctionWithRelatedPredicates,
      context: LogicalPlanningContext
    ): QueryGraph = {
      val disjunction = disjunctionWithRelatedPredicates.disjunction
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

    def findPlansPerPredicate(disjunctionWithRelatedPredicates: DisjunctionWithRelatedPredicates)
      : Array[Array[LogicalPlan]] = {
      val disjunction = disjunctionWithRelatedPredicates.disjunction
      val relatedPredicates = disjunctionWithRelatedPredicates.relatedPredicates
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

    def computeJoinedSolvedQueryGraph(
      plans: Seq[LogicalPlan],
      disjunctionWithRelatedPredicates: DisjunctionWithRelatedPredicates
    ): QueryGraph = {
      // Start by creating a query graph containing only the variables that are involved by the disjunction, and the correct arguments.
      val queryGraph = disjunctionWithRelatedPredicates.disjunction.qgWithOnlyRelevantVariable(bareQg)

      val solvedQgs = plans.map(solvedQueryGraph)

      // Let the predicate kinds add the predicates that each plan claims to solve to the queryGraph
      predicateKinds.foldLeft(queryGraph)((accQg, dp) =>
        dp.addSolvedToQueryGraph(accQg, solvedQgs, disjunctionWithRelatedPredicates, context)
      )
    }

    def mergePlansWithUnion(plans: Array[LogicalPlan], joinedSolvedQueryGraph: QueryGraph): LogicalPlan = {
      val distinctPlans = plans.distinct
      distinctPlans match {
        case Array(singlePlan) =>
          // This implies that the query plan solves both the lhs and the rhs of the disjunction. So map the query plan to combined joinedSolvedQueryGraph.
          context.staticComponents.logicalPlanProducer.updateSolvedForOr(singlePlan, joinedSolvedQueryGraph, context)
        case _ =>
          // Determines if we can plan OrderedUnion
          val maybeSortColumns = orderedUnionColumns(distinctPlans, context)

          // Join the plans with Union
          val unionPlan = distinctPlans.reduce[LogicalPlan] {
            case (p1, p2) => planUnionOrOrderedUnion(maybeSortColumns, p1, p2, Nil, context)
          }

          // Plan a single Distinct on top
          val orPlan = planDistinctOrOrderedDistinct(maybeSortColumns, unionPlan, context)

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
      disjunctionWithRelatedPredicates =
        DisjunctionWithRelatedPredicates(
          disjunction,
          predicateKinds.flatMap(_.collectRelatedPredicates(qg, disjunction)).toSeq
        )
      plansPerExpression = findPlansPerPredicate(disjunctionWithRelatedPredicates)
      // We can only solve the whole OR. If one predicate didn't yield any plan, we have to give up.
      if plansPerExpression.forall(_.nonEmpty)
      // Find each combination of best plans, with one best plan for each predicate in the disjunction
      combinations = combine(plansPerExpression)
      plans <- combinations
      if plans.nonEmpty
    } yield mergePlansWithUnion(
      plans,
      computeJoinedSolvedQueryGraph(plans, disjunctionWithRelatedPredicates)
    )
  }
}
