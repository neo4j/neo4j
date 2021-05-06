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

import org.neo4j.cypher.internal.compiler.planner.logical.LeafPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.planner.logical.ordering.InterestingOrderConfig
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.DisjunctionForOneVariable
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.InlinedRelationshipTypePredicateKind
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.WhereClausePredicateKind
import org.neo4j.cypher.internal.compiler.planner.logical.steps.OrLeafPlanner.bestPlansByHeuristic
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.helpers.SeqCombiner.combine
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.ordering
import org.neo4j.cypher.internal.logical
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexScan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeUniqueIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexContainsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexEndsWithScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.util.InputPosition

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
    def addSolvedToQueryGraph(qg: QueryGraph,
                              solvedQgs: Seq[QueryGraph],
                              disjunction: DisjunctionForOneVariable,
                              context: LogicalPlanningContext): QueryGraph
  }

  /**
   * A disjunction of predicates that all use only one variable
   *
   * @param variableName the name of the variable
   * @param predicates   the predicates.
   */
  case class DisjunctionForOneVariable(variableName: String, predicates: Seq[DistributablePredicate])

  /**
   * A predicate that can be distributed by the OrLeafPlanner
   */
  sealed trait DistributablePredicate {
    /**
     * Add this predicate to a query graph.
     */
    def addToQueryGraph(qg: QueryGraph): QueryGraph

    /**
     * Test whether a query graph contains this predicate.
     */
    def queryGraphContains(qg: QueryGraph): Boolean
  }

  /**
   * Predicates which are expressed in the WHERE part, i.e. in queryGraph.selections.
   */
  final case object WhereClausePredicateKind extends PredicateKind {

    override def findDisjunctions(qg: QueryGraph): Seq[DisjunctionForOneVariable] = qg.selections.flatPredicates.collect {
      case Ors(exprs) =>
        // All expressions in the OR must be for the same variable, otherwise we cannot solve it with Union of LeafPlans.
        variableUsedInExpression(exprs.head, qg.argumentIds) match {
          case Some(singleUsedVar) if exprs.tail.map(variableUsedInExpression(_, qg.argumentIds)).forall(_.contains(singleUsedVar)) =>
            Some(DisjunctionForOneVariable(singleUsedVar.name, exprs.map(WhereClausePredicate)))
          case _ => None
        }
    }.flatten

    override def stripAllFromQueryGraph(qg: QueryGraph): QueryGraph = qg.withSelections(Selections())

    override def collectRelatedPredicates(qg: QueryGraph, disjunction: DisjunctionForOneVariable): Seq[DistributablePredicate] = {
      qg.selections.flatPredicates.collect {
        // Those predicates which only use the variable that is used in the OR
        // Any Ors will not get added. Those can either be the disjunction itself, or any other OR which we can't solve with the leaf planners anyway.
        case e if variableUsedInExpression(e, qg.argumentIds).map(_.name).contains(disjunction.variableName) && !e.isInstanceOf[Ors] => WhereClausePredicate(e)
      }
    }

    override def addSolvedToQueryGraph(qg: QueryGraph,
                                       solvedQgs: Seq[QueryGraph],
                                       disjunction: DisjunctionForOneVariable,
                                       context: LogicalPlanningContext): QueryGraph = {
      // Predicates solved by all plans can be added top-level, otherwise the planner will have to plan another Selection for them.
      val predicatesSolvedByAllPlans = solvedQgs.head.selections.flatPredicates.filter { predicate =>
        solvedQgs.tail.forall(_.selections.flatPredicates.contains(predicate))
      }

      // Predicates solved by only one plan each must be added inside an Ors
      val disjunctivePredicates = solvedQgs.flatMap(_.selections.flatPredicates.filterNot(predicatesSolvedByAllPlans.contains))
      val qgWithPredicatesSolvedByAllPlans = qg.addPredicates(predicatesSolvedByAllPlans: _*)

      if (disjunctivePredicates.nonEmpty) {
        qgWithPredicatesSolvedByAllPlans.addPredicates(Ors(disjunctivePredicates)(InputPosition.NONE))
      } else {
        qgWithPredicatesSolvedByAllPlans
      }
    }
  }

  final case class WhereClausePredicate(e: Expression) extends DistributablePredicate {
    override def addToQueryGraph(qg: QueryGraph): QueryGraph = e match {
      case HasTypes(Variable(varName), Seq(singleType)) =>
        InlinedRelationshipTypePredicate(varName, singleType).addToQueryGraph(qg)
      case _ => qg.addPredicates(e)
    }

    override def queryGraphContains(qg: QueryGraph): Boolean = qg.selections.flatPredicates.contains(e)
  }

  /**
   * Relationship type predicates which are inlined like (a)-[r:REL1|REL2]-()
   */
  final case object InlinedRelationshipTypePredicateKind extends PredicateKind {

    override def findDisjunctions(qg: QueryGraph): Seq[DisjunctionForOneVariable] = qg.patternRelationships.collect {
      case PatternRelationship(name, _, _, types, SimplePatternLength) if types.length > 1 => DisjunctionForOneVariable(name, types.map(InlinedRelationshipTypePredicate(name, _)))
    }.toSeq

    override def stripAllFromQueryGraph(qg: QueryGraph): QueryGraph = qg.withPatternRelationships(qg.patternRelationships.map(_.copy(types = Seq())))

    override def collectRelatedPredicates(qg: QueryGraph, disjunction: DisjunctionForOneVariable): Seq[DistributablePredicate] = {
      def includesHasTypes(disjunction: DisjunctionForOneVariable) =
        disjunction.predicates.exists {
          case WhereClausePredicate(e) => e.isInstanceOf[HasTypes]
          case _ => false
        }

      // We should only collect related inlined type predicates if there are no HasTypes in the disjunction,
      // otherwise we will have multiple type predicates for the same variable, which has no solution.
      if (!includesHasTypes(disjunction)) {
        qg.patternRelationships.toSeq.collect {
          // PatternRelationships that have inlined type predicates
          case rel@PatternRelationship(disjunction.variableName, _, _, types, SimplePatternLength) => types.map(InlinedRelationshipTypePredicate(rel.name, _))
        }.flatten
      } else {
        Seq.empty
      }
    }

    def addTypesToRelationship(qg: QueryGraph, variableName: String, types: Seq[RelTypeName]): QueryGraph = {
      // Replace the rel without a predicate with a rel with a predicate
      val relWithoutInlinedTypePredicate = qg.patternRelationships.find {
        case PatternRelationship(`variableName`, _, _, _, _) => true
        case _ => false
      }.head
      val relWithInlinedTypePredicate = relWithoutInlinedTypePredicate.copy(types = types)
      qg
        .withoutPatternRelationships(Set(relWithoutInlinedTypePredicate))
        .withPatternRelationships(Set(relWithInlinedTypePredicate))
    }

    override def addSolvedToQueryGraph(qg: QueryGraph,
                                       solvedQgs: Seq[QueryGraph],
                                       disjunction: DisjunctionForOneVariable,
                                       context: LogicalPlanningContext): QueryGraph = {
      val relTypes = solvedQgs.map { solvedQG =>
        solvedQG.patternRelationships.collectFirst {
          case PatternRelationship(disjunction.variableName, _, _, Seq(singleType), _) => singleType
        }
      }

      // If all plans solve the relationship, let's build the disjunction of solved types
      if (relTypes.forall(_.isDefined)) {
        val types = relTypes.flatten.distinct
        addTypesToRelationship(qg, disjunction.variableName, types)
      } else {
        qg
      }
    }
  }

  final case class InlinedRelationshipTypePredicate(variableName: String, typ: RelTypeName) extends DistributablePredicate {
    override def addToQueryGraph(qg: QueryGraph): QueryGraph = InlinedRelationshipTypePredicateKind.addTypesToRelationship(qg, variableName, Seq(typ))

    override def queryGraphContains(qg: QueryGraph): Boolean = qg.patternRelationships.exists {
      case PatternRelationship(`variableName`, _, _, Seq(`typ`), _) => true
      case _ => false
    }
  }

  /**
   * If an expression uses exactly one non-argument variable, return it. Otherwise, return None.
   */
  private def variableUsedInExpression(e: Expression, argumentIds: Set[String]): Option[Variable] = {
    val nonArgVars = e.findByAllClass[Variable].filterNot(v => argumentIds.contains(v.name))
    if (nonArgVars.distinct.size == 1) nonArgVars.headOption else None
  }

  private def bestPlansByHeuristic(plans: Seq[LogicalPlan]): Seq[LogicalPlan] = {
    if (plans.isEmpty) plans else plans.groupBy(planOrderingHeuristic).minBy(_._1)._2
  }

  private def planOrderingHeuristic(logicalPlan: LogicalPlan): Int = logicalPlan match {
    case _: NodeIndexSeek |
         _: NodeUniqueIndexSeek |
         _: NodeIndexEndsWithScan |
         _: NodeIndexContainsScan |
         _: DirectedRelationshipIndexEndsWithScan |
         _: UndirectedRelationshipIndexEndsWithScan |
         _: DirectedRelationshipIndexContainsScan |
         _: UndirectedRelationshipIndexContainsScan |
         _: DirectedRelationshipIndexSeek |
         _: UndirectedRelationshipIndexSeek |
         _: DirectedRelationshipIndexEndsWithScan |
         _: UndirectedRelationshipIndexEndsWithScan |
         _: DirectedRelationshipIndexContainsScan |
         _: UndirectedRelationshipIndexContainsScan => 0
    case _: NodeIndexScan |
         _: UndirectedRelationshipIndexScan |
         _: DirectedRelationshipIndexScan => 1
    case _: NodeByLabelScan |
         _: DirectedRelationshipTypeScan |
         _: UndirectedRelationshipTypeScan => 2
    case _ => 3
  }

}

case class OrLeafPlanner(inner: Seq[LeafPlanner]) extends LeafPlanner {

  private val predicateKinds = Seq(WhereClausePredicateKind, InlinedRelationshipTypePredicateKind)

  override def apply(qg: QueryGraph, interestingOrderConfig: InterestingOrderConfig, context: LogicalPlanningContext): Seq[LogicalPlan] = {
    // The queryGraph without any predicates
    val bareQg = predicateKinds.foldLeft(qg)((accQg, dp) => dp.stripAllFromQueryGraph(accQg))

    def solvedQueryGraph(plan: LogicalPlan): QueryGraph = context.planningAttributes.solveds.get(plan.id).asSinglePlannerQuery.tailOrSelf.queryGraph

    def findPlansPerPredicate(disjunction: DisjunctionForOneVariable): Array[Array[LogicalPlan]] = {
      // Collect any other top-level predicates that only use this variable
      val relatedPredicates = predicateKinds.flatMap(_.collectRelatedPredicates(qg, disjunction))

      // Add all related predicates to the bare queryGraph
      val qgWithRelatedPredicates = relatedPredicates.foldLeft(bareQg)((accQg, dp) => dp.addToQueryGraph(accQg))

      // Add each expression in the OR separately
      disjunction.predicates.map { predicate =>
        val qgForExpression = predicate.addToQueryGraph(qgWithRelatedPredicates)

        // Obtain plans for each for the query graph with this expression added
        val innerLeafPlans = inner.flatMap(_ (qgForExpression, interestingOrderConfig, context)).distinct

        // This is a Seq of possible solutions per expression
        // We really only want the best option IndexSeek > IndexScan > LabelScan as combine() explodes to p^n
        // (number of plans ^ number of predicates) so we really want p to be 1
        bestPlansByHeuristic(innerLeafPlans)
          // Apply selections on top of the best leaf plans.
          .map(p => context.config.applySelections(p, qgForExpression, interestingOrderConfig, context))
          // Only keep a plan if it actually solves the predicate from the disjunction
          .filter(plan => predicate.queryGraphContains(solvedQueryGraph(plan)))
          .toArray
      }.toArray
    }

    def computeJoinedSolvedQueryGraph(plans: Seq[LogicalPlan], disjunction: DisjunctionForOneVariable): QueryGraph = {
      val solvedRel = bareQg.patternRelationships.find(_.name == disjunction.variableName)
      // Start by creating a query graph containing only the variables that are involved by the disjunction, and the correct arguments.
      val queryGraph = QueryGraph(
        argumentIds = bareQg.argumentIds,
        patternNodes = solvedRel.fold(bareQg.patternNodes.filter(_ == disjunction.variableName))(r => Set(r.left, r.right)),
        patternRelationships = solvedRel.toSet
      )

      val solvedQgs = plans.map(solvedQueryGraph)

      // Let the predicate kinds add the predicates that each plan claims to solve to the queryGraph
      predicateKinds.foldLeft(queryGraph)((accQg, dp) => dp.addSolvedToQueryGraph(accQg, solvedQgs, disjunction, context))
    }

    def mergePlansWithUnion(plans: Array[LogicalPlan], joinedSolvedQueryGraph: QueryGraph): LogicalPlan = {
      // Determines if we can plan OrderedUnion
      val maybeSortColumn = Option(context.planningAttributes.providedOrders(plans.head.id))
        // We only support a sorted union if the plans are sorted by a single column.
        .filter(_.columns.size == 1)
        // All plans must be ordered by the same thing.
        .filter(head => plans.tail.map(p => context.planningAttributes.providedOrders(p.id)).forall(_ == head))
        .flatMap(_.columns.headOption)
        // The only sort column must be by a variable. Convert to a logical plan ColumnOrder.
        .collect {
          case ordering.ColumnOrder.Asc(v@Variable(varName), _) => (v, logical.plans.Ascending(varName))
          case ordering.ColumnOrder.Desc(v@Variable(varName), _) => (v, logical.plans.Descending(varName))
        }

      // Join the plans with Union
      val unionPlan = plans.reduce[LogicalPlan] {
        case (p1, p2) =>
          maybeSortColumn match {
            case Some((_, sortColumn)) => context.logicalPlanProducer.planOrderedUnion(p1, p2, List(), Seq(sortColumn), context)
            case None => context.logicalPlanProducer.planUnion(p1, p2, List(), context)
          }
      }

      // Plan a single Distinct on top
      val orPlan = maybeSortColumn match {
        case Some((sortVariable, _)) => context.logicalPlanProducer.planOrderedDistinctForUnion(unionPlan, Seq(sortVariable), context)
        case None => context.logicalPlanProducer.planDistinctForUnion(unionPlan, context)
      }

      // Update solved with the joinedSolvedQueryGraph
      context.logicalPlanProducer.updateSolvedForOr(orPlan, joinedSolvedQueryGraph, context)
    }

    for {
      predicateKind <- predicateKinds
      disjunction <- predicateKind.findDisjunctions(qg)
      plansPerExpression = findPlansPerPredicate(disjunction)
      // We can only solve the whole OR. If one predicate didn't yield any plan, we have to give up.
      if plansPerExpression.forall(_.nonEmpty)
      // Find each combination of best plans, with one best plan for each predicate in the disjunction
      combinations = combine(plansPerExpression)
      plans <- combinations
      if plans.nonEmpty
    } yield mergePlansWithUnion(plans, computeJoinedSolvedQueryGraph(plans, disjunction))
  }
}
