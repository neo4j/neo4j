/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v3_5.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_5.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.v3_5.helpers.PredicateHelper.coercePredicates
import org.neo4j.cypher.internal.compiler.v3_5.planner._
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_5.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.ir.v3_5._
import org.neo4j.cypher.internal.planner.v3_5.spi.PlanningAttributes
import org.neo4j.cypher.internal.v3_5.ast
import org.neo4j.cypher.internal.v3_5.ast._
import org.neo4j.cypher.internal.v3_5.expressions._
import org.neo4j.cypher.internal.v3_5.logical.plans
import org.neo4j.cypher.internal.v3_5.logical.plans.{DeleteExpression => DeleteExpressionPlan, Limit => LimitPlan, LoadCSV => LoadCSVPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.v3_5.util.AssertionRunner.Thunk
import org.neo4j.cypher.internal.v3_5.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.v3_5.util.attribution.{Attributes, IdGen}
import org.neo4j.cypher.internal.v3_5.util.{AssertionRunner, ExhaustiveShortestPathForbiddenException, InternalException}

/*
 * The responsibility of this class is to produce the correct solved PlannerQuery when creating logical plans.
 * No other functionality or logic should live here - this is supposed to be a very simple class that does not need
 * much testing
 */
case class LogicalPlanProducer(cardinalityModel: CardinalityModel, planningAttributes: PlanningAttributes, idGen: IdGen) extends ListSupport {

  implicit val implicitIdGen: IdGen = idGen
  private val solveds = planningAttributes.solveds
  private val cardinalities = planningAttributes.cardinalities
  private val providedOrders = planningAttributes.providedOrders

  def planLock(plan: LogicalPlan, nodesToLock: Set[String], context: LogicalPlanningContext): LogicalPlan =
    annotate(LockNodes(plan, nodesToLock), solveds.get(plan.id), providedOrders.get(plan.id), context)

  def solvePredicate(plan: LogicalPlan, solvedExpression: Expression, context: LogicalPlanningContext): LogicalPlan = {
    // Keep other attributes but change solved
    val keptAttributes = Attributes(idGen, cardinalities, providedOrders)
    val newPlan = plan.copyPlanWithIdGen(keptAttributes.copy(plan.id))
    val solvedPlannerQuery = solveds.get(plan.id).amendQueryGraph(_.addPredicates(solvedExpression))
    solveds.set(newPlan.id, solvedPlannerQuery)
    newPlan
  }

  def planAllNodesScan(idName: String, argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph(argumentIds = argumentIds, patternNodes = Set(idName)))
    // Is this ordered by node id?
    annotate(AllNodesScan(idName, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved: PlannerQuery = solveds.get(right.id).updateTailOrSelf(_.amendQueryGraph(_.withArgumentIds(Set.empty)))
    val solved: PlannerQuery = solveds.get(left.id) ++ rhsSolved
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id)
    annotate(Apply(left, right), solved, providedOrder, context)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.withTail(solveds.get(right.id)))
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id)
    annotate(Apply(left, right), solved, providedOrder, context)
  }

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(left.id)
    annotate(CartesianProduct(left, right), solved, providedOrder, context)
  }

  def planDirectedRelationshipByIdSeek(idName: String,
                                       relIds: SeekableArgs,
                                       startNode: String,
                                       endNode: String,
                                       pattern: PatternRelationship,
                                       argumentIds: Set[String],
                                       solvedPredicates: Seq[Expression] = Seq.empty,
                                       context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    // Is this ordered by relationship id?
    annotate(DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planUndirectedRelationshipByIdSeek(idName: String,
                                         relIds: SeekableArgs,
                                         leftNode: String,
                                         rightNode: String,
                                         pattern: PatternRelationship,
                                         argumentIds: Set[String],
                                         solvedPredicates: Seq[Expression] = Seq.empty,
                                         context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    // Is this ordered by relationship id?
    annotate(UndirectedRelationshipByIdSeek(idName, relIds, leftNode, rightNode, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planSimpleExpand(left: LogicalPlan,
                       from: String,
                       dir: SemanticDirection,
                       to: String,
                       pattern: PatternRelationship,
                       mode: ExpansionMode,
                       context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).amendQueryGraph(_.addPatternRelationship(pattern))
    val providedOrder = providedOrders.get(left.id)
    annotate(Expand(left, from, dir, pattern.types, to, pattern.name, mode), solved, providedOrder, context)
  }

  def planVarExpand(source: LogicalPlan,
                    from: String,
                    dir: SemanticDirection,
                    to: String,
                    pattern: PatternRelationship,
                    temporaryNode: String,
                    temporaryRelationship: String,
                    relationshipPredicate: Expression,
                    nodePredicate: Expression,
                    solvedPredicates: Seq[Expression],
                    legacyPredicates: Seq[(LogicalVariable, Expression)] = Seq.empty,
                    mode: ExpansionMode,
                    context: LogicalPlanningContext): LogicalPlan = pattern.length match {
    case l: VarPatternLength =>
      val projectedDir = projectedDirection(pattern, from, dir)

      val solved = solveds.get(source.id).amendQueryGraph(_
        .addPatternRelationship(pattern)
        .addPredicates(solvedPredicates: _*)
      )
      annotate(VarExpand(
        source = source,
        from = from,
        dir = dir,
        projectedDir = projectedDir,
        types = pattern.types,
        to = to,
        relName = pattern.name,
        length = l,
        mode = mode,
        tempNode = temporaryNode,
        tempRelationship = temporaryRelationship,
        nodePredicate = nodePredicate,
        relationshipPredicate = relationshipPredicate,
        legacyPredicates = legacyPredicates), solved, providedOrders.get(source.id), context)

    case _ => throw new InternalException("Expected a varlength path to be here")
  }

  def planHiddenSelection(predicates: Seq[Expression], left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    annotate(Selection(coercePredicates(predicates), left), solveds.get(left.id), providedOrders.get(left.id), context)
  }

  def planNodeByIdSeek(idName: String, nodeIds: SeekableArgs,
                       solvedPredicates: Seq[Expression] = Seq.empty,
                       argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    // Is this ordered by node id?
    annotate(NodeByIdSeek(idName, nodeIds, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planNodeByLabelScan(idName: String, label: LabelName, solvedPredicates: Seq[Expression],
                          solvedHint: Option[UsingScanHint] = None, argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    // Is this ordered by node id?
    annotate(NodeByLabelScan(idName, label, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planNodeIndexSeek(idName: String,
                        label: LabelToken,
                        properties: Seq[IndexedProperty],
                        valueExpr: QueryExpression[Expression],
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedPredicatesForCardinalityEstimation: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[String],
                        providedOrder: ProvidedOrder,
                        context: LogicalPlanningContext): LogicalPlan = {
    val queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    // We know solvedPredicates is a subset of solvedPredicatesForCardinalityEstimation
    val solved = RegularPlannerQuery(queryGraph = queryGraph)
    val solvedForCardinalityEstimation = RegularPlannerQuery(queryGraph.addPredicates(solvedPredicatesForCardinalityEstimation: _*))

    val plan = NodeIndexSeek(idName, label, properties, valueExpr, argumentIds, toIndexOrder(providedOrder))
    val cardinality = cardinalityModel(solvedForCardinalityEstimation, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    providedOrders.set(plan.id, providedOrder)
    plan
  }

  def planNodeIndexScan(idName: String,
                        label: LabelToken,
                        property: IndexedProperty,
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[String],
                        providedOrder: ProvidedOrder,
                        context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeIndexScan(idName, label, property, argumentIds, toIndexOrder(providedOrder)), solved, providedOrder, context)
  }

  def planNodeIndexContainsScan(idName: String,
                                label: LabelToken,
                                property: IndexedProperty,
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[String],
                                providedOrder: ProvidedOrder,
                                context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeIndexContainsScan(idName, label, property, valueExpr, argumentIds, toIndexOrder(providedOrder)), solved, providedOrder, context)
  }

  def planNodeIndexEndsWithScan(idName: String,
                                label: LabelToken,
                                property: IndexedProperty,
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[String],
                                providedOrder: ProvidedOrder,
                                context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeIndexEndsWithScan(idName, label, property, valueExpr, argumentIds, toIndexOrder(providedOrder)), solved, providedOrder, context)
  }

  def planNodeHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Seq[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val plannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    val solved = plannerQuery.amendQueryGraph(_.addHints(hints))
    annotate(NodeHashJoin(nodes, left, right), solved, providedOrders.get(right.id), context)
  }

  def planValueHashJoin(left: LogicalPlan, right: LogicalPlan, join: Equals, originalPredicate: Equals, context: LogicalPlanningContext): LogicalPlan = {
    val plannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    val solved = plannerQuery.amendQueryGraph(_.addPredicates(originalPredicate))
    annotate(ValueHashJoin(left, right, join), solved, providedOrders.get(right.id), context)
  }

  def planNodeUniqueIndexSeek(idName: String,
                              label: LabelToken,
                              properties: Seq[IndexedProperty],
                              valueExpr: QueryExpression[Expression],
                              solvedPredicates: Seq[Expression] = Seq.empty,
                              solvedPredicatesForCardinalityEstimation: Seq[Expression] = Seq.empty,
                              solvedHint: Option[UsingIndexHint] = None,
                              argumentIds: Set[String],
                              providedOrder: ProvidedOrder,
                              context: LogicalPlanningContext): LogicalPlan = {
    val queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    // We know solvedPredicates is a subset of solvedPredicatesForCardinalityEstimation
    val solved = RegularPlannerQuery(queryGraph = queryGraph)
    val solvedForCardinalityEstimation = RegularPlannerQuery(queryGraph.addPredicates(solvedPredicatesForCardinalityEstimation: _*))

    val plan = NodeUniqueIndexSeek(idName, label, properties, valueExpr, argumentIds, toIndexOrder(providedOrder))
    val cardinality = cardinalityModel(solvedForCardinalityEstimation, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    providedOrders.set(plan.id, providedOrder)
    plan
  }

  def planAssertSameNode(node: String, left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    annotate(AssertSameNode(node, left, right), solved, providedOrders.get(left.id), context)
  }

  def planOptionalExpand(left: LogicalPlan,
                         from: String,
                         dir: SemanticDirection,
                         to: String,
                         pattern: PatternRelationship,
                         mode: ExpansionMode = ExpandAll,
                         predicates: Seq[Expression] = Seq.empty,
                         solvedQueryGraph: QueryGraph,
                         context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).amendQueryGraph(_.withAddedOptionalMatch(solvedQueryGraph))
    annotate(OptionalExpand(left, from, dir, pattern.types, to, pattern.name, mode, predicates), solved, providedOrders.get(left.id), context)
  }

  def planOptional(inputPlan: LogicalPlan, ids: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .withAddedOptionalMatch(solveds.get(inputPlan.id).queryGraph)
      .withArgumentIds(ids)
    )
    annotate(Optional(inputPlan, ids), solved, providedOrders.get(inputPlan.id), context)
  }

  def planActiveRead(inputPlan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inputPlan.id)
    annotate(ActiveRead(inputPlan), solved, providedOrders.get(inputPlan.id), context)
  }

  def planLeftOuterHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Seq[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).amendQueryGraph(_.withAddedOptionalMatch(solveds.get(right.id).queryGraph.addHints(hints)))
    val providedOrder = providedOrders.get(right.id).upToExcluding(nodes)
    annotate(LeftOuterHashJoin(nodes, left, right), solved, providedOrder, context)
  }

  def planRightOuterHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Seq[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(right.id).amendQueryGraph(_.withAddedOptionalMatch(solveds.get(left.id).queryGraph.addHints(hints)))
    annotate(RightOuterHashJoin(nodes, left, right), solved, providedOrders.get(right.id), context)
  }

  def planSelection(left: LogicalPlan, predicates: Seq[Expression], reported: Seq[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(reported: _*)))
    annotate(Selection(coercePredicates(predicates), left), solved, providedOrders.get(left.id), context)
  }

  def planHorizonSelection(left: LogicalPlan, predicates: Seq[Expression], reported: Seq[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.updateHorizon {
      case p: QueryProjection => p.addPredicates(reported: _*)
      case _ => throw new IllegalArgumentException("You can only plan HorizonSelection after a projection")
    })
    annotate(Selection(coercePredicates(predicates), left), solved, providedOrders.get(left.id), context)
  }

  def planSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(SelectOrAntiSemiApply(outer, inner, expr), solveds.get(outer.id), providedOrders.get(outer.id), context)

  def planLetSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: String, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetSelectOrAntiSemiApply(outer, inner, id, expr), solveds.get(outer.id), providedOrders.get(outer.id), context)

  def planSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(SelectOrSemiApply(outer, inner, expr), solveds.get(outer.id), providedOrders.get(outer.id), context)

  def planLetSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: String, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetSelectOrSemiApply(outer, inner, id, expr), solveds.get(outer.id), providedOrders.get(outer.id), context)

  def planLetAntiSemiApply(left: LogicalPlan, right: LogicalPlan, id: String, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetAntiSemiApply(left, right, id), solveds.get(left.id), providedOrders.get(left.id), context)

  def planLetSemiApply(left: LogicalPlan, right: LogicalPlan, id: String, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetSemiApply(left, right, id), solveds.get(left.id), providedOrders.get(left.id), context)

  def planAntiSemiApply(left: LogicalPlan, right: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(AntiSemiApply(left, right), solved, providedOrders.get(left.id), context)
  }

  def planSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
    annotate(SemiApply(left, right), solved, providedOrders.get(left.id), context)
  }

  def planQueryArgument(queryGraph: QueryGraph, context: LogicalPlanningContext): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels = queryGraph.patternRelationships.filter(rel => queryGraph.argumentIds.contains(rel.name))
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgument(patternNodes, patternRels, otherIds, context)
  }

  def planArgumentFrom(plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(Argument(plan.availableSymbols), solveds.get(plan.id), ProvidedOrder.empty, context)

  def planArgument(patternNodes: Set[String],
                   patternRels: Set[PatternRelationship] = Set.empty,
                   other: Set[String] = Set.empty,
                   context: LogicalPlanningContext): LogicalPlan = {
    val relIds = patternRels.map(_.name)
    val coveredIds = patternNodes ++ relIds ++ other

    val solved = RegularPlannerQuery(queryGraph =
      QueryGraph(
        argumentIds = coveredIds,
        patternNodes = patternNodes,
        patternRelationships = Set.empty
      ))

    annotate(Argument(coveredIds), solved, ProvidedOrder.empty, context)
  }

  def planArgument(context: LogicalPlanningContext): LogicalPlan =
    annotate(Argument(Set.empty), PlannerQuery.empty, ProvidedOrder.empty, context)

  def planEmptyProjection(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(EmptyResult(inner), solveds.get(inner.id), ProvidedOrder.empty, context)

  def planStarProjection(inner: LogicalPlan, reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val newSolved: PlannerQuery = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.withAddedProjections(reported)))
    // Keep some attributes, but change solved
    val keptAttributes = Attributes(idGen, cardinalities, providedOrders)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    annotate(newPlan, newSolved, providedOrders.get(inner.id), context)
  }

  def planRegularProjection(inner: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.withAddedProjections(reported)))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(inner.id).columns, expressions)

    val providedOrder = ProvidedOrder(columnsWithRenames)

    annotate(Projection(inner, expressions), solved, providedOrder, context)
  }

  def planAggregation(left: LogicalPlan,
                      grouping: Map[String, Expression],
                      aggregation: Map[String, Expression],
                      reportedGrouping: Map[String, Expression],
                      reportedAggregation: Map[String, Expression],
                      context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingExpressions = reportedGrouping, aggregationExpressions = reportedAggregation)
    ))
    // Trim provided order for each sort column, if it is a non-grouping column
    val trimmed = providedOrders.get(left.id).columns.takeWhile {
      case ProvidedOrder.ColumnOfProperty((varName, propName)) =>
        grouping.values.exists {
          case CachedNodeProperty(`varName`, PropertyKeyName(`propName`)) => true
          case Property(Variable(`varName`), PropertyKeyName(`propName`)) => true
          case _ => false
        }
      case ProvidedOrder.Column(varName) =>
        grouping.values.exists {
          case Variable(`varName`) => true
          case _ => false
        }
    }
    val trimmedAndRenamed = renameProvidedOrderColumns(trimmed, grouping)

    annotate(Aggregation(left, grouping, aggregation), solved, ProvidedOrder(trimmedAndRenamed), context)
  }

  /**
    * The only purpose of this method is to set the solved correctly for something that is already sorted.
    */
  def updateSolvedForSortedItems(inner: LogicalPlan,
                                 items: Seq[ast.SortItem],
                                 interestingOrder: InterestingOrder,
                                 context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSortItems(items))).withInterestingOrder(interestingOrder))
    val providedOrder = providedOrders.get(inner.id)
    annotate(inner.copyPlanWithIdGen(idGen), solved, providedOrder, context)
  }

  def planRollup(lhs: LogicalPlan,
                 rhs: LogicalPlan,
                 collectionName: String,
                 variableToCollect: String,
                 nullable: Set[String],
                 context: LogicalPlanningContext): LogicalPlan = {
    annotate(RollUpApply(lhs, rhs, collectionName, variableToCollect, nullable), solveds.get(lhs.id), providedOrders.get(lhs.id), context)
  }

  def planCountStoreNodeAggregation(query: PlannerQuery, projectedColumn: String, labels: List[Option[LabelName]], argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(query.queryGraph, query.interestingOrder, query.horizon)
    annotate(NodeCountFromCountStore(projectedColumn, labels, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planCountStoreRelationshipAggregation(query: PlannerQuery, idName: String, startLabel: Option[LabelName],
                                            typeNames: Seq[RelTypeName], endLabel: Option[LabelName], argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = RegularPlannerQuery(query.queryGraph, query.interestingOrder, query.horizon)
    annotate(RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, argumentIds), solved, ProvidedOrder.empty, context)
  }

  def planSkip(inner: LogicalPlan, count: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSkipExpression(count))))
    annotate(SkipPlan(inner, count), solved, providedOrders.get(inner.id), context)
  }

  def planLoadCSV(inner: LogicalPlan, variableName: String, url: Expression, format: CSVFormat, fieldTerminator: Option[StringLiteral], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(LoadCSVProjection(variableName, url, format, fieldTerminator)))
    annotate(LoadCSVPlan(inner, url, variableName, format, fieldTerminator.map(_.value), context.legacyCsvQuoteEscaping,
                         context.csvBufferSize), solved, providedOrders.get(inner.id), context)
  }

  def planUnwind(inner: LogicalPlan, name: String, expression: Expression, reported: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(UnwindProjection(name, reported)))
    annotate(UnwindCollection(inner, name, expression), solved, providedOrders.get(inner.id), context)
  }

  def planCallProcedure(inner: LogicalPlan, call: ResolvedCall, reported: ResolvedCall, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(ProcedureCallProjection(reported)))
    annotate(ProcedureCall(inner, call), solved, providedOrders.get(inner.id), context)
  }

  def planPassAll(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(PassthroughAllHorizon()))
    // Keep cardinality, but change solved
    val keptAttributes = Attributes(idGen, cardinalities)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    annotate(newPlan, solved, providedOrders.get(inner.id), context)
  }

  def planLimit(inner: LogicalPlan, count: Expression, ties: Ties = DoNotIncludeTies, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withLimitExpression(count))))
    annotate(LimitPlan(inner, count, ties), solved, providedOrders.get(inner.id), context)
  }

  def planSort(inner: LogicalPlan, sortColumns: Seq[ColumnOrder], reportedSortItems: Seq[ast.SortItem], interestingOrder: InterestingOrder, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSortItems(reportedSortItems))).withInterestingOrder(interestingOrder))
    val providedOrder = ProvidedOrder(sortColumns.map(sortColumnToProvided))
    annotate(Sort(inner, sortColumns), solved, providedOrder, context)
  }

  def planShortestPath(inner: LogicalPlan, shortestPaths: ShortestPathPattern, predicates: Seq[Expression],
                       withFallBack: Boolean, disallowSameNode: Boolean = true, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).amendQueryGraph(_.addShortestPath(shortestPaths).addPredicates(predicates: _*))
    annotate(FindShortestPaths(inner, shortestPaths, predicates, withFallBack, disallowSameNode), solved, providedOrders.get(inner.id), context)
  }

  def planEndpointProjection(inner: LogicalPlan, start: String, startInScope: Boolean, end: String, endInScope: Boolean, patternRel: PatternRelationship, context: LogicalPlanningContext): LogicalPlan = {
    val relTypes = patternRel.types.asNonEmptyOption
    val directed = patternRel.dir != SemanticDirection.BOTH
    val solved = solveds.get(inner.id).amendQueryGraph(_.addPatternRelationship(patternRel))
    annotate(ProjectEndpoints(inner, patternRel.name,
      start, startInScope,
      end, endInScope,
      relTypes, directed, patternRel.length), solved, providedOrders.get(inner.id), context)
  }

  def planUnion(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    annotate(Union(left, right), solveds.get(left.id), ProvidedOrder.empty, context)
    /* TODO: This is not correct in any way: solveds.get(left.id)
     LogicalPlan.solved contains a PlannerQuery, but to represent a Union, we'd need a UnionQuery instead
     Not very important at the moment, but dirty.
     */
  }

  def planDistinctStar(left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val returnAll = QueryProjection.forIds(left.availableSymbols) map {
      case AliasedReturnItem(e, Variable(key)) => key -> e // This smells awful.
    }

    annotate(Distinct(left, returnAll.toMap), solveds.get(left.id), providedOrders.get(left.id), context)
  }

  def planDistinct(left: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = solveds.get(left.id).updateTailOrSelf(_.updateQueryProjection(_ => DistinctQueryProjection(reported)))
    val columnsWithRenames = renameProvidedOrderColumns(providedOrders.get(left.id).columns, expressions)
    val providedOrder = ProvidedOrder(columnsWithRenames)
    annotate(Distinct(left, expressions), solved, providedOrder, context)
  }

  def updateSolvedForOr(orPlan: LogicalPlan, orPredicate: Ors, predicates: Set[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(orPlan.id).updateTailOrSelf { that =>

      /*
        * We want to report all solved predicates, so we have kept track of what each subplan reports to solve.
        * There is no need to report the predicates that are inside the OR (exprs),
        * since we will add the OR itself instead.
        */
      val newSelections = Selections.from((predicates -- orPredicate.exprs + orPredicate).toSeq)
      that.amendQueryGraph(qg => qg.withSelections(newSelections))
    }
    val cardinality = context.cardinality.apply(solved, context.input, context.semanticTable)
    val providedOrder = providedOrders.get(orPlan.id)
    // Change solved and cardinality
    val keptAttributes = Attributes(idGen)
    val newPlan = orPlan.copyPlanWithIdGen(keptAttributes.copy(orPlan.id))
    solveds.set(newPlan.id, solved)
    cardinalities.set(newPlan.id, cardinality)
    providedOrders.set(newPlan.id, providedOrder)
    newPlan
  }

  def planTriadicSelection(positivePredicate: Boolean,
                           left: LogicalPlan,
                           sourceId: String,
                           seenId: String,
                           targetId: String,
                           right: LogicalPlan,
                           predicate: Expression,
                           context: LogicalPlanningContext): LogicalPlan = {
    val solved = (solveds.get(left.id) ++ solveds.get(right.id)).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
    annotate(TriadicSelection(left, right, positivePredicate, sourceId, seenId, targetId), solved, providedOrders.get(left.id), context)
  }

  def planCreate(inner: LogicalPlan, pattern: CreatePattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(plans.Create(inner, pattern.nodes, pattern.relationships), solved, providedOrders.get(inner.id), context)
  }

  def planMergeCreateNode(inner: LogicalPlan, pattern: CreateNode, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(CreatePattern(List(pattern), Nil)))

    annotate(MergeCreateNode(inner, pattern.idName, pattern.labels, pattern.properties), solved, providedOrders.get(inner.id), context)
  }

  def planMergeCreateRelationship(inner: LogicalPlan, pattern: CreateRelationship, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(CreatePattern(Nil, List(pattern))))

    annotate(MergeCreateRelationship(inner, pattern.idName, pattern.startNode, pattern.relType,
      pattern.endNode, pattern.properties), solved, providedOrders.get(inner.id), context)
  }

  def planConditionalApply(lhs: LogicalPlan, rhs: LogicalPlan, idNames: Seq[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(lhs.id) ++ solveds.get(rhs.id)
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(lhs.id)
    annotate(ConditionalApply(lhs, rhs, idNames), solved, providedOrder, context)
  }


  def planAntiConditionalApply(inner: LogicalPlan, outer: LogicalPlan, idNames: Seq[String], context: LogicalPlanningContext, maybeSolved: Option[PlannerQuery] = None): LogicalPlan = {
    val solved = maybeSolved.getOrElse(solveds.get(inner.id) ++ solveds.get(outer.id))
    // If the LHS has duplicate values, we cannot guarantee any added order from the RHS
    val providedOrder = providedOrders.get(inner.id)
    annotate(AntiConditionalApply(inner, outer, idNames), solved, providedOrder, context)
  }

  def planDeleteNode(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced)
      annotate(DetachDeleteNode(inner, delete.expression), solved, providedOrders.get(inner.id), context)
    else
      annotate(DeleteNode(inner, delete.expression), solved, providedOrders.get(inner.id), context)
  }

  def planDeleteRelationship(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    annotate(DeleteRelationship(inner, delete.expression), solved, providedOrders.get(inner.id), context)
  }

  def planDeletePath(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced)
      annotate(DetachDeletePath(inner, delete.expression), solved, providedOrders.get(inner.id), context)
    else
      annotate(DeletePath(inner, delete.expression), solved, providedOrders.get(inner.id), context)
  }

  def planDeleteExpression(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced)
      annotate(DetachDeleteExpression(inner, delete.expression), solved, providedOrders.get(inner.id), context)
    else
      annotate(DeleteExpressionPlan(inner, delete.expression), solved, providedOrders.get(inner.id), context)
  }

  def planSetLabel(inner: LogicalPlan, pattern: SetLabelPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetLabels(inner, pattern.idName, pattern.labels), solved, providedOrders.get(inner.id), context)
  }

  def planSetNodeProperty(inner: LogicalPlan, patternToPlan: SetNodePropertyPattern, solvedPattern: SetNodePropertyPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(solvedPattern))

    annotate(SetNodeProperty(inner, patternToPlan.idName, patternToPlan.propertyKey, patternToPlan.expression), solved, providedOrders.get(inner.id), context)
  }

  def planSetNodePropertiesFromMap(inner: LogicalPlan,
                                   patternToPlan: SetNodePropertiesFromMapPattern, solvedPattern: SetNodePropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(solvedPattern))

    annotate(SetNodePropertiesFromMap(inner, patternToPlan.idName, patternToPlan.expression, patternToPlan.removeOtherProps), solved, providedOrders.get(inner.id), context)
  }

  def planSetRelationshipProperty(inner: LogicalPlan, patternToPlan: SetRelationshipPropertyPattern, solvedPattern: SetRelationshipPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(solvedPattern))

    annotate(SetRelationshipProperty(inner, patternToPlan.idName, patternToPlan.propertyKey, patternToPlan.expression), solved, providedOrders.get(inner.id), context)
  }

  def planSetRelationshipPropertiesFromMap(inner: LogicalPlan,
                                           patternToPlan: SetRelationshipPropertiesFromMapPattern, solvedPattern: SetRelationshipPropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(solvedPattern))

    annotate(SetRelationshipPropertiesFromMap(inner, patternToPlan.idName, patternToPlan.expression, patternToPlan.removeOtherProps), solved, providedOrders.get(inner.id), context)
  }

  def planSetPropertiesFromMap(inner: LogicalPlan, patternToPlan: SetPropertiesFromMapPattern, solvedPattern: SetPropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(solvedPattern))

    annotate(SetPropertiesFromMap(inner, patternToPlan.entityExpression, patternToPlan.expression, patternToPlan.removeOtherProps), solved, providedOrders.get(inner.id), context)
  }

  def planSetProperty(inner: LogicalPlan, patternToPlan: SetPropertyPattern, solvedPattern: SetPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(solvedPattern))

    annotate(SetProperty(inner, patternToPlan.entityExpression, patternToPlan.propertyKeyName, patternToPlan.expression), solved, providedOrders.get(inner.id), context)
  }

  def planRemoveLabel(inner: LogicalPlan, pattern: RemoveLabelPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(RemoveLabels(inner, pattern.idName, pattern.labels), solved, providedOrders.get(inner.id), context)
  }

  def planForeachApply(left: LogicalPlan, innerUpdates: LogicalPlan, pattern: ForeachPattern, context: LogicalPlanningContext, expression: Expression): LogicalPlan = {
    val solved = solveds.get(left.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(ForeachApply(left, innerUpdates, pattern.variable, expression), solved, providedOrders.get(left.id), context)
  }

  def planEager(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(Eager(inner), solveds.get(inner.id), providedOrders.get(inner.id), context)

  def planError(inner: LogicalPlan, exception: ExhaustiveShortestPathForbiddenException, context: LogicalPlanningContext): LogicalPlan =
    annotate(ErrorPlan(inner, exception), solveds.get(inner.id), providedOrders.get(inner.id), context)

  def planProduceResult(inner: LogicalPlan, columns: Seq[String], context: LogicalPlanningContext): LogicalPlan = {
    val produceResult = ProduceResult(inner, columns)
    solveds.copy(inner.id, produceResult.id)
    // Do not calculate cardinality for ProduceResult. Since the passed context does not have accurate label information
    // It will get a wrong value with some projections. Use the cardinality of inner instead
    cardinalities.copy(inner.id, produceResult.id)
    providedOrders.copy(inner.id, produceResult.id)
    produceResult
  }

  /**
    * Compute cardinality for a plan. Set this cardinality in the Cardinalities attribute.
    * Set the other attributes with the provided arguments (solved and providedOrder).
    *
    * @return the same plan
    */
  private def annotate(plan: LogicalPlan, solved: PlannerQuery, providedOrder: ProvidedOrder, context: LogicalPlanningContext) = {
    assertNoBadExpressionsExists(plan)
    val cardinality = cardinalityModel(solved, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    providedOrders.set(plan.id, providedOrder)
    plan
  }

  /**
    * There probably exists some type level way of achieving this with type safety instead of manually searching through the expression tree like this
    */
  private def assertNoBadExpressionsExists(root: Any): Unit = {
    AssertionRunner.runUnderAssertion(new Thunk {
      override def apply(): Unit = new FoldableAny(root).treeExists {
        case _: PatternComprehension | _: PatternExpression | _: MapProjection =>
          throw new InternalException(s"This expression should not be added to a logical plan:\n$root")
        case _ =>
          false
      }
    })
  }

  private def projectedDirection(pattern: PatternRelationship, from: String, dir: SemanticDirection): SemanticDirection = {
    if (dir == SemanticDirection.BOTH) {
      if (from == pattern.left)
        SemanticDirection.OUTGOING
      else
        SemanticDirection.INCOMING
    }
    else
      pattern.dir
  }

  /**
    * The provided order is used to describe the current ordering of the LogicalPlan within a complete plan tree. For
    * index leaf operators this can be planned as an IndexOrder for the index to provide. In that case it only works
    * if all columns are sorted in the same direction, so we need to narrow the scope for these index operations.
    */
  private def toIndexOrder(providedOrder: ProvidedOrder): IndexOrder = providedOrder match {
    case ProvidedOrder.empty => IndexOrderNone
    case ProvidedOrder(columns) if columns.forall(c => c.isAscending) => IndexOrderAscending
    case ProvidedOrder(columns) if columns.forall(c => !c.isAscending) => IndexOrderDescending
    case _ => throw new IllegalStateException("Cannot mix ascending and descending columns when using index order")
  }

  private def sortColumnToProvided(columnOrder: ColumnOrder): ProvidedOrder.Column =
    columnOrder match {
      case Ascending(id) => ProvidedOrder.Asc(id)
      case Descending(id) => ProvidedOrder.Desc(id)
    }

  /**
    * Rename sort columns if they are renamed in a projection.
    */
  private def renameProvidedOrderColumns(columns: Seq[ProvidedOrder.Column], projectExpressions: Map[String, Expression]): Seq[ProvidedOrder.Column] = {
    columns.map {
      case columnOrder@ProvidedOrder.ColumnOfProperty((varName, propName)) =>
        projectExpressions.collectFirst {
          case (newName, Property(Variable(`varName`), PropertyKeyName(`propName`))) => ProvidedOrder.Column(newName, columnOrder.isAscending)
          case (newName, CachedNodeProperty(`varName`, PropertyKeyName(`propName`))) => ProvidedOrder.Column(newName, columnOrder.isAscending)
          case (newName, Variable(`varName`)) => ProvidedOrder.Column(newName + "." + propName, columnOrder.isAscending)
        }.getOrElse(columnOrder)
      case columnOrder@ProvidedOrder.Column(varName) =>
        projectExpressions.collectFirst {
          case (newName, Variable(`varName`)) => ProvidedOrder.Column(newName, columnOrder.isAscending)
        }.getOrElse(columnOrder)
    }
  }
}
