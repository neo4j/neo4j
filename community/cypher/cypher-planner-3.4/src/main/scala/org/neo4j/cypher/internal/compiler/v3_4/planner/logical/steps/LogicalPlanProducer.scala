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
package org.neo4j.cypher.internal.compiler.v3_4.planner.logical.steps

import org.neo4j.cypher.internal.planner.v3_4.spi.PlanningAttributes.{Cardinalities, Solveds}
import org.neo4j.cypher.internal.compiler.v3_4.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.v3_4.planner._
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.frontend.v3_4.ast
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.ir.v3_4._
import org.neo4j.cypher.internal.util.v3_4.attribution.{Attributes, IdGen}
import org.neo4j.cypher.internal.util.v3_4.attribution.IdGen
import org.neo4j.cypher.internal.util.v3_4.{ExhaustiveShortestPathForbiddenException, InternalException}
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.logical.plans.{DeleteExpression => DeleteExpressionPlan, Limit => LimitPlan, LoadCSV => LoadCSVPlan, Skip => SkipPlan, _}

/*
 * The responsibility of this class is to produce the correct solved PlannerQuery when creating logical plans.
 * No other functionality or logic should live here - this is supposed to be a very simple class that does not need
 * much testing
 */
case class LogicalPlanProducer(cardinalityModel: CardinalityModel, solveds: Solveds, cardinalities: Cardinalities, idGen : IdGen) extends ListSupport {

  implicit val implicitIdGen: IdGen = idGen

  def planLock(plan: LogicalPlan, nodesToLock: Set[String], context: LogicalPlanningContext): LogicalPlan =
    annotate(LockNodes(plan, nodesToLock), solveds.get(plan.id), context)

  def solvePredicate(plan: LogicalPlan, solved: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val pq = solveds.get(plan.id).amendQueryGraph(_.addPredicates(solved))
    // Keep cardinality but change solved
    val keptAttributes = Attributes(idGen, cardinalities)
    val newPlan = plan.copyPlanWithIdGen(keptAttributes.copy(plan.id))
    annotate(newPlan, pq, context)
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
    annotate(Aggregation(left, grouping, aggregation), solved, context)
  }

  def planAllNodesScan(idName: String, argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph(argumentIds = argumentIds, patternNodes = Set(idName)))
    annotate(AllNodesScan(idName, argumentIds), solved, context)
  }

  def planApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved: PlannerQuery = solveds.get(right.id).updateTailOrSelf(_.amendQueryGraph(_.withArgumentIds(Set.empty)))
    val solved: PlannerQuery = solveds.get(left.id) ++ rhsSolved
    annotate(Apply(left, right), solved, context)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.withTail(solveds.get(right.id)))
    annotate(Apply(left, right), solved, context)
  }

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    annotate(CartesianProduct(left, right), solved, context)
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
    annotate(DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, argumentIds), solved, context)
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
    annotate(UndirectedRelationshipByIdSeek(idName, relIds, leftNode, rightNode, argumentIds), solved, context)
  }

  def planSimpleExpand(left: LogicalPlan,
                       from: String,
                       dir: SemanticDirection,
                       to: String,
                       pattern: PatternRelationship,
                       mode: ExpansionMode,
                       context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).amendQueryGraph(_.addPatternRelationship(pattern))
    annotate(Expand(left, from, dir, pattern.types, to, pattern.name, mode), solved, context)
  }

  def planVarExpand(source: LogicalPlan,
                    from: String,
                    dir: SemanticDirection,
                    to: String,
                    pattern: PatternRelationship,
                    temporaryNode: String,
                    temporaryEdge: String,
                    edgePredicate: Expression,
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
        tempEdge = temporaryEdge,
        nodePredicate = nodePredicate,
        edgePredicate = edgePredicate,
        legacyPredicates = legacyPredicates), solved, context)

    case _ => throw new InternalException("Expected a varlength path to be here")
  }

  def planHiddenSelection(predicates: Seq[Expression], left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    annotate(Selection(predicates, left), solveds.get(left.id), context)
  }

  def planNodeByIdSeek(idName: String, nodeIds: SeekableArgs,
                       solvedPredicates: Seq[Expression] = Seq.empty,
                       argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeByIdSeek(idName, nodeIds, argumentIds), solved, context)
  }

  def planNodeByLabelScan(idName: String, label: LabelName, solvedPredicates: Seq[Expression],
                          solvedHint: Option[UsingScanHint] = None, argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeByLabelScan(idName, label, argumentIds), solved, context)
  }

  def planNodeIndexSeek(idName: String,
                        label: LabelToken,
                        propertyKeys: Seq[PropertyKeyToken],
                        valueExpr: QueryExpression[Expression],
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedPredicatesForCardinalityEstimation: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[String],
                        context: LogicalPlanningContext): LogicalPlan = {
    val queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    // We know solvedPredicates is a subset of solvedPredicatesForCardinalityEstimation
    val solved = RegularPlannerQuery(queryGraph = queryGraph)
    val solvedForCardinalityEstimation = RegularPlannerQuery(queryGraph.addPredicates(solvedPredicatesForCardinalityEstimation: _*))

    val plan = NodeIndexSeek(idName, label, propertyKeys, valueExpr, argumentIds)
    val cardinality = cardinalityModel(solvedForCardinalityEstimation, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    plan
  }

  def planNodeIndexScan(idName: String,
                        label: LabelToken,
                        propertyKey: PropertyKeyToken,
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[String],
                        context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeIndexScan(idName, label, propertyKey, argumentIds), solved, context)
  }

  def planNodeIndexContainsScan(idName: String,
                                label: LabelToken,
                                propertyKey: PropertyKeyToken,
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[String],
                                context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeIndexContainsScan(idName, label, propertyKey, valueExpr, argumentIds), solved, context)
  }

  def planNodeIndexEndsWithScan(idName: String,
                                label: LabelToken,
                                propertyKey: PropertyKeyToken,
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[String],
                                context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeIndexEndsWithScan(idName, label, propertyKey, valueExpr, argumentIds), solved, context)
  }

  def planNodeHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Seq[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {

    val plannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    val solved = plannerQuery.amendQueryGraph(_.addHints(hints))
    annotate(NodeHashJoin(nodes, left, right), solved, context)
  }

  def planValueHashJoin(left: LogicalPlan, right: LogicalPlan, join: Equals, originalPredicate: Equals, context: LogicalPlanningContext): LogicalPlan = {
    val plannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    val solved = plannerQuery.amendQueryGraph(_.addPredicates(originalPredicate))
    annotate(ValueHashJoin(left, right, join), solved, context)
  }

  def planNodeUniqueIndexSeek(idName: String,
                              label: LabelToken,
                              propertyKeys: Seq[PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              solvedPredicates: Seq[Expression] = Seq.empty,
                              solvedHint: Option[UsingIndexHint] = None,
                              argumentIds: Set[String],
                              context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    annotate(NodeUniqueIndexSeek(idName, label, propertyKeys, valueExpr, argumentIds), solved, context)
  }

  def planAssertSameNode(node: String, left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = solveds.get(left.id) ++ solveds.get(right.id)
    annotate(AssertSameNode(node, left, right), solved, context)
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
    annotate(OptionalExpand(left, from, dir, pattern.types, to, pattern.name, mode, predicates), solved, context)
  }

  def planOptional(inputPlan: LogicalPlan, ids: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .withAddedOptionalMatch(solveds.get(inputPlan.id).queryGraph)
      .withArgumentIds(ids)
    )
    annotate(Optional(inputPlan, ids), solved, context)
  }

  def planActiveRead(inputPlan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inputPlan.id)
    annotate(ActiveRead(inputPlan), solved, context)
  }

  def planLeftOuterHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Seq[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).amendQueryGraph(_.withAddedOptionalMatch(solveds.get(right.id).queryGraph.addHints(hints)))
    annotate(LeftOuterHashJoin(nodes, left, right), solved, context)
  }

  def planRightOuterHashJoin(nodes: Set[String], left: LogicalPlan, right: LogicalPlan, hints: Seq[UsingJoinHint], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(right.id).amendQueryGraph(_.withAddedOptionalMatch(solveds.get(left.id).queryGraph.addHints(hints)))
    annotate(RightOuterHashJoin(nodes, left, right), solved, context)
  }

  def planSelection(left: LogicalPlan, predicates: Seq[Expression], reported: Seq[Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(reported: _*)))
    annotate(Selection(predicates, left), solved, context)
  }

  def planSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(SelectOrAntiSemiApply(outer, inner, expr), solveds.get(outer.id), context)

  def planLetSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: String, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetSelectOrAntiSemiApply(outer, inner, id, expr), solveds.get(outer.id), context)

  def planSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(SelectOrSemiApply(outer, inner, expr), solveds.get(outer.id), context)

  def planLetSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: String, expr: Expression, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetSelectOrSemiApply(outer, inner, id, expr), solveds.get(outer.id), context)

  def planLetAntiSemiApply(left: LogicalPlan, right: LogicalPlan, id: String, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetAntiSemiApply(left, right, id), solveds.get(left.id), context)

  def planLetSemiApply(left: LogicalPlan, right: LogicalPlan, id: String, context: LogicalPlanningContext): LogicalPlan =
    annotate(LetSemiApply(left, right, id), solveds.get(left.id), context)

  def planAntiSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: PatternExpression, expr: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    annotate(AntiSemiApply(left, right), solved, context)
  }

  def planSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(left.id).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
    annotate(SemiApply(left, right), solved, context)
  }

  def planQueryArgument(queryGraph: QueryGraph, context: LogicalPlanningContext): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels = queryGraph.patternRelationships.filter(rel => queryGraph.argumentIds.contains(rel.name))
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgument(patternNodes, patternRels, otherIds, context)
  }

  def planArgumentFrom(plan: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(Argument(plan.availableSymbols), solveds.get(plan.id), context)

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

    annotate(Argument(coveredIds), solved, context)
  }

  def planArgument(context: LogicalPlanningContext): LogicalPlan =
    annotate(Argument(Set.empty), PlannerQuery.empty, context)

  def planEmptyProjection(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(EmptyResult(inner), solveds.get(inner.id), context)

  def planStarProjection(inner: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val newSolved: PlannerQuery = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.withProjections(reported)))
    // Keep cardinality, but change solved
    val keptAttributes = Attributes(idGen, cardinalities)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    annotate(newPlan, newSolved, context)
  }

  def planRegularProjection(inner: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.withProjections(reported)))
    annotate(Projection(inner, expressions), solved, context)
  }

  def planRollup(lhs: LogicalPlan,
                 rhs: LogicalPlan,
                 collectionName: String,
                 variableToCollect: String,
                 nullable: Set[String],
                 context: LogicalPlanningContext): LogicalPlan = {
    annotate(RollUpApply(lhs, rhs, collectionName, variableToCollect, nullable), solveds.get(lhs.id), context)
  }

  def planCountStoreNodeAggregation(query: PlannerQuery, projectedColumn: String, labels: List[Option[LabelName]], argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(query.queryGraph, query.horizon)
    annotate(NodeCountFromCountStore(projectedColumn, labels, argumentIds), solved, context)
  }

  def planCountStoreRelationshipAggregation(query: PlannerQuery, idName: String, startLabel: Option[LabelName],
                                            typeNames: Seq[RelTypeName], endLabel: Option[LabelName], argumentIds: Set[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = RegularPlannerQuery(query.queryGraph, query.horizon)
    annotate(RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, argumentIds), solved, context)
  }

  def planSkip(inner: LogicalPlan, count: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSkipExpression(count))))
    annotate(SkipPlan(inner, count), solved, context)
  }

  def planLoadCSV(inner: LogicalPlan, variableName: String, url: Expression, format: CSVFormat, fieldTerminator: Option[StringLiteral], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(LoadCSVProjection(variableName, url, format, fieldTerminator)))
    annotate(LoadCSVPlan(inner, url, variableName, format, fieldTerminator.map(_.value), context.legacyCsvQuoteEscaping,
                         context.csvBufferSize), solved, context)
  }

  def planUnwind(inner: LogicalPlan, name: String, expression: Expression, reported: Expression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(UnwindProjection(name, reported)))
    annotate(UnwindCollection(inner, name, expression), solved, context)
  }

  def planCallProcedure(inner: LogicalPlan, call: ResolvedCall, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(ProcedureCallProjection(call)))
    annotate(ProcedureCall(inner, call), solved, context)
  }

  def planPassAll(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.withHorizon(PassthroughAllHorizon()))
    // Keep cardinality, but change solved
    val keptAttributes = Attributes(idGen, cardinalities)
    val newPlan = inner.copyPlanWithIdGen(keptAttributes.copy(inner.id))
    annotate(newPlan, solved, context)
  }

  def planLimit(inner: LogicalPlan, count: Expression, ties: Ties = DoNotIncludeTies, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withLimitExpression(count))))
    annotate(LimitPlan(inner, count, ties), solved, context)
  }

  def planSort(inner: LogicalPlan, descriptions: Seq[ColumnOrder], items: Seq[ast.SortItem], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSortItems(items))))
    annotate(Sort(inner, descriptions), solved, context)
  }

  def planShortestPath(inner: LogicalPlan, shortestPaths: ShortestPathPattern, predicates: Seq[Expression],
                       withFallBack: Boolean, disallowSameNode: Boolean = true, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).amendQueryGraph(_.addShortestPath(shortestPaths).addPredicates(predicates: _*))
    annotate(FindShortestPaths(inner, shortestPaths, predicates, withFallBack, disallowSameNode), solved, context)
  }

  def planEndpointProjection(inner: LogicalPlan, start: String, startInScope: Boolean, end: String, endInScope: Boolean, patternRel: PatternRelationship, context: LogicalPlanningContext): LogicalPlan = {
    val relTypes = patternRel.types.asNonEmptyOption
    val directed = patternRel.dir != SemanticDirection.BOTH
    val solved = solveds.get(inner.id).amendQueryGraph(_.addPatternRelationship(patternRel))
    annotate(ProjectEndpoints(inner, patternRel.name,
      start, startInScope,
      end, endInScope,
      relTypes, directed, patternRel.length), solved, context)
  }

  def planUnion(left: LogicalPlan, right: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    annotate(Union(left, right), solveds.get(left.id), context)
    /* TODO: This is not correct in any way.
     LogicalPlan.solved contains a PlannerQuery, but to represent a Union, we'd need a UnionQuery instead
     Not very important at the moment, but dirty.
     */
  }

  def planDistinctStar(left: LogicalPlan, context: LogicalPlanningContext): LogicalPlan = {
    val returnAll = QueryProjection.forIds(left.availableSymbols) map {
      case AliasedReturnItem(e, Variable(key)) => key -> e // This smells awful.
    }

    annotate(Distinct(left, returnAll.toMap), solveds.get(left.id), context)
  }

  def planDistinct(left: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression], context: LogicalPlanningContext): LogicalPlan = {

    val solved: PlannerQuery = solveds.get(left.id).updateTailOrSelf(_.updateQueryProjection(_ => DistinctQueryProjection(reported)))
    annotate(Distinct(left, expressions), solved, context)
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
    // Change solved and cardinality
    val keptAttributes = Attributes(idGen)
    val newPlan = orPlan.copyPlanWithIdGen(keptAttributes.copy(orPlan.id))
    solveds.set(newPlan.id, solved)
    cardinalities.set(newPlan.id, cardinality)
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
    annotate(TriadicSelection(left, right, positivePredicate, sourceId, seenId, targetId), solved, context)
  }

  def planCreateNode(inner: LogicalPlan, pattern: CreateNodePattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(CreateNode(inner, pattern.nodeName, pattern.labels, pattern.properties), solved, context)
  }

  def planMergeCreateNode(inner: LogicalPlan, pattern: CreateNodePattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(MergeCreateNode(inner, pattern.nodeName, pattern.labels, pattern.properties), solved, context)
  }

  def planCreateRelationship(inner: LogicalPlan, pattern: CreateRelationshipPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(CreateRelationship(inner, pattern.relName, pattern.startNode, pattern.relType,
      pattern.endNode, pattern.properties), solved, context)
  }

  def planMergeCreateRelationship(inner: LogicalPlan, pattern: CreateRelationshipPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(MergeCreateRelationship(inner, pattern.relName, pattern.startNode, pattern.relType,
      pattern.endNode, pattern.properties), solved, context)
  }

  def planConditionalApply(lhs: LogicalPlan, rhs: LogicalPlan, idNames: Seq[String], context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(lhs.id) ++ solveds.get(rhs.id)

    annotate(ConditionalApply(lhs, rhs, idNames), solved, context)
  }


  def planAntiConditionalApply(inner: LogicalPlan, outer: LogicalPlan, idNames: Seq[String], context: LogicalPlanningContext, maybeSolved: Option[PlannerQuery] = None): LogicalPlan = {
    val solved = maybeSolved.getOrElse(solveds.get(inner.id) ++ solveds.get(outer.id))

    annotate(AntiConditionalApply(inner, outer, idNames), solved, context)
  }

  def planDeleteNode(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced)
      annotate(DetachDeleteNode(inner, delete.expression), solved, context)
    else
      annotate(DeleteNode(inner, delete.expression), solved, context)
  }

  def planDeleteRelationship(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    annotate(DeleteRelationship(inner, delete.expression), solved, context)
  }

  def planDeletePath(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced)
      annotate(DetachDeletePath(inner, delete.expression), solved, context)
    else
      annotate(DeletePath(inner, delete.expression), solved, context)
  }

  def planDeleteExpression(inner: LogicalPlan, delete: DeleteExpression, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced)
      annotate(DetachDeleteExpression(inner, delete.expression), solved, context)
    else
      annotate(DeleteExpressionPlan(inner, delete.expression), solved, context)
  }

  def planSetLabel(inner: LogicalPlan, pattern: SetLabelPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetLabels(inner, pattern.idName, pattern.labels), solved, context)
  }

  def planSetNodeProperty(inner: LogicalPlan, pattern: SetNodePropertyPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetNodeProperty(inner, pattern.idName, pattern.propertyKey, pattern.expression), solved, context)
  }

  def planSetNodePropertiesFromMap(inner: LogicalPlan,
                                   pattern: SetNodePropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetNodePropertiesFromMap(inner, pattern.idName, pattern.expression, pattern.removeOtherProps), solved, context)
  }

  def planSetRelationshipProperty(inner: LogicalPlan, pattern: SetRelationshipPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetRelationshipPropery(inner, pattern.idName, pattern.propertyKey, pattern.expression), solved, context)
  }

  def planSetRelationshipPropertiesFromMap(inner: LogicalPlan,
                                           pattern: SetRelationshipPropertiesFromMapPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetRelationshipPropertiesFromMap(inner, pattern.idName, pattern.expression, pattern.removeOtherProps), solved, context)
  }

  def planSetProperty(inner: LogicalPlan, pattern: SetPropertyPattern, context: LogicalPlanningContext): LogicalPlan = {
    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(SetProperty(inner, pattern.entityExpression, pattern.propertyKeyName, pattern.expression), solved, context)
  }

  def planRemoveLabel(inner: LogicalPlan, pattern: RemoveLabelPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(inner.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(RemoveLabels(inner, pattern.idName, pattern.labels), solved, context)
  }

  def planForeachApply(left: LogicalPlan, innerUpdates: LogicalPlan, pattern: ForeachPattern, context: LogicalPlanningContext): LogicalPlan = {

    val solved = solveds.get(left.id).amendQueryGraph(_.addMutatingPatterns(pattern))

    annotate(ForeachApply(left, innerUpdates, pattern.variable, pattern.expression), solved, context)
  }

  def planEager(inner: LogicalPlan, context: LogicalPlanningContext): LogicalPlan =
    annotate(Eager(inner), solveds.get(inner.id), context)

  def planError(inner: LogicalPlan, exception: ExhaustiveShortestPathForbiddenException, context: LogicalPlanningContext): LogicalPlan =
    annotate(ErrorPlan(inner, exception), solveds.get(inner.id), context)

  def planProduceResult(inner: LogicalPlan, columns: Seq[String], context: LogicalPlanningContext): LogicalPlan = {
    val produceResult = ProduceResult(inner, columns)
    solveds.copy(inner.id, produceResult.id)
    // Do not calculate cardinality for ProduceResult. Since the passed context does not have accurate label information
    // It will get a wrong value with some projections. Use the cardinality of inner instead
    cardinalities.copy(inner.id, produceResult.id)
    produceResult
  }

  private def annotate(plan: LogicalPlan, solved: PlannerQuery, context: LogicalPlanningContext): LogicalPlan = {
    val cardinality = cardinalityModel(solved, context.input, context.semanticTable)
    solveds.set(plan.id, solved)
    cardinalities.set(plan.id, cardinality)
    plan
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
}
