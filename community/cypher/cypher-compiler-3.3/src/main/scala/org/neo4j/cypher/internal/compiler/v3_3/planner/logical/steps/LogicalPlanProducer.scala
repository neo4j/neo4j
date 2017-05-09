/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v3_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v3_3.ast.ResolvedCall
import org.neo4j.cypher.internal.compiler.v3_3.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v3_3.helpers.ListSupport
import org.neo4j.cypher.internal.compiler.v3_3.pipes.LazyType
import org.neo4j.cypher.internal.compiler.v3_3.planner._
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.{DeleteExpression => DeleteExpressionPlan, Limit => LimitPlan, LoadCSV => LoadCSVPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.{LogicalPlanningContext, SortDescription}
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.symbols._
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection, ast, _}
import org.neo4j.cypher.internal.ir.v3_3._

/*
 * The responsibility of this class is to produce the correct solved PlannerQuery when creating logical plans.
 * No other functionality or logic should live here - this is supposed to be a very simple class that does not need
 * much testing
 */
case class LogicalPlanProducer(cardinalityModel: CardinalityModel) extends ListSupport {
  def planLock(plan: LogicalPlan, nodesToLock: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan =
    LockNodes(plan, nodesToLock)(plan.solved)

  def solvePredicate(plan: LogicalPlan, solved: Expression)(implicit context: LogicalPlanningContext): LogicalPlan =
    plan.updateSolved(_.amendQueryGraph(_.addPredicates(solved)))

  def planAggregation(left: LogicalPlan,
                      grouping: Map[String, Expression],
                      aggregation: Map[String, Expression],
                      reportedAggregation: Map[String, Expression])
                     (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingKeys = grouping, aggregationExpressions = reportedAggregation)
    ))
    Aggregation(left, grouping, aggregation)(solved)
  }

  def planAllNodesScan(idName: IdName, argumentIds: Set[IdName])
                      (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph(argumentIds = argumentIds, patternNodes = Set(idName)))
    AllNodesScan(idName, argumentIds)(solved)
  }

  def planApply(left: LogicalPlan, right: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved: PlannerQuery = right.solved.updateTailOrSelf(_.amendQueryGraph(_.withArgumentIds(Set.empty)))
    val solved: PlannerQuery = left.solved ++ rhsSolved
    Apply(left, right)(solved = solved)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.updateTailOrSelf(_.withTail(right.solved))
    Apply(left, right)(solved = solved)
  }

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan)
                          (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = left.solved ++ right.solved
    CartesianProduct(left, right)(solved)
  }

  def planDirectedRelationshipByIdSeek(idName: IdName,
                                       relIds: SeekableArgs,
                                       startNode: IdName,
                                       endNode: IdName,
                                       pattern: PatternRelationship,
                                       argumentIds: Set[IdName],
                                       solvedPredicates: Seq[Expression] = Seq.empty)
                                      (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    DirectedRelationshipByIdSeek(idName, relIds, startNode, endNode, argumentIds)(solved)
  }

  def planUndirectedRelationshipByIdSeek(idName: IdName,
                                         relIds: SeekableArgs,
                                         leftNode: IdName,
                                         rightNode: IdName,
                                         pattern: PatternRelationship,
                                         argumentIds: Set[IdName],
                                         solvedPredicates: Seq[Expression] = Seq.empty)
                                        (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    UndirectedRelationshipByIdSeek(idName, relIds, leftNode, rightNode, argumentIds)(solved)
  }

  def planSimpleExpand(left: LogicalPlan,
                       from: IdName,
                       dir: SemanticDirection,
                       to: IdName,
                       pattern: PatternRelationship,
                       mode: ExpansionMode)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.amendQueryGraph(_.addPatternRelationship(pattern))
    Expand(left, from, dir, pattern.types, to, pattern.name, mode)(solved)
  }

  def planVarExpand(left: LogicalPlan,
                    from: IdName,
                    dir: SemanticDirection,
                    to: IdName,
                    pattern: PatternRelationship,
                    predicates: Seq[(Variable, Expression)],
                    allPredicates: Seq[Expression],
                    mode: ExpansionMode)(implicit context: LogicalPlanningContext): LogicalPlan = pattern.length match {
    case l: VarPatternLength =>
      val projectedDir = projectedDirection(pattern, from, dir)

      val solved = left.solved.amendQueryGraph(_
        .addPatternRelationship(pattern)
        .addPredicates(allPredicates: _*)
      )
      VarExpand(left, from, dir, projectedDir, pattern.types, to, pattern.name, l, mode, predicates)(solved)

    case _ => throw new InternalException("Expected a varlength path to be here")
  }

  def planHiddenSelection(predicates: Seq[Expression], left: LogicalPlan)
                         (implicit context: LogicalPlanningContext): LogicalPlan = {
    Selection(predicates, left)(left.solved)
  }

  def planNodeByIdSeek(idName: IdName, nodeIds: SeekableArgs,
                       solvedPredicates: Seq[Expression] = Seq.empty,
                       argumentIds: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    NodeByIdSeek(idName, nodeIds, argumentIds)(solved)
  }

  def planNodeByLabelScan(idName: IdName, label: LabelName, solvedPredicates: Seq[Expression],
                          solvedHint: Option[UsingScanHint] = None, argumentIds: Set[IdName])
                         (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    NodeByLabelScan(idName, label, argumentIds)(solved)
  }

  def planNodeIndexSeek(idName: IdName,
                        label: ast.LabelToken,
                        propertyKeys: Seq[ast.PropertyKeyToken],
                        valueExpr: QueryExpression[Expression],
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    NodeIndexSeek(idName, label, propertyKeys, valueExpr, argumentIds)(solved)
  }

  def planNodeIndexScan(idName: IdName,
                        label: ast.LabelToken,
                        propertyKey: ast.PropertyKeyToken,
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    NodeIndexScan(idName, label, propertyKey, argumentIds)(solved)
  }

  def planNodeIndexContainsScan(idName: IdName,
                                label: ast.LabelToken,
                                propertyKey: ast.PropertyKeyToken,
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    NodeIndexContainsScan(idName, label, propertyKey, valueExpr, argumentIds)(solved)
  }

  def planNodeIndexEndsWithScan(idName: IdName,
                                label: ast.LabelToken,
                                propertyKey: ast.PropertyKeyToken,
                                solvedPredicates: Seq[Expression],
                                solvedHint: Option[UsingIndexHint],
                                valueExpr: Expression,
                                argumentIds: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    NodeIndexEndsWithScan(idName, label, propertyKey, valueExpr, argumentIds)(solved)
  }

  def planLegacyNodeIndexSeek(idName: IdName, hint: LegacyIndexHint, argumentIds: Set[IdName])
                             (implicit context: LogicalPlanningContext): LogicalPlan = {
    val patternNode = hint match {
      case n: NodeHint => Seq(IdName(n.variable.name))
      case _ => Seq.empty
    }
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(patternNode: _*)
      .addHints(Some(hint))
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    LegacyNodeIndexSeek(idName, hint, argumentIds)(solved)
  }

  def planLegacyRelationshipIndexSeek(idName: IdName, hint: LegacyIndexHint, argumentIds: Set[IdName])
                                     (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addHints(Some(hint))
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    LegacyRelationshipIndexSeek(idName, hint, argumentIds)(solved)
  }

  def planNodeHashJoin(nodes: Set[IdName], left: LogicalPlan, right: LogicalPlan, hints: Set[UsingJoinHint])
                      (implicit context: LogicalPlanningContext): LogicalPlan = {

    val plannerQuery = left.solved ++ right.solved
    val solved = plannerQuery.amendQueryGraph(_.addHints(hints))
    NodeHashJoin(nodes, left, right)(solved)
  }

  def planValueHashJoin(left: LogicalPlan, right: LogicalPlan, join: Equals, originalPredicate: Equals)
                       (implicit context: LogicalPlanningContext): LogicalPlan = {
    val plannerQuery = left.solved ++ right.solved
    val solved = plannerQuery.amendQueryGraph(_.addPredicates(originalPredicate))
    ValueHashJoin(left, right, join)(solved)
  }

  def planNodeUniqueIndexSeek(idName: IdName,
                              label: ast.LabelToken,
                              propertyKeys: Seq[ast.PropertyKeyToken],
                              valueExpr: QueryExpression[Expression],
                              solvedPredicates: Seq[Expression] = Seq.empty,
                              solvedHint: Option[UsingIndexHint] = None,
                              argumentIds: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toIndexedSeq)
    )
    NodeUniqueIndexSeek(idName, label, propertyKeys, valueExpr, argumentIds)(solved)
  }

  def planAssertSameNode(node: IdName, left: LogicalPlan, right: LogicalPlan)
                        (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = left.solved ++ right.solved
    AssertSameNode(node, left, right)(solved)
  }

  def planOptionalExpand(left: LogicalPlan,
                         from: IdName,
                         dir: SemanticDirection,
                         to: IdName,
                         pattern: PatternRelationship,
                         mode: ExpansionMode = ExpandAll,
                         predicates: Seq[Expression] = Seq.empty,
                         solvedQueryGraph: QueryGraph)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.amendQueryGraph(_.withAddedOptionalMatch(solvedQueryGraph))
    OptionalExpand(left, from, dir, pattern.types, to, pattern.name, mode, predicates)(solved)
  }

  def planOptional(inputPlan: LogicalPlan, ids: Set[IdName])(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(queryGraph = QueryGraph.empty
      .withAddedOptionalMatch(inputPlan.solved.queryGraph)
      .withArgumentIds(ids)
    )
    Optional(inputPlan, ids)(solved)
  }

  def planOuterHashJoin(nodes: Set[IdName], left: LogicalPlan, right: LogicalPlan)
                       (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.amendQueryGraph(_.withAddedOptionalMatch(right.solved.queryGraph))
    OuterHashJoin(nodes, left, right)(solved)
  }

  def planSelection(left: LogicalPlan, predicates: Seq[Expression], reported: Seq[Expression])
                   (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(reported: _*)))
    Selection(predicates, left)(solved)
  }

  def planSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression)
                               (implicit context: LogicalPlanningContext): LogicalPlan =
    SelectOrAntiSemiApply(outer, inner, expr)(outer.solved)

  def planLetSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: IdName, expr: Expression)
                                  (implicit context: LogicalPlanningContext): LogicalPlan =
    LetSelectOrAntiSemiApply(outer, inner, id, expr)(outer.solved)

  def planSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression)
                           (implicit context: LogicalPlanningContext): LogicalPlan =
    SelectOrSemiApply(outer, inner, expr)(outer.solved)

  def planLetSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: IdName, expr: Expression)
                              (implicit context: LogicalPlanningContext): LogicalPlan =
    LetSelectOrSemiApply(outer, inner, id, expr)(outer.solved)

  def planLetAntiSemiApply(left: LogicalPlan, right: LogicalPlan, id: IdName)
                          (implicit context: LogicalPlanningContext): LogicalPlan =
    LetAntiSemiApply(left, right, id)(left.solved)

  def planLetSemiApply(left: LogicalPlan, right: LogicalPlan, id: IdName)
                      (implicit context: LogicalPlanningContext): LogicalPlan =
    LetSemiApply(left, right, id)(left.solved)

  def planAntiSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: PatternExpression, expr: Expression)
                       (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(expr)))
    AntiSemiApply(left, right)(solved)
  }

  def planSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: Expression)
                   (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = left.solved.updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
    SemiApply(left, right)(solved)
  }

  def planQueryArgumentRow(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels = queryGraph.patternRelationships.filter(rel => queryGraph.argumentIds.contains(rel.name))
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgumentRow(patternNodes, patternRels, otherIds)
  }

  def planArgumentRowFrom(plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan =
    Argument(plan.availableSymbols)(plan.solved)(Map.empty)

  def planArgumentRow(patternNodes: Set[IdName],
                      patternRels: Set[PatternRelationship] = Set.empty,
                      other: Set[IdName] = Set.empty)
                     (implicit context: LogicalPlanningContext): LogicalPlan = {
    val relIds = patternRels.map(_.name)
    val coveredIds = patternNodes ++ relIds ++ other
    val typeInfoSeq = patternNodes.toIndexedSeq.map((x: IdName) => x.name -> CTNode) ++
      relIds.toIndexedSeq.map((x: IdName) => x.name -> CTRelationship) ++
      other.toIndexedSeq.map((x: IdName) => x.name -> CTAny)
    val typeInfo = typeInfoSeq.toMap

    val solved = RegularPlannerQuery(queryGraph =
      QueryGraph(
        argumentIds = coveredIds,
        patternNodes = patternNodes,
        patternRelationships = Set.empty
      ))

    if (coveredIds.isEmpty) SingleRow()(solved) else Argument(coveredIds)(solved)(typeInfo)
  }

  def planSingleRow()(implicit context: LogicalPlanningContext): LogicalPlan =
    SingleRow()(PlannerQuery.empty)

  def planEmptyProjection(inner: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan =
    EmptyResult(inner)(inner.solved)

  def planStarProjection(inner: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression])
                        (implicit context: LogicalPlanningContext): LogicalPlan =
    inner.updateSolved(_.updateTailOrSelf(_.updateQueryProjection(_.withProjections(reported))))

  def planRegularProjection(inner: LogicalPlan, expressions: Map[String, Expression], reported: Map[String, Expression])
                           (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.withProjections(reported)))
    Projection(inner, expressions)(solved)
  }

  def planRollup(lhs: LogicalPlan, rhs: LogicalPlan,
                 collectionName: IdName, variableToCollect: IdName,
                 nullable: Set[IdName]): LogicalPlan = {
    RollUpApply(lhs, rhs, collectionName, variableToCollect, nullable)(lhs.solved)
  }

  def planCountStoreNodeAggregation(query: PlannerQuery, projectedColumn: IdName, label: Option[LabelName], argumentIds: Set[IdName])
                                   (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = RegularPlannerQuery(query.queryGraph, query.horizon)
    NodeCountFromCountStore(projectedColumn, label, argumentIds)(solved)
  }

  def planCountStoreRelationshipAggregation(query: PlannerQuery, idName: IdName, startLabel: Option[LabelName],
                                            typeNames: Seq[RelTypeName], endLabel: Option[LabelName], argumentIds: Set[IdName])
                                           (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved: PlannerQuery = RegularPlannerQuery(query.queryGraph, query.horizon)
    RelationshipCountFromCountStore(idName, startLabel, typeNames, endLabel, argumentIds)(solved)
  }

  def planSkip(inner: LogicalPlan, count: Expression)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSkipExpression(count))))
    SkipPlan(inner, count)(solved)
  }

  def planLoadCSV(inner: LogicalPlan, variableName: IdName, url: Expression, format: CSVFormat, fieldTerminator: Option[StringLiteral])
                 (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.updateTailOrSelf(_.withHorizon(LoadCSVProjection(variableName, url, format, fieldTerminator)))
    LoadCSVPlan(inner, url, variableName, format, fieldTerminator.map(_.value), context.legacyCsvQuoteEscaping)(solved)
  }

  def planUnwind(inner: LogicalPlan, name: IdName, expression: Expression)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.updateTailOrSelf(_.withHorizon(UnwindProjection(name, expression)))
    UnwindCollection(inner, name, expression)(solved)
  }

  def planCallProcedure(inner: LogicalPlan, call: ResolvedCall)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.updateTailOrSelf(_.withHorizon(ProcedureCallProjection(call)))
    ProcedureCall(inner, call)(solved)
  }

  def planPassAll(inner: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.updateTailOrSelf(_.withHorizon(PassthroughAllHorizon()))
    inner.updateSolved(solved)
  }

  def planLimit(inner: LogicalPlan, count: Expression, ties: Ties = DoNotIncludeTies)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withLimitExpression(count))))
    LimitPlan(inner, count, ties)(solved)
  }

  def planSort(inner: LogicalPlan, descriptions: Seq[SortDescription], items: Seq[ast.SortItem])
              (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSortItems(items))))
    Sort(inner, descriptions)(solved)
  }

  def planShortestPath(inner: LogicalPlan, shortestPaths: ShortestPathPattern, predicates: Seq[Expression],
                       withFallBack: Boolean, disallowSameNode: Boolean = true)
                      (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.amendQueryGraph(_.addShortestPath(shortestPaths).addPredicates(predicates: _*))
    FindShortestPaths(inner, shortestPaths, predicates, withFallBack, disallowSameNode)(solved)
  }

  def planEndpointProjection(inner: LogicalPlan, start: IdName, startInScope: Boolean, end: IdName, endInScope: Boolean, patternRel: PatternRelationship)
                            (implicit context: LogicalPlanningContext): LogicalPlan = {
    val relTypes = patternRel.types.asNonEmptyOption
    val directed = patternRel.dir != SemanticDirection.BOTH
    val solved = inner.solved.amendQueryGraph(_.addPatternRelationship(patternRel))
    ProjectEndpoints(inner, patternRel.name,
      start, startInScope,
      end, endInScope,
      relTypes, directed, patternRel.length)(solved)
  }

  def planUnion(left: LogicalPlan, right: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    Union(left, right)(left.solved)
    /* TODO: This is not correct in any way.
     LogicalPlan.solved contains a PlannerQuery, but to represent a Union, we'd need a UnionQuery instead
     Not very important at the moment, but dirty.
     */
  }

  def planDistinct(left: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val returnAll = QueryProjection.forIds(left.availableSymbols) map {
      case AliasedReturnItem(e, Variable(key)) => key -> e // This smells awful.
    }

    Aggregation(left, returnAll.toMap, Map.empty)(left.solved)
  }

  def updateSolvedForOr(orPlan: LogicalPlan, orPredicate: Ors, predicates: Set[Expression])
                       (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = orPlan.solved.updateTailOrSelf { that =>

      /*
        * We want to report all solved predicates, so we have kept track of what each subplan reports to solve.
        * There is no need to report the predicates that are inside the OR (exprs),
        * since we will add the OR itself instead.
        */
      val newSelections = Selections.from((predicates -- orPredicate.exprs + orPredicate).toSeq)
      that.amendQueryGraph(qg => qg.withSelections(newSelections))
    }
    val cardinality = context.cardinality.apply(solved, context.input, context.semanticTable)
    orPlan.updateSolved(CardinalityEstimation.lift(solved, cardinality))
  }

  def planTriadicSelection(positivePredicate: Boolean,
                           left: LogicalPlan,
                           sourceId: IdName,
                           seenId: IdName,
                           targetId: IdName,
                           right: LogicalPlan,
                           predicate: Expression)
                          (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = (left.solved ++ right.solved).updateTailOrSelf(_.amendQueryGraph(_.addPredicates(predicate)))
    TriadicSelection(positivePredicate, left, sourceId, seenId, targetId, right)(solved)
  }

  def planCreateNode(inner: LogicalPlan, pattern: CreateNodePattern)
                    (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    CreateNode(inner, pattern.nodeName, pattern.labels, pattern.properties)(solved)
  }

  def planMergeCreateNode(inner: LogicalPlan, pattern: CreateNodePattern)
                         (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    MergeCreateNode(inner, pattern.nodeName, pattern.labels, pattern.properties)(solved)
  }

  def planCreateRelationship(inner: LogicalPlan, pattern: CreateRelationshipPattern)
                            (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    CreateRelationship(inner, pattern.relName, pattern.startNode, pattern.relType,
      pattern.endNode, pattern.properties)(solved)
  }

  def planMergeCreateRelationship(inner: LogicalPlan, pattern: CreateRelationshipPattern)
                                 (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    MergeCreateRelationship(inner, pattern.relName, pattern.startNode, LazyType(pattern.relType)(context.semanticTable),
      pattern.endNode, pattern.properties)(solved)
  }

  def planConditionalApply(lhs: LogicalPlan, rhs: LogicalPlan, idNames: Seq[IdName])
                          (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = lhs.solved ++ rhs.solved

    ConditionalApply(lhs, rhs, idNames)(solved)
  }

  def planAntiConditionalApply(inner: LogicalPlan, outer: LogicalPlan, idNames: Seq[IdName])
                              (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved ++ outer.solved

    AntiConditionalApply(inner, outer, idNames)(solved)
  }

  def planDeleteNode(inner: LogicalPlan, delete: DeleteExpression)
                    (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced) DetachDeleteNode(inner, delete.expression)(solved)
    else DeleteNode(inner, delete.expression)(solved)
  }

  def planDeleteRelationship(inner: LogicalPlan, delete: DeleteExpression)
                            (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(delete))

    DeleteRelationship(inner, delete.expression)(solved)
  }

  def planDeletePath(inner: LogicalPlan, delete: DeleteExpression)
                    (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced) DetachDeletePath(inner, delete.expression)(solved)
    else DeletePath(inner, delete.expression)(solved)
  }

  def planDeleteExpression(inner: LogicalPlan, delete: DeleteExpression)
                          (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(delete))

    if (delete.forced) DetachDeleteExpression(inner, delete.expression)(solved)
    else DeleteExpressionPlan(inner, delete.expression)(solved)
  }

  def planSetLabel(inner: LogicalPlan, pattern: SetLabelPattern)
                  (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    SetLabels(inner, pattern.idName, pattern.labels)(solved)
  }

  def planSetNodeProperty(inner: LogicalPlan, pattern: SetNodePropertyPattern)
                         (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    SetNodeProperty(inner, pattern.idName, pattern.propertyKey, pattern.expression)(solved)
  }

  def planSetNodePropertiesFromMap(inner: LogicalPlan,
                                   pattern: SetNodePropertiesFromMapPattern)
                                  (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    SetNodePropertiesFromMap(inner, pattern.idName, pattern.expression, pattern.removeOtherProps)(solved)
  }

  def planSetRelationshipProperty(inner: LogicalPlan, pattern: SetRelationshipPropertyPattern)
                                 (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    SetRelationshipPropery(inner, pattern.idName, pattern.propertyKey, pattern.expression)(solved)
  }

  def planSetRelationshipPropertiesFromMap(inner: LogicalPlan,
                                           pattern: SetRelationshipPropertiesFromMapPattern)
                                          (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    SetRelationshipPropertiesFromMap(inner, pattern.idName, pattern.expression, pattern.removeOtherProps)(solved)
  }

  def planSetProperty(inner: LogicalPlan, pattern: SetPropertyPattern)
                     (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    SetProperty(inner, pattern.entityExpression, pattern.propertyKeyName, pattern.expression)(solved)
  }

  def planRemoveLabel(inner: LogicalPlan, pattern: RemoveLabelPattern)
                     (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = inner.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    RemoveLabels(inner, pattern.idName, pattern.labels)(solved)
  }

  def planForeachApply(left: LogicalPlan, innerUpdates: LogicalPlan, pattern: ForeachPattern)
                      (implicit context: LogicalPlanningContext): LogicalPlan = {

    val solved = left.solved.amendQueryGraph(_.addMutatingPatterns(pattern))

    ForeachApply(left, innerUpdates, pattern.variable.name, pattern.expression)(solved)
  }

  def planEager(inner: LogicalPlan): LogicalPlan =     Eager(inner)(inner.solved)

  def planError(inner: LogicalPlan, exception: ExhaustiveShortestPathForbiddenException): LogicalPlan =
    ErrorPlan(inner, exception)(inner.solved)

  implicit def estimatePlannerQuery(plannerQuery: PlannerQuery)
                                   (implicit context: LogicalPlanningContext): PlannerQuery with CardinalityEstimation = {
    val cardinality = cardinalityModel(plannerQuery, context.input, context.semanticTable)
    CardinalityEstimation.lift(plannerQuery, cardinality)
  }

  private def projectedDirection(pattern: PatternRelationship, from: IdName, dir: SemanticDirection): SemanticDirection = {
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
