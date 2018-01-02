/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_3.commands.QueryExpression
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{LazyLabel, SortDescription}
import org.neo4j.cypher.internal.compiler.v2_3.planner._
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.LogicalPlanningContext
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.Metrics.CardinalityModel
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{Limit => LimitPlan, Skip => SkipPlan, _}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, SemanticDirection, ast, symbols}
import org.neo4j.function
import org.neo4j.graphdb.Relationship

case class LogicalPlanProducer(cardinalityModel: CardinalityModel) extends CollectionSupport {
  def solvePredicate(plan: LogicalPlan, solved: Expression)(implicit context: LogicalPlanningContext) =
    plan.updateSolved(_.updateGraph(_.addPredicates(solved)))

  def planAggregation(left: LogicalPlan, grouping: Map[String, Expression], aggregation: Map[String, Expression])
                     (implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateTailOrSelf(_.withHorizon(
      AggregatingQueryProjection(groupingKeys = grouping, aggregationExpressions = aggregation)
    ))
    Aggregation(left, grouping, aggregation)(solved)
  }

  def planAllNodesScan(idName: IdName, argumentIds: Set[IdName])(implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph(argumentIds = argumentIds, patternNodes = Set(idName)))
    AllNodesScan(idName, argumentIds)(solved)
  }

  def planApply(left: LogicalPlan, right: LogicalPlan)(implicit context: LogicalPlanningContext) = {
    // We don't want to keep the arguments that this Apply is inserting on the RHS, so we remove them here.
    val rhsSolved: PlannerQuery = right.solved.updateTailOrSelf(_.updateGraph(_.withArgumentIds(Set.empty)))
    val lhsSolved: PlannerQuery = left.solved
    val solved: PlannerQuery = lhsSolved ++ rhsSolved
    Apply(left, right)(solved = solved)
  }

  def planTailApply(left: LogicalPlan, right: LogicalPlan)(implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateTailOrSelf(_.withTail(right.solved))
    Apply(left, right)(solved = solved)
  }

  def planCartesianProduct(left: LogicalPlan, right: LogicalPlan)(implicit context: LogicalPlanningContext) = {
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
                                      (implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toSeq)
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
                                        (implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternRelationship(pattern)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toSeq)
    )
    UndirectedRelationshipByIdSeek(idName, relIds, leftNode, rightNode, argumentIds)(solved)
  }

  def planSimpleExpand(left: LogicalPlan,
                       from: IdName,
                       dir: SemanticDirection,
                       to: IdName,
                       pattern: PatternRelationship,
                       mode: ExpansionMode)(implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateGraph(_.addPatternRelationship(pattern))
    Expand(left, from, dir, pattern.types, to, pattern.name, mode)(solved)
  }

  def planVarExpand(left: LogicalPlan,
                    from: IdName,
                    dir: SemanticDirection,
                    to: IdName,
                    pattern: PatternRelationship,
                    predicates: Seq[(Identifier, Expression)],
                    allPredicates: Seq[Expression],
                    mode: ExpansionMode)(implicit context: LogicalPlanningContext) = pattern.length match {
    case l: VarPatternLength =>
      val projectedDir = if (dir == SemanticDirection.BOTH) {
        if (from == pattern.left) SemanticDirection.OUTGOING else SemanticDirection.INCOMING
      } else pattern.dir

      val solved = left.solved.updateGraph(_
        .addPatternRelationship(pattern)
        .addPredicates(allPredicates: _*)
      )
      VarExpand(left, from, dir, projectedDir, pattern.types, to, pattern.name, l, mode, predicates)(solved)

    case _ => throw new InternalException("Expected a varlength path to be here")
  }

  def planHiddenSelection(predicates: Seq[Expression], left: LogicalPlan)(implicit context: LogicalPlanningContext) = {
    Selection(predicates, left)(left.solved)
  }

  def planNodeByIdSeek(idName: IdName, nodeIds: SeekableArgs,
                       solvedPredicates: Seq[Expression] = Seq.empty,
                       argumentIds: Set[IdName])(implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addArgumentIds(argumentIds.toSeq)
    )
    NodeByIdSeek(idName, nodeIds, argumentIds)(solved)
  }

  def planNodeByLabelScan(idName: IdName, label: LazyLabel, solvedPredicates: Seq[Expression],
                          solvedHint: Option[UsingScanHint] = None, argumentIds: Set[IdName])
                         (implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toSeq)
    )
    NodeByLabelScan(idName, label, argumentIds)(solved)
  }

  def planNodeIndexSeek(idName: IdName,
                        label: ast.LabelToken,
                        propertyKey: ast.PropertyKeyToken,
                        valueExpr: QueryExpression[Expression],
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[IdName])(implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toSeq)
    )
    NodeIndexSeek(idName, label, propertyKey, valueExpr, argumentIds)(solved)
  }

  def planNodeIndexScan(idName: IdName,
                        label: ast.LabelToken,
                        propertyKey: ast.PropertyKeyToken,
                        solvedPredicates: Seq[Expression] = Seq.empty,
                        solvedHint: Option[UsingIndexHint] = None,
                        argumentIds: Set[IdName])(implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toSeq)
    )
    NodeIndexScan(idName, label, propertyKey, argumentIds)(solved)
  }

  def planLegacyHintSeek(idName: IdName, hint: LegacyIndexHint, argumentIds: Set[IdName])
                        (implicit context: LogicalPlanningContext) = {
    val patternNode = hint match {
      case n: NodeHint => Seq(IdName(n.identifier.name))
      case _ => Seq.empty
    }
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternNodes(patternNode: _*)
      .addHints(Some(hint))
      .addArgumentIds(argumentIds.toSeq)
    )
    LegacyIndexSeek(idName, hint, argumentIds)(solved)
  }

  def planNodeHashJoin(nodes: Set[IdName], left: LogicalPlan, right: LogicalPlan, hints: Set[UsingJoinHint])
                      (implicit context: LogicalPlanningContext) = {

    val plannerQuery = left.solved ++ right.solved
    val solved = plannerQuery.updateGraph(_.addHints(hints))
    NodeHashJoin(nodes, left, right)(solved)
  }

  def planNodeUniqueIndexSeek(idName: IdName,
                              label: ast.LabelToken,
                              propertyKey: ast.PropertyKeyToken,
                              valueExpr: QueryExpression[Expression],
                              solvedPredicates: Seq[Expression] = Seq.empty,
                              solvedHint: Option[UsingIndexHint] = None,
                              argumentIds: Set[IdName])(implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .addPatternNodes(idName)
      .addPredicates(solvedPredicates: _*)
      .addHints(solvedHint)
      .addArgumentIds(argumentIds.toSeq)
    )
    NodeUniqueIndexSeek(idName, label, propertyKey, valueExpr, argumentIds)(solved)
  }

  def planOptionalExpand(left: LogicalPlan,
                         from: IdName,
                         dir: SemanticDirection,
                         to: IdName,
                         pattern: PatternRelationship,
                         mode: ExpansionMode = ExpandAll,
                         predicates: Seq[Expression] = Seq.empty,
                         solvedQueryGraph: QueryGraph)(implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateGraph(_.withAddedOptionalMatch(solvedQueryGraph))
    OptionalExpand(left, from, dir, pattern.types, to, pattern.name, mode, predicates)(solved)
  }

  def planOptional(inputPlan: LogicalPlan, ids: Set[IdName])(implicit context: LogicalPlanningContext) = {
    val solved = PlannerQuery(graph = QueryGraph.empty
      .withAddedOptionalMatch(inputPlan.solved.graph)
      .withArgumentIds(ids)
    )
    Optional(inputPlan)(solved)
  }

  def planOuterHashJoin(nodes: Set[IdName], left: LogicalPlan, right: LogicalPlan)
                       (implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateGraph(_.withAddedOptionalMatch(right.solved.graph))
    OuterHashJoin(nodes, left, right)(solved)
  }

  def planSelection(predicates: Seq[Expression], left: LogicalPlan)(implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(predicates: _*)))
    Selection(predicates, left)(solved)
  }

  def planSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression)
                               (implicit context: LogicalPlanningContext) =
    SelectOrAntiSemiApply(outer, inner, expr)(outer.solved)

  def planLetSelectOrAntiSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: IdName, expr: Expression)
                                  (implicit context: LogicalPlanningContext) =
    LetSelectOrAntiSemiApply(outer, inner, id, expr)(outer.solved)

  def planSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, expr: Expression)
                           (implicit context: LogicalPlanningContext) =
    SelectOrSemiApply(outer, inner, expr)(outer.solved)

  def planLetSelectOrSemiApply(outer: LogicalPlan, inner: LogicalPlan, id: IdName, expr: Expression)
                              (implicit context: LogicalPlanningContext) =
    LetSelectOrSemiApply(outer, inner, id, expr)(outer.solved)

  def planLetAntiSemiApply(left: LogicalPlan, right: LogicalPlan, id: IdName)
                          (implicit context: LogicalPlanningContext) =
    LetAntiSemiApply(left, right, id)(left.solved)

  def planLetSemiApply(left: LogicalPlan, right: LogicalPlan, id: IdName)
                      (implicit context: LogicalPlanningContext) =
    LetSemiApply(left, right, id)(left.solved)

  def planAntiSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: PatternExpression, expr: Expression)
                       (implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(expr)))
    AntiSemiApply(left, right)(solved)
  }

  def planSemiApply(left: LogicalPlan, right: LogicalPlan, predicate: Expression)
                   (implicit context: LogicalPlanningContext) = {
    val solved = left.solved.updateTailOrSelf(_.updateGraph(_.addPredicates(predicate)))
    SemiApply(left, right)(solved)
  }

  def planQueryArgumentRow(queryGraph: QueryGraph)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val patternNodes = queryGraph.argumentIds intersect queryGraph.patternNodes
    val patternRels = queryGraph.patternRelationships.filter(rel => queryGraph.argumentIds.contains(rel.name))
    val otherIds = queryGraph.argumentIds -- patternNodes
    planArgumentRow(patternNodes, patternRels, otherIds)
  }

  def planArgumentRowFrom(plan: LogicalPlan)(implicit context: LogicalPlanningContext): LogicalPlan = {
    val types: Map[String, CypherType] = plan.availableSymbols.map {
      case n if context.semanticTable.isNode(n.name) => n.name -> symbols.CTNode
      case r if context.semanticTable.isRelationship(r.name) => r.name -> symbols.CTRelationship
      case v => v.name -> symbols.CTAny
    }.toMap
    Argument(plan.availableSymbols)(plan.solved)(types)
  }

  def planArgumentRow(patternNodes: Set[IdName], patternRels: Set[PatternRelationship] = Set.empty, other: Set[IdName] = Set.empty)
                     (implicit context: LogicalPlanningContext): LogicalPlan = {
    val relIds = patternRels.map(_.name)
    val coveredIds = patternNodes ++ relIds ++ other
    val typeInfoSeq = patternNodes.toSeq.map((x: IdName) => x.name -> CTNode) ++
                      relIds.toSeq.map((x: IdName) => x.name -> CTRelationship) ++
                      other.toSeq.map((x: IdName) => x.name -> CTAny)
    val typeInfo = typeInfoSeq.toMap

    val solved = PlannerQuery(graph =
      QueryGraph(
        argumentIds = coveredIds,
        patternNodes = patternNodes,
        patternRelationships = Set.empty
      ))

    Argument(coveredIds)(solved)(typeInfo)
  }

  def planSingleRow()(implicit context: LogicalPlanningContext) =
    SingleRow()(PlannerQuery.empty)

  def planStarProjection(inner: LogicalPlan, expressions: Map[String, Expression])
                        (implicit context: LogicalPlanningContext) =
    inner.updateSolved(_.updateTailOrSelf(_.updateQueryProjection(_.withProjections(expressions))))

  def planRegularProjection(inner: LogicalPlan, expressions: Map[String, Expression])
                           (implicit context: LogicalPlanningContext) = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.withProjections(expressions)))
    Projection(inner, expressions)(solved)
  }

  def planSkip(inner: LogicalPlan, count: Expression)(implicit context: LogicalPlanningContext) = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSkipExpression(count))))
    SkipPlan(inner, count)(solved)
  }

  def planUnwind(inner: LogicalPlan, name: IdName, expression: Expression)(implicit context: LogicalPlanningContext) = {
    val solved = inner.solved.updateTailOrSelf(_.withHorizon(UnwindProjection(name, expression)))
    UnwindCollection(inner, name, expression)(solved)
  }

  def planLimit(inner: LogicalPlan, count: Expression)(implicit context: LogicalPlanningContext) = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withLimitExpression(count))))
    LimitPlan(inner, count)(solved)
  }

  def planSort(inner: LogicalPlan, descriptions: Seq[SortDescription], items: Seq[ast.SortItem])
              (implicit context: LogicalPlanningContext) = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(_.withSortItems(items))))
    Sort(inner, descriptions)(solved)
  }

  def planSortedLimit(inner: LogicalPlan, limit: Expression, items: Seq[ast.SortItem])
                     (implicit context: LogicalPlanningContext) = {
    val solved = inner.solved.updateTailOrSelf(_.updateQueryProjection(_.updateShuffle(
      _.withLimitExpression(limit)
        .withSortItems(items))))
    SortedLimit(inner, limit, items)(solved)
  }

  def planSortedSkipAndLimit(inner: LogicalPlan, skip: Expression, limit: Expression, items: Seq[ast.SortItem])
                            (implicit context: LogicalPlanningContext) = {
    val solvedBySortedLimit = inner.solved.updateTailOrSelf(
      _.updateQueryProjection(_.updateShuffle(_.withSkipExpression(skip)
                                               .withLimitExpression(limit)
                                               .withSortItems(items))
      ))
    val sortedLimit = SortedLimit(inner, ast.Add(limit, skip)(limit.position), items)(solvedBySortedLimit)

    planSkip(sortedLimit, skip)
  }

  def planShortestPaths(inner: LogicalPlan, shortestPaths: ShortestPathPattern, predicates: Seq[Expression])
                       (implicit context: LogicalPlanningContext) = {
    // TODO: Tell the planner that the shortestPath predicates are solved in shortestPath
    val solved = inner.solved.updateGraph(_.addShortestPath(shortestPaths))
    FindShortestPaths(inner, shortestPaths, predicates)(solved)
  }

  def planEndpointProjection(inner: LogicalPlan, start: IdName, startInScope: Boolean, end: IdName, endInScope: Boolean, patternRel: PatternRelationship)
                            (implicit context: LogicalPlanningContext) = {
    val relTypes = patternRel.types.asNonEmptyOption
    val directed = patternRel.dir != SemanticDirection.BOTH
    val solved = inner.solved.updateGraph(_.addPatternRelationship(patternRel))
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
      case AliasedReturnItem(e, Identifier(key)) => key -> e // This smells awful.
    }

    Aggregation(left, returnAll.toMap, Map.empty)(left.solved)
  }

  def planTriadicSelection(positivePredicate: Boolean, left: LogicalPlan, sourceId: IdName, seenId: IdName, targetId: IdName, right: LogicalPlan, predicate: Expression)
                         (implicit context: LogicalPlanningContext): LogicalPlan = {
    val solved = (left.solved ++ right.solved).updateTailOrSelf(_.updateGraph(_.addPredicates(predicate)))
    TriadicSelection(positivePredicate, left, sourceId, seenId, targetId, right)(solved)
  }

  private implicit def estimatePlannerQuery(plannerQuery: PlannerQuery)(implicit context: LogicalPlanningContext): PlannerQuery with CardinalityEstimation = {
    val cardinality = cardinalityModel(plannerQuery, context.input, context.semanticTable)
    CardinalityEstimation.lift(plannerQuery, cardinality)
  }
}
