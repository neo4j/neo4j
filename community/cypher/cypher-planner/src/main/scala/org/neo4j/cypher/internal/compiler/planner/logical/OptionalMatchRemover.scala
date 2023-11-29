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
package org.neo4j.cypher.internal.compiler.planner.logical

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.SeqSupport.RichSeq
import org.neo4j.cypher.internal.compiler.phases.CompilationContains
import org.neo4j.cypher.internal.compiler.phases.LogicalPlanState
import org.neo4j.cypher.internal.compiler.phases.PlannerContext
import org.neo4j.cypher.internal.compiler.planner.logical.steps.QuerySolvableByGetDegree.SetExtractor
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.DistinctQueryProjection
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.Predicate
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryProjection
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.SinglePlannerQuery
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ordering.InterestingOrder
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.topDown

import scala.annotation.tailrec
import scala.collection.immutable.ListSet
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.TailCalls
import scala.util.control.TailCalls.TailRec

/**
 * Remove optional match, if possible.
 *
 * Move pattern parts of the optional match into a WHERE clause in the optional match, if possible.
 */
case object OptionalMatchRemover extends PlannerQueryRewriter with StepSequencer.Step with DefaultPostCondition
    with PlanPipelineTransformerFactory {

  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)

  override def instance(from: LogicalPlanState, context: PlannerContext): Rewriter = {
    topDown(
      rewriter = Rewriter.lift {
        case RegularSinglePlannerQuery(
            graph,
            interestingOrder,
            proj @ AggregatingQueryProjection(distinctExpressions, aggregations, _, _, _),
            tail,
            queryInput
          )
          if noOptionalShortestPathOrQpp(graph) && graph.mutatingPatterns.isEmpty && validAggregations(aggregations) =>
          val projectionDeps: Iterable[LogicalVariable] =
            (distinctExpressions.values ++ aggregations.values).flatMap(_.dependencies)
          rewrite(projectionDeps, graph, interestingOrder, proj, tail, queryInput, from.anonymousVariableNameGenerator)

        case RegularSinglePlannerQuery(
            graph,
            interestingOrder,
            proj @ DistinctQueryProjection(distinctExpressions, _, _, _),
            tail,
            queryInput
          ) if noOptionalShortestPathOrQpp(graph) && graph.mutatingPatterns.isEmpty =>
          val projectionDeps: Iterable[LogicalVariable] = distinctExpressions.values.flatMap(_.dependencies)
          rewrite(projectionDeps, graph, interestingOrder, proj, tail, queryInput, from.anonymousVariableNameGenerator)

        // Remove OPTIONAL MATCH if preceding MATCH solves the exact same query graph e.g.:
        // OPTIONAL MATCH (n)
        // MATCH (n)           -> QueryGraph {Nodes: ['n'], Arguments: ['n']}
        // OPTIONAL MATCH (n)  -> QueryGraph {Nodes: ['n'], Arguments: ['n']}
        case RegularSinglePlannerQuery(qg: QueryGraph, io, h, t, qi)
          if qg.optionalMatches.exists(om => qg.connectedComponents.contains(om)) =>
          val newQg = qg.withOptionalMatches(qg.optionalMatches.filterNot(om => qg.connectedComponents.contains(om)))
          RegularSinglePlannerQuery(queryGraph = newQg, io, h, t, qi)
      },
      cancellation = context.cancellationChecker
    )
  }

  private def rewrite(
    projectionDeps: Iterable[LogicalVariable],
    graph: QueryGraph,
    interestingOrder: InterestingOrder,
    proj: QueryProjection,
    tail: Option[SinglePlannerQuery],
    queryInput: Option[Seq[Variable]],
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): RegularSinglePlannerQuery = {
    val updateDeps = graph.mutatingPatterns.flatMap(_.dependencies.map(_.name))
    val dependencies: Set[String] = projectionDeps.map(_.name).toSet ++ updateDeps

    val optionalMatches = graph.optionalMatches.flatMapWithTail {
      (original: QueryGraph, tail: Seq[QueryGraph]) =>
        // The dependencies on an optional match are:
        val allDeps =
          // dependencies from optional matches listed later in the query
          tail.flatMap(g => g.argumentIds ++ g.selections.variableDependencies.map(_.name)).toSet ++
            // any dependencies from the next horizon
            dependencies --
            // But we don't need to solve variables already present by the non-optional part of the QG
            graph.idsWithoutOptionalMatchesOrUpdates

        val mustInclude = allDeps -- original.argumentIds
        val mustKeep = smallestGraphIncluding(original, mustInclude)

        if (mustKeep.isEmpty)
          // We did not find anything in this OPTIONAL MATCH. Since there are no variable deps from this clause,
          // and it can't change cardinality, it's safe to ignore it
          None
        else {
          val ExtractionResult(predicatesForPatternExpression, predicatesToKeep, elementsToKeep) = {
            // We must keep all variables calculated in the previous step plus all arguments ids.
            val elementsToKeep0 = mustInclude ++ original.argumentIds

            // We must keep all variables connecting the so far elementsToKeep
            val elementsToKeep1 = smallestGraphIncluding(original, elementsToKeep0)

            // Now, if two relationships, that are currently not kept, overlap, unless the overlap is on an elementToKeep, we also need to keep the relationships.
            // Here, we keep the adjacent nodes, and in the next step we add the relationship itself
            val elementsToKeep2 =
              elementsToKeep1 ++ overlappingRels(original.patternRelationships, elementsToKeep1).flatMap(r =>
                Seq(r.left, r.right).map(_.name)
              )

            // We must (again) keep all variables connecting the so far elementsToKeep
            val elementsToKeep3 = smallestGraphIncluding(original, elementsToKeep2)

            extractElementsAndPatterns(original, elementsToKeep3)
          }

          val (patternsToKeep, patternsToFilter) =
            original.patternRelationships.partition(r => elementsToKeep(r.variable.name))
          val patternNodes = original.patternNodes.filter(elementsToKeep.apply)

          val patternPredicates = patternsToFilter.map(toAst(
            elementsToKeep,
            predicatesForPatternExpression,
            _,
            anonymousVariableNameGenerator
          ))

          val newOptionalGraph = original
            .withPatternRelationships(patternsToKeep)
            .withPatternNodes(patternNodes)
            .withSelections(Selections.from(predicatesToKeep) ++ patternPredicates)

          Some(newOptionalGraph)
        }
    }

    val matches = graph.withOptionalMatches(optionalMatches)
    RegularSinglePlannerQuery(matches, interestingOrder, horizon = proj, tail = tail, queryInput = queryInput)
  }

  /**
   * Given a set of relationships of the original query graph and a set of so-far elements to keep, return all relationships that must be kept.
   * These are all so-far kept relationships plus all other relationships that have an overlap, unless the overlap is on an elementToKeep.
   */
  private def overlappingRels(rels: Set[PatternRelationship], elementsToKeep: Set[String]): Set[PatternRelationship] = {
    val (keptRels, notYetKeptRels) = rels.partition(r => elementsToKeep(r.variable.name))
    val alsoKeptRels = notYetKeptRels.filter { rel =>
      val relIds = rel.coveredIds.map(_.name) -- elementsToKeep
      (notYetKeptRels - rel).exists { rel2 =>
        val rel2Ids = rel2.coveredIds.map(_.name) -- elementsToKeep
        relIds.intersect(rel2Ids).nonEmpty
      }
    }
    keptRels ++ alsoKeptRels
  }

  /**
   * @param predicatesForIRExpressions predicates that can get moved into [[ExistsIRExpression]]s..
   *                                   These are currently only `HasLabels`.
   *                                   This is a map from node variable name to the predicates.
   * @param predicatesToKeep           predicate expressions that cannot be moved into [[ExistsIRExpression]]s.
   * @param elementsToKeep             node and relationship variables that cannot be moved into [[ExistsIRExpression]]s.
   */
  case class ExtractionResult(
    predicatesForIRExpressions: Map[String, Expression],
    predicatesToKeep: Set[Expression],
    elementsToKeep: Set[String]
  )

  @tailrec
  private def extractElementsAndPatterns(original: QueryGraph, elementsToKeepInitial: Set[String]): ExtractionResult = {
    val PartitionedPredicates(predicatesForIRExpressions, predicatesToKeep) =
      partitionPredicates(original.selections.predicates, elementsToKeepInitial)

    val variablesNeededForPredicates = predicatesToKeep.flatMap(expression => expression.dependencies.map(_.name))
    val elementsToKeep = smallestGraphIncluding(original, elementsToKeepInitial ++ variablesNeededForPredicates)

    if (elementsToKeep.equals(elementsToKeepInitial)) {
      ExtractionResult(predicatesForIRExpressions, predicatesToKeep, elementsToKeep)
    } else {
      extractElementsAndPatterns(original, elementsToKeep)
    }
  }

  /**
   * @param predicatesForIRExpressions predicates that can get moved into [[ExistsIRExpression]]s..
   *                                   These are currently only `HasLabels`.
   *                                   This is a map from node variable name to the label names.
   * @param predicatesToKeep           predicate expressions that cannot be moved into [[ExistsIRExpression]]s.
   */
  case class PartitionedPredicates(
    predicatesForIRExpressions: Map[String, Expression],
    predicatesToKeep: Set[Expression]
  )

  /**
   * Checks if an Expression is a composition of HasLabels on the same variable.
   */
  def checkLabelExpression(expression: Expression, variable: String): TailRec[Boolean] = {
    expression match {
      case HasLabels(Variable(varName), labels) if variable == varName =>
        require(labels.size == 1) // We know there is only a single label here because AST rewriting
        TailCalls.done(true)

      case Ands(predicates) => predicates.toSeq.forallTailRec(checkLabelExpression(_, variable))
      case Ors(predicates)  => predicates.toSeq.forallTailRec(checkLabelExpression(_, variable))
      case Not(predicate) =>
        TailCalls.tailcall(checkLabelExpression(predicate, variable))

      case _ => TailCalls.done(false)
    }
  }

  /**
   * This method extracts predicates that need to be part of [[ExistsIRExpression]]s.
   *
   * @param predicates All the original predicates of the QueryGraph
   * @param kept       Set of all variables that should not be moved to [[ExistsIRExpression]]s.
   * @return Map of label predicates to move to [[ExistsIRExpression]]s,
   *         and the set of remaining predicates.
   */
  private def partitionPredicates(predicates: Set[Predicate], kept: Set[String]): PartitionedPredicates = {

    val predicatesForIRExpressions = mutable.Map.empty[String, Expression]
    val predicatesToKeep = mutable.Set.empty[Expression]

    def addLabel(idName: String, newLabelExpression: Expression): Unit = {
      val current = predicatesForIRExpressions.get(idName)
      val labelExpression = current match {
        case None =>
          newLabelExpression
        case Some(labelExpression) =>
          Ands(ListSet(labelExpression, newLabelExpression))(InputPosition.NONE)
      }
      predicatesForIRExpressions += idName -> labelExpression
    }

    predicates.foreach {
      case Predicate(deps, predicate)
        if deps.size == 1 && !kept(deps.head.name) && checkLabelExpression(predicate, deps.head.name).result =>
        addLabel(deps.head.name, predicate)

      case Predicate(_, expr) =>
        predicatesToKeep += expr
    }

    PartitionedPredicates(predicatesForIRExpressions.toMap, predicatesToKeep.toSet)
  }

  private def validAggregations(aggregations: Map[String, Expression]): Boolean = {
    aggregations.isEmpty ||
    aggregations.values.forall {
      case func: FunctionInvocation => func.distinct
      case _                        => false
    }
  }

  private def noOptionalShortestPathOrQpp(qg: QueryGraph): Boolean = {
    qg.optionalMatches.forall(qg =>
      qg.shortestRelationshipPatterns.isEmpty && qg.quantifiedPathPatterns.isEmpty
    )
  }

  private def toAst(
    elementsToKeep: Set[String],
    predicates: Map[String, Expression],
    pattern: PatternRelationship,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Expression = {

    val innerVars = pattern.boundaryNodesSet.map(_.name)
    val innerPreds = innerVars.flatMap(predicates.get)

    val arguments = innerVars.intersect(elementsToKeep)
    val query = RegularSinglePlannerQuery(
      queryGraph = QueryGraph(
        argumentIds = arguments,
        patternNodes = pattern.boundaryNodesSet.map(_.name),
        patternRelationships = Set(pattern),
        selections = Selections.from(innerPreds)
      ),
      horizon = RegularQueryProjection()
    )

    val whereString = innerPreds match {
      case SetExtractor()           => ""
      case SetExtractor(singlePred) => "\n  WHERE " + stringifier(singlePred)
      case _                        => "\n  WHERE " + stringifier(Ands(innerPreds)(InputPosition.NONE))
    }

    ExistsIRExpression(
      query,
      varFor(anonymousVariableNameGenerator.nextName),
      s"EXISTS { MATCH $pattern$whereString }"
    )(
      InputPosition.NONE,
      None, // There is no reasonable way of calculating introduced variables, so IRExpressions should not be accessing it and it can be left blank
      Some(arguments.map(Variable(_)(InputPosition.NONE)))
    )
  }

  implicit class FlatMapWithTailable(in: IndexedSeq[QueryGraph]) {

    def flatMapWithTail(f: (QueryGraph, Seq[QueryGraph]) => IterableOnce[QueryGraph]): IndexedSeq[QueryGraph] = {

      @tailrec
      def recurse(
        that: QueryGraph,
        rest: Seq[QueryGraph],
        builder: mutable.Builder[QueryGraph, ListBuffer[QueryGraph]]
      ): Unit = {
        builder ++= f(that, rest)
        if (rest.nonEmpty)
          recurse(rest.head, rest.tail, builder)
      }
      if (in.isEmpty)
        IndexedSeq.empty
      else {
        val builder = ListBuffer.newBuilder[QueryGraph]
        recurse(in.head, in.tail, builder)
        builder.result().toIndexedSeq
      }
    }
  }

  /**
   * Return all variables in the smallest graph that includes all of `mustInclude`.
   */
  def smallestGraphIncluding(qg: QueryGraph, mustInclude: Set[String]): Set[String] = {
    if (mustInclude.size < 2)
      mustInclude intersect qg.allCoveredIds
    else {
      val mustIncludeRels = qg.patternRelationships.filter(r => mustInclude(r.variable.name))
      val mustIncludeNodes =
        mustInclude.intersect(qg.patternNodes) ++ mustIncludeRels.flatMap(r => Seq(r.left, r.right).map(_.name))
      var accumulatedElements = mustIncludeNodes
      for {
        lhs <- mustIncludeNodes
        rhs <- mustIncludeNodes
        if lhs < rhs
      } {
        accumulatedElements ++= findPathBetween(qg, lhs, rhs)
      }
      accumulatedElements ++ mustInclude
    }
  }

  private case class PathSoFar(end: String, alreadyVisited: Set[PatternRelationship])

  private def hasExpandedInto(from: Seq[PathSoFar], into: Seq[PathSoFar]): Seq[Set[String]] =
    for {
      lhs <- from
      rhs <- into
      if rhs.alreadyVisited.exists(p => p.coveredIds.map(_.name).contains(lhs.end))
    } yield {
      (lhs.alreadyVisited ++ rhs.alreadyVisited).flatMap(_.coveredIds.map(_.name))
    }

  private def expand(queryGraph: QueryGraph, from: Seq[PathSoFar]): Seq[PathSoFar] = {
    from.flatMap {
      case PathSoFar(end, alreadyVisited) =>
        queryGraph.patternRelationships.collect {
          case pr if !alreadyVisited(pr) && pr.coveredIds.map(_.name)(end) =>
            PathSoFar(pr.otherSide(varFor(end)).name, alreadyVisited + pr)
        }
    }
  }

  private def findPathBetween(qg: QueryGraph, startFromL: String, startFromR: String): Set[String] = {
    var l = Seq(PathSoFar(startFromL, Set.empty))
    var r = Seq(PathSoFar(startFromR, Set.empty))

    var i = 0
    while (i <= qg.patternRelationships.size) {
      if (i % 2 == 0) {
        l = expand(qg, l)
        val matches = hasExpandedInto(l, r)
        if (matches.nonEmpty)
          return matches.minBy(_.size)
      } else {
        r = expand(qg, r)
        val matches = hasExpandedInto(r, l)
        if (matches.nonEmpty)
          return matches.minBy(_.size)
      }
      i += 1
    }

    // Did not find any path. Let's do the safe thing and return everything
    qg.patternRelationships.flatMap(_.coveredIds).map(_.name)
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // This works on the IR
    CompilationContains[PlannerQuery]()
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[PlannerContext, LogicalPlanState, LogicalPlanState] = this
}
