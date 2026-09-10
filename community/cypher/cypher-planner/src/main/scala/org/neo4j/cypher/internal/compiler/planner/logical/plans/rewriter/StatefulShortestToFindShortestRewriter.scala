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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.compiler.planner.logical.convertToInlinedPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractQppPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractShortestPathPredicates
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NodeUniquenessPredicate
import org.neo4j.cypher.internal.expressions.PathLengthQuantifier
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipUniquenessPredicate
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.VarLengthBound
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern.CountInteger
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.disjoinRelTypesToLabelExpression
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.AllowSameNode
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.planner.spi.PlanningAttributes.Solveds
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.TopDownMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.topDown

import scala.collection.Set

/**
 * Re-writes StatefulShortestPath to FindShortestPath where possible, as benchmarks have shown the latter
 * is faster than StatefulShortestPath.
 *
 * Re-writes are possible when the following are true:
 *
 * Rule 1: Selector is SHORTEST 1 or ALL SHORTEST (or the equivalent)
 *
 * Rule 2: Target node of the pattern is bound
 *
 * Rule 3: Exactly the one selective path pattern owned by this SSP
 * (current solved SPPs minus source solved SPPs contains exactly one SPP).
 *
 * Rule 4: Predicates must be inlineable into FindShortestPath
 *
 * Rule 5: No property access from SPD shard required
 *
 * Rule 6: Exactly one relationship pattern
 *
 * Rule 7: Minimum length 0 or 1
 *
 * Rule 8: Exactly one var-length relationship or one QPP
 *
 * Rule 9: No node group variables, except deterministic directed single-rel QPP groups
 * reconstructible as prefix/suffix slices of the specialized path (undirected excluded).
 */
case class StatefulShortestToFindShortestRewriter(
  solveds: Solveds,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  isShardedDatabase: Boolean = false
) extends Rewriter with TopDownMergeableRewriter {

  override val innerRewriter: Rewriter = Rewriter.lift {
    case RewritableSpp(plan) => plan
  }

  private object RewritableSpp {

    def unapply(plan: AnyRef): Option[FindShortestPaths] = plan match {
      case ssp: StatefulShortestPath if isRewriteCandidate(ssp) =>
        for {
          spp <- singleSelectivePathPatternWithOneRelationship(ssp)
          rewritten <- rewriteVarLengthSpp(spp, ssp) orElse rewriteQppSpp(spp, ssp)
        } yield rewritten
      case _ => None
    }
  }

  // Rule 3: Exactly the one selective path pattern belonging to this SSP.
  // The SSP's solved QueryGraph is built as sourceSolved.amend(addSelectivePathPattern(solvedSpp)),
  // so the pattern owned by this SSP is the set difference between current and source solved SPPs.
  // This is strictly more precise than requiring the cumulative last QueryGraph to contain exactly
  // one SPP: it allows an FSP-compatible SSP stacked on top of other already-solved SPPs to still
  // be specialized, while falling back to StatefulShortestPath whenever the delta is empty,
  // larger than one, or otherwise ambiguous.
  // Rule 6: Exactly one relationship pattern (checked on the owned SPP).
  private def singleSelectivePathPatternWithOneRelationship(ssp: StatefulShortestPath): Option[SelectivePathPattern] = {
    val current = solveds.get(ssp.id).asSinglePlannerQuery.last.queryGraph.selectivePathPatterns
    val source = solveds.get(ssp.source.id).asSinglePlannerQuery.last.queryGraph.selectivePathPatterns
    (current -- source).toSeq match {
      case Seq(spp) if spp.relationships.size == 1 => Some(spp)
      case _                                       => None
    }
  }

  private def isRewriteCandidate(ssp: StatefulShortestPath): Boolean =
    hasBoundTargetEndpoint(ssp) &&
      asksForShortestPathOrAllShortestPaths(ssp)

  // Rule 2: Target node of the pattern is bound
  private def hasBoundTargetEndpoint(ssp: StatefulShortestPath): Boolean =
    ssp.source.availableSymbols.contains(ssp.targetNode)

  // Rule 1: Selector is SHORTEST 1 or ALL SHORTEST (or the equivalent)
  private def asksForShortestPathOrAllShortestPaths(ssp: StatefulShortestPath): Boolean =
    ssp.selector.k == CountInteger(1)

  // Rule 9 (bounded P10 extension): node group variables are allowed ONLY when every
  // required grouping can be reconstructed as a deterministic prefix/suffix slice of the
  // specialized path. For a QPP consisting of one repeated relationship, each traversed
  // edge contributes exactly one left-node, one relationship and one right-node binding,
  // so with path p = (v0,e1,v1,...,ek,vk) and nodes(p) = [v0..vk]:
  //   leftGroup  = nodes(p)[0 .. k-1] = prefix (empty when k=0)
  //   rightGroup = nodes(p)[1 .. k]   = suffix (empty when k=0)
  // There is no repetition-boundary ambiguity because every repetition has exactly one rel.
  // Var-length (non-QPP) patterns have no node groups to reconstruct; they still require
  // empty node groupings.

  private def rewriteVarLengthSpp(
    spp: SelectivePathPattern,
    ssp: StatefulShortestPath
  ): Option[FindShortestPaths] =
    singleVarLengthRelationshipMinLengthOneOrZero(spp.asQueryGraph.patternRelationships.toSeq)
      .flatMap(patternRelationship => sspVarLengthToFindShortest(ssp, spp, patternRelationship))

  // Rule 6: Exactly one relationship pattern
  // Rule 7: Minimum length 0 or 1
  private def singleVarLengthRelationshipMinLengthOneOrZero(
    patternRelationships: Seq[PatternRelationship]
  ): Option[PatternRelationship] = patternRelationships match {
    case Seq(pr) => pr.length match {
        case VarPatternLength(0 | 1, _) => Some(pr)
        case _                          => None
      }
    case _ => None
  }

  private def sspVarLengthToFindShortest(
    ssp: StatefulShortestPath,
    spp: SelectivePathPattern,
    pr: PatternRelationship
  ): Option[FindShortestPaths] = {
    // Var-length patterns have no QPP node-group reconstruction theorem;
    // preserve the existing Rule 9 restriction exactly for this branch.
    if (ssp.nodeVariableGroupings.nonEmpty) return None
    val (fromNode, toNode) = pr.inOrder
    val shortestPathsPatternPart =
      createShortestPathsPatternPart(
        varLengthRange(pr),
        fromNode,
        toNode,
        pr.variable,
        pr.dir,
        pr.types,
        !ssp.selector.isGroup
      )
    val shortestRelPattern = ShortestRelationshipPattern(
      // Non-Interpreted runtimes require path variables to be named up front
      Some(varFor(anonymousVariableNameGenerator.nextName)),
      pr,
      !ssp.selector.isGroup
    )(shortestPathsPatternPart)

    val (nodePredicates, relationshipPredicates, _) = extractPredicates(
      spp.selections.flatPredicates,
      pr.variable,
      fromNode,
      toNode,
      targetNodeIsBound = true,
      // singleVarLengthRelationshipMinLengthOneOrZero has already guaranteed VarPatternLength here
      pr.length.asInstanceOf[VarPatternLength]
    )

    val inlineablePredicates = (nodePredicates ++ relationshipPredicates).map(_.predicate)

    // Rule 4: Predicates must be inlineable into FindShortestPath
    // Rule 5: No property access from SPD shard required
    if (
      allSppPredicatesAreInlineable(spp, inlineablePredicates) &&
      !requiresPropertyAccessFromShards(inlineablePredicates)
    ) {
      Some(FindShortestPaths(
        ssp.source,
        shortestRelPattern,
        nodePredicates.toSeq,
        relationshipPredicates.toSeq,
        Seq.empty,
        withFallBack = false,
        AllowSameNode,
        ssp.pathMode
      )(SameId(ssp.id)))
    } else {
      None
    }
  }

  private def varLengthRange(pr: PatternRelationship): Option[Option[Range]] = {
    val pos = InputPosition.NONE
    pr.length match {
      case SimplePatternLength => None
      case VarPatternLength(min, max) => Some(Some(Range(
          Some(PathLengthQuantifier(min.toString)(pos)),
          max.map(i => PathLengthQuantifier(i.toString)(pos))
        )(pos)))
    }
  }

  private def rewriteQppSpp(
    spp: SelectivePathPattern,
    ssp: StatefulShortestPath
  ): Option[FindShortestPaths] = {
    // Rule 7: Minimum length 0 or 1
    // Rule 8: Exactly one var-length relationship or one QPP
    spp.pathPattern.allQuantifiedPathPatterns.toSeq match {
      case Seq(qpp) if hasSingleRelPatternMinLengthOneOrZero(qpp) => sppQppToFindShortest(ssp, spp, qpp)
      case _                                                      => None
    }
  }

  private def hasSingleRelPatternMinLengthOneOrZero(qpp: QuantifiedPathPattern): Boolean =
    qpp.patternRelationships.size == 1 && qpp.repetition.min < 2

  /**
   * P10 bounded node-group theorem.
   *
   * For a QPP with exactly one PatternRelationship, every repetition contributes exactly
   * one left-node, one relationship and one right-node binding. Therefore with
   * path p = (v0,e1,v1,...,ek,vk), left groups are prefix nodes(p)[0..k-1] and right
   * groups are suffix nodes(p)[1..k] (both empty when k=0). No automaton state is needed
   * for traversal; reconstruction is O(k) output work.
   *
   * Returns Some((left, right)) when every required grouping is reconstructible
   * (including the empty case), None otherwise. The caller must retain SSP on None.
   * Orientation is always query order (left outer -> right outer); the FSP path is
   * materialized in that order, so no reverseGroupVariableProjections handling is needed.
   *
   * Conservative guard: undirected (BOTH) single-rel QPPs with consumed node groups are
   * rejected. Patched differential testing on 2026.08 shows directed OUTGOING/INCOMING
   * reconstruct exactly, while undirected same-node (source==target at runtime) exposes
   * a pre-existing FSP/SSP traversal discrepancy (FSP misses self-loop or throws
   * EntityNotFound for node -1) that already affects group-free undirected rewrites.
   * Since planner cannot prove runtime source!=target for distinct outer variables,
   * P10 must not enlarge that unsafe region; undirected groups stay on SSP.
   */
  private def nodeGroupsForSingleRelQpp(
    ssp: StatefulShortestPath,
    qpp: QuantifiedPathPattern
  ): Option[(Option[LogicalVariable], Option[LogicalVariable])] = {
    if (ssp.nodeVariableGroupings.isEmpty) {
      // No node groups to reconstruct: preserve exact existing behavior for the
      // already-eligible shapes (no additional singleton checks here).
      Some((None, None))
    } else {
      if (qpp.patternRelationships.size != 1) return None
      if (qpp.patternRelationships.head.dir == SemanticDirection.BOTH) return None
      // Conservative: FSP cannot project singleton (non-group) variables. For single-rel
      // QPPs with consumed groups these are empty in practice; reject otherwise rather
      // than silently dropping a required output.
      if (ssp.singletonNodeVariables.nonEmpty || ssp.singletonRelationshipVariables.nonEmpty) return None
      val leftInner = qpp.leftBinding.inner
      val rightInner = qpp.rightBinding.inner
      // Ambiguous when the single edge reuses the same inner variable on both sides;
      // do not guess slice assignment in that degenerate case.
      if (leftInner == rightInner) return None
      var left: Option[LogicalVariable] = None
      var right: Option[LogicalVariable] = None
      val it = ssp.nodeVariableGroupings.iterator
      while (it.hasNext) {
        val g = it.next()
        if (g.singleton == leftInner) {
          if (left.isDefined) return None
          left = Some(g.group)
        } else if (g.singleton == rightInner) {
          if (right.isDefined) return None
          right = Some(g.group)
        } else {
          // Grouping for a singleton that is not one of the two inner nodes of the
          // single-rel pattern (should not happen); retain SSP conservatively.
          return None
        }
      }
      Some((left, right))
    }
  }

  private def sppQppToFindShortest(
    ssp: StatefulShortestPath,
    spp: SelectivePathPattern,
    qpp: QuantifiedPathPattern
  ): Option[FindShortestPaths] = {
    // P10: resolve deterministic node-group outputs before predicate checks so that
    // unsupported group shapes fail fast without affecting existing eligible queries.
    val nodeGroupsOpt = nodeGroupsForSingleRelQpp(ssp, qpp)
    if (nodeGroupsOpt.isEmpty) return None
    val (leftNodeGroup, rightNodeGroup) = nodeGroupsOpt.get
    val qppPredicates = extractedPredicatesFromQpp(ssp, spp, qpp)
    val inlineablePredicates = qppPredicates.allPredicates

    // Rule 4: Predicates must be inlineable into FindShortestPath
    // Rule 5: No property access from SPD shard required
    if (
      allSppPredicatesAreInlineable(spp, inlineablePredicates) &&
      !requiresPropertyAccessFromShards(inlineablePredicates)
    ) {
      val innerRelationshipVariable = qpp.relationshipVariableGroupings.head.singleton
      convertToInlinedPredicates(
        outerStartNode = qpp.leftBinding.outer,
        innerStartNode = qpp.leftBinding.inner,
        innerEndNode = qpp.rightBinding.inner,
        outerEndNode = qpp.rightBinding.outer,
        innerRelationship = innerRelationshipVariable,
        predicatesToInline = qppPredicates.singletonPredicates.toVector,
        mode = convertToInlinedPredicates.Mode.Shortest(
          predicatesOutsideRepetition =
            solveds.get(ssp.source.id).asSinglePlannerQuery.queryGraph.selections.flatPredicates
        ),
        pathDirection = qpp.patternRelationships.head.dir,
        pathRepetition = qpp.repetition,
        anonymousVariableNameGenerator = anonymousVariableNameGenerator
      )
        .map(inlinedPredicates =>
          FindShortestPaths(
            ssp.source,
            shortestRelationshipPattern(ssp, qpp),
            inlinedPredicates.nodePredicates ++ qppPredicates.nodePredicates,
            inlinedPredicates.relationshipPredicates ++ qppPredicates.relationshipPredicates,
            Seq.empty,
            withFallBack = false,
            AllowSameNode,
            ssp.pathMode,
            leftNodeGroup,
            rightNodeGroup
          )(SameId(ssp.id))
        )
    } else {
      None
    }
  }

  // Checks if all predicates in the SPP, minus those that are handled by FindShortestPath itself,
  // are inlineable into FindShortestPath (which will enforce uniqueness and path length requirements)
  private def allSppPredicatesAreInlineable(
    spp: SelectivePathPattern,
    inlineablePredicates: Iterable[Expression]
  ): Boolean =
    withoutPredicatesHandledByFindShortest(spp.selections.flatPredicates).size == inlineablePredicates.size

  private def withoutPredicatesHandledByFindShortest(expressions: Seq[Expression]): Seq[Expression] =
    expressions.filter {
      case far: ForAllRepetitions =>
        far.originalInnerPredicate match {
          case _: RelationshipUniquenessPredicate => false
          case _: NodeUniquenessPredicate         => false
          case _                                  => true
        }
      case _: RelationshipUniquenessPredicate => false
      case _: NodeUniquenessPredicate         => false
      case _: VarLengthBound                  => false
      case _                                  => true
    }

  // Rule 5: No property access from SPD shard required
  private def requiresPropertyAccessFromShards(predicates: Iterable[Expression]): Boolean =
    isShardedDatabase && predicates.exists(requiresPropertyAccess)

  private def requiresPropertyAccess(expr: Expression): Boolean =
    expr.folder.treeExists {
      case _: Property => true
    }

  private case class QppPredicates(
    nodePredicates: extractPredicates.NodePredicates,
    relationshipPredicates: extractPredicates.RelationshipPredicates,
    singletonPredicates: Set[Expression]
  ) {

    def allPredicates: Iterable[Expression] =
      nodePredicates.view.map(_.predicate) ++
        relationshipPredicates.view.map(_.predicate) ++
        singletonPredicates.view

    def size: Int = allPredicates.size
  }

  private def extractedPredicatesFromQpp(
    ssp: StatefulShortestPath,
    spp: SelectivePathPattern,
    qpp: QuantifiedPathPattern
  ): QppPredicates = {
    val (forAllRepetitions, otherPredicates) =
      spp.selections.flatPredicatesSet.partition(_.isInstanceOf[ForAllRepetitions])

    // Gets QPP predicates expressed in terms of singleton variables. These will need to be converted into inlineable
    // predicates later.
    val singletonPredicates = extractQppPredicates(
      forAllRepetitions.toVector,
      qpp.variableGroupings,
      ssp.source.availableSymbols,
      insideRepeat = false,
      repeatStartNode = None // not needed when insideRepeat = false
    ).predicates.map(_.extracted)
      // We drop NodeUniquenessPredicate as that will be enforced by FindShortestPaths.
      // We don't need to do the same for relationships because as we know there is one relationship
      // pattern, and so no RelationshipUniquenessPredicate will have been added.
      .filter {
        case _: NodeUniquenessPredicate => false
        case _                          => true
      }

    // The rest are iterable all(), none(), etc predicates, already in the inlineable form.
    val (nodePredicates, relPredicates, _) = extractShortestPathPredicates(
      otherPredicates,
      qpp.pathVariables.headOption.map(_.variable),
      qpp.relationshipVariableGroupings.headOption.map(_.group)
    )

    QppPredicates(
      nodePredicates = nodePredicates,
      relationshipPredicates = relPredicates,
      singletonPredicates = singletonPredicates.toSet
    )
  }

  // Converts a StatefulShortestPath containing a QuantifiedPathPattern into a ShortestRelationshipPattern.
  // Juxtaposed nodes become boundary nodes, and the relationship uses the group variable name
  // so filters can reference it.
  private def shortestRelationshipPattern(
    ssp: StatefulShortestPath,
    qpp: QuantifiedPathPattern
  ): ShortestRelationshipPattern = {
    val (outerFrom, outerTo) = (qpp.leftBinding.outer, qpp.rightBinding.outer)
    val qppPatternRelationship = qpp.patternRelationships.head
    val varPatternLength = VarPatternLength(qpp.repetition.min.toInt, qpp.repetition.max.limit.map(_.toInt))
    val shortestPathsPatternPart = createShortestPathsPatternPart(
      getRange(qpp),
      outerFrom,
      outerTo,
      qpp.relationshipVariableGroupings.head.group,
      qppPatternRelationship.dir,
      qppPatternRelationship.types,
      !ssp.selector.isGroup
    )
    val updatedQppPatternRelationship = qppPatternRelationship.copy(
      boundaryNodes = (outerFrom, outerTo),
      // Post filters reference variables in the inner pattern of a QPP as group variables
      variable = qpp.relationshipVariableGroupings.head.group,
      length = varPatternLength
    )

    ShortestRelationshipPattern(
      // Non-Interpreted runtimes require path variables to be named up front
      Some(varFor(anonymousVariableNameGenerator.nextName)),
      updatedQppPatternRelationship,
      !ssp.selector.isGroup
    )(shortestPathsPatternPart)
  }

  private def getRange(qpp: QuantifiedPathPattern): Option[Some[Range]] = {
    val pos = InputPosition.NONE
    Some(Some(Range(
      Some(PathLengthQuantifier(qpp.repetition.min.toString)(pos)),
      qpp.repetition.max.limit.map(i => PathLengthQuantifier(i.toString)(pos))
    )(pos)))
  }

  private def createShortestPathsPatternPart(
    length: Option[Option[Range]],
    from: LogicalVariable,
    to: LogicalVariable,
    relationship: LogicalVariable,
    direction: SemanticDirection,
    relTypes: Seq[RelTypeName],
    single: Boolean
  ) = {
    val pos: InputPosition = InputPosition.NONE
    ShortestPathsPatternPart(
      RelationshipChain(
        NodePattern(Some(from), None, None, None)(
          pos
        ), // labels, properties and predicates are not used at runtime
        RelationshipPattern(
          Some(relationship),
          disjoinRelTypesToLabelExpression(relTypes),
          length,
          None, // properties are not used at runtime
          None,
          direction
        )(pos),
        NodePattern(Some(to), None, None, None)(
          pos
        ) // labels, properties and predicates are not used at runtime
      )(pos),
      single = single
    )(pos)
  }

  private val instance: Rewriter = topDown(innerRewriter)

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
