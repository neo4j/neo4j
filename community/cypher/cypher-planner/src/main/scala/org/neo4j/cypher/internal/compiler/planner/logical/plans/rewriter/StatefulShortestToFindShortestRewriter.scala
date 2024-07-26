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

import org.neo4j.cypher.internal.compiler.planner.logical.SkipRewriteOnZeroRepetitions
import org.neo4j.cypher.internal.compiler.planner.logical.convertToInlinedPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractPredicates.RelationshipPredicates
import org.neo4j.cypher.internal.compiler.planner.logical.idp.extractQPPPredicates
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipUniquenessPredicate
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.VarLengthBound
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.SelectivePathPattern
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
 * This rewriter will sometimes transform a Stateful Shortest Path into a findShortestPath.
 *
 * Through benchmarking it has been found that legacy find shortest path is faster than the new Shortest, we therefor
 * rewrite simple Shortest plans into legacy find shortest where possible.
 * To be able to do this we need to follow certain rules:
 * 1.1 Shortest pattern must be just a single var-length relationship
 *  1.2 or a QPP that is equivalent to a var-length relationship,
 * 2.0 start and end nodes must be bound
 * 3.0 predicates needs to be inlineable in a legacy find shortest plan
 * 4.1 Minimum bound is 0 (otherwise the startNode = endNode is not supported)
 *  4.2 OR relationship is directed.
 * 5.0 Minimum bound must be 0 or 1 for QPP
 * 6.0 Selection asks for shortest 1
 *
 * If a Shortest pattern follows these rules we can rewrite it into a legacy shortest path.
 */
case class StatefulShortestToFindShortestRewriter(
  solveds: Solveds,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator
) extends Rewriter with TopDownMergeableRewriter {

  override val innerRewriter: Rewriter = Rewriter.lift {
    case statefulShortest @ StatefulShortestPath(
        source,
        _,
        targetNode,
        _,
        _,
        _,
        _,
        _,
        _,
        _,
        selector,
        _,
        _,
        _
      )
      // 2.0 start and end nodes are bound and 6.0 selection asks for shortest 1
      if source.availableSymbols.contains(targetNode) &&
        selector.k == 1 && statefulShortest.nodeVariableGroupings.isEmpty =>
      exactlyOne(
        solveds.get(statefulShortest.id).asSinglePlannerQuery.last.queryGraph.selectivePathPatterns.toSeq.distinct
      )
        .filter(_.relationships.size == 1)
        .flatMap(selectivePathPattern =>
          findShortestFromVarLengthShortest(selectivePathPattern, statefulShortest) orElse
            findShortestFromQppShortest(selectivePathPattern, statefulShortest)
        ).getOrElse(statefulShortest)
  }

  private val instance: Rewriter = topDown(innerRewriter)

  private def findShortestFromVarLengthShortest(
    selectivePathPattern: SelectivePathPattern,
    statefulShortestPath: StatefulShortestPath
  ): Option[FindShortestPaths] =
    isVarlengthWithValidLength(selectivePathPattern.asQueryGraph.patternRelationships.toSeq).flatMap(
      patternRelationship =>
        sspVarLengthToFindShortest(statefulShortestPath, selectivePathPattern, patternRelationship)
    )

  /**
   * This method checks if the VarLengthPattern is able to be used in a findShortestPaths.
   * - It can only contain one quantified relationship
   * - It must contain a VarPatternLength with minimum repetition 0 OR be Directed.
   */
  private def isVarlengthWithValidLength(patternRelationships: Seq[PatternRelationship])
    : Option[PatternRelationship] = {
    exactlyOne(patternRelationships).filter(patternRelationship =>
      patternRelationship.length match {
        case VarPatternLength(min, _) =>
          min == 0 || //    4.1 Minimum bound is 0
          patternRelationship.dir != BOTH //    4.2 OR relationship is directed.
        case _ => false
      }
    )
  }

  private def getRange(pr: PatternRelationship): Option[Option[Range]] = {
    val pos = InputPosition.NONE
    pr.length match {
      case SimplePatternLength => None
      case VarPatternLength(min, max) => Some(Some(Range(
          Some(UnsignedDecimalIntegerLiteral(min.toString)(pos)),
          max.map(i => UnsignedDecimalIntegerLiteral(i.toString)(pos))
        )(pos)))
    }
  }

  /**
   * This method converts a Stateful Shortest Path containing a VarLengthPattern into a FindShortestPaths plan if applicable.
   */
  private def sspVarLengthToFindShortest(
    statefulShortestPath: StatefulShortestPath,
    spp: SelectivePathPattern,
    pr: PatternRelationship
  ): Option[FindShortestPaths] = {
    val (fromNode, toNode) = pr.inOrder
    val shortestPathsPatternPart =
      createShortestPathsPatternPart(
        getRange(pr),
        fromNode,
        toNode,
        pr.variable,
        pr.dir,
        pr.types,
        !statefulShortestPath.selector.isGroup
      )
    val shortestRelPattern = ShortestRelationshipPattern(
      // We need to create a anonymous variable here since we get an error in runtimes other than interpreted otherwise
      Some(varFor(anonymousVariableNameGenerator.nextName)),
      pr,
      !statefulShortestPath.selector.isGroup
    )(shortestPathsPatternPart)

    val (nodePredicates, relationshipPredicates, _) = extractPredicates(
      spp.selections.flatPredicates,
      pr.variable,
      fromNode,
      toNode,
      targetNodeIsBound = true,
      // Length here will always be a VarPatternLength otherwise isVarlengthWithValidLength would not return a PatternRelationship.
      pr.length.asInstanceOf[VarPatternLength]
    )
    // 3.0 predicates needs to be inlineable in a legacy find shortest plan
    if ((nodePredicates ++ relationshipPredicates).size == withoutUniqueness(spp.selections.flatPredicates).size) {
      Some(FindShortestPaths(
        statefulShortestPath.source,
        shortestRelPattern,
        nodePredicates.toSeq,
        relationshipPredicates.toSeq,
        Seq.empty,
        withFallBack = false,
        AllowSameNode
      )(SameId(statefulShortestPath.id)))
    } else {
      None
    }
  }

  private def getRange(qpp: QuantifiedPathPattern): Option[Some[Range]] = {
    val pos = InputPosition.NONE
    Some(Some(Range(
      Some(UnsignedDecimalIntegerLiteral(qpp.repetition.min.toString)(pos)),
      qpp.repetition.max.limit.map(i => UnsignedDecimalIntegerLiteral(i.toString)(pos))
    )(pos)))
  }

  /**
   * 1.2 QPP is equivalent to a var-length relationship
   * For a qpp to be viable to convert to a varLengthPattern
   * - It can only contain one quantified relationship
   * - The minimum repetition must be 0 or 1
   * - The relationship must be directed OR the minimum repetition must be 0.
   */
  private def isValidQpp(qpp: QuantifiedPathPattern): Boolean =
    qpp.patternRelationships.size == 1 &&
      qpp.repetition.min < 2 &&
      (qpp.patternRelationships.exists(_.dir != BOTH) ||
        qpp.repetition.min == 0)

  private def findShortestFromQppShortest(
    selectivePathPattern: SelectivePathPattern,
    statefulShortestPath: StatefulShortestPath
  ): Option[FindShortestPaths] =
    exactlyOne(selectivePathPattern.pathPattern.allQuantifiedPathPatterns.toSeq)
      .filter(isValidQpp)
      .flatMap(qpp => sppQppToFindShortest(statefulShortestPath, selectivePathPattern, qpp))

  /**
   * This method converts a StatefulShortestPath containing a Quantified Path Pattern into a findShortest plan if applicable
   */
  private def sppQppToFindShortest(
    statefulShortestPath: StatefulShortestPath,
    spp: SelectivePathPattern,
    qpp: QuantifiedPathPattern
  ): Option[FindShortestPaths] = {
    val (nodePredicates, relationshipPredicates) = extractedPredicatesFromQpp(statefulShortestPath, spp, qpp)
    if (
      nodePredicates.size + relationshipPredicates.size == withoutUniqueness(
        spp.selections.flatPredicates
      ).size && qpp.relationshipVariableGroupings.size == 1
    ) {
      val innerRelationshipVariable = qpp.relationshipVariableGroupings.head.singleton
      convertToInlinedPredicates(
        outerStartNode = qpp.leftBinding.outer,
        innerStartNode = qpp.leftBinding.inner,
        innerEndNode = qpp.rightBinding.inner,
        outerEndNode = qpp.rightBinding.outer,
        innerRelationship = innerRelationshipVariable,
        predicatesToInline = nodePredicates ++ relationshipPredicates.map(rel =>
          rel.predicate.replaceAllOccurrencesBy(rel.variable, innerRelationshipVariable)
        ),
        predicatesOutsideRepetition =
          solveds.get(statefulShortestPath.source.id).asSinglePlannerQuery.queryGraph.selections.flatPredicates,
        pathDirection = qpp.patternRelationships.head.dir,
        pathRepetition = qpp.repetition,
        anonymousVariableNameGenerator = anonymousVariableNameGenerator,
        nodeToRelationshipRewriteOption = SkipRewriteOnZeroRepetitions
      )
        .flatMap(inlinedPredicates =>
          Some(FindShortestPaths(
            statefulShortestPath.source,
            shortestRelationshipPattern(statefulShortestPath, qpp),
            inlinedPredicates.nodePredicates,
            inlinedPredicates.relationshipPredicates,
            Seq.empty,
            withFallBack = false,
            AllowSameNode
          )(SameId(statefulShortestPath.id)))
        )
    } else {
      None
    }

  }

  /**
   * This method collects all predicates that can be inlined into a findShortestPaths plan splitting splitting them between
   * - Node predicates priorly inlined on the QPP
   * - Node predicates referencing the nodes in the Stateful Shortest Pattern
   * - Relationship predicates
   */
  private def extractedPredicatesFromQpp(
    statefulShortestPath: StatefulShortestPath,
    spp: SelectivePathPattern,
    qpp: QuantifiedPathPattern
  ): (Seq[Expression], RelationshipPredicates) = {
    val (innerFrom, innerTo) = (qpp.leftBinding.inner, qpp.rightBinding.inner)
    val extractedPredicates = extractQPPPredicates(
      spp.selections.flatPredicates,
      qpp.variableGroupings,
      statefulShortestPath.source.availableSymbols
    ).predicates
    val symbols =
      statefulShortestPath.source.availableSymbols + innerFrom + innerTo ++ qpp.relationshipVariableGroupings.headOption
        .map(_.singleton)

    val nodePredicates = extractedPredicates
      .map(_.extracted)
      .filter(_.dependencies.forall(symbols.contains))
      .filter(_.dependencies.exists(Set(innerFrom, innerTo).contains))

    val (pathBasedNodePredicates, relationshipPredicates, _) = extractPredicates(
      extractedPredicates.map(_.original),
      qpp.relationships.head,
      innerFrom,
      innerTo,
      targetNodeIsBound = true,
      VarPatternLength(qpp.repetition.min.toInt, qpp.repetition.max.limit.map(_.toInt))
    )

    (nodePredicates ++ pathBasedNodePredicates.map(_.predicate), relationshipPredicates)
  }

  /**
   * This method converts a StatefulShortestPath containing a Quantified Path Pattern into a Shortest Relationship Pattern,
   * changing the juxtaposed nodes into boundary nodes of the SRP and the Relationship to contain the group variable name
   * so that later filters can point to the relationship.
   */
  private def shortestRelationshipPattern(
    statefulShortestPath: StatefulShortestPath,
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
      !statefulShortestPath.selector.isGroup
    )
    val updatedQppPatternRelationship = qppPatternRelationship.copy(
      boundaryNodes = (outerFrom, outerTo),
      // This needs to be the group variable since post filters referencing the relationship will reference the group variable
      variable = qpp.relationshipVariableGroupings.head.group,
      length = varPatternLength
    )

    ShortestRelationshipPattern(
      // We need to create a anonymous variable here since we get an error in runtimes other than interpreted otherwise
      Some(varFor(anonymousVariableNameGenerator.nextName)),
      updatedQppPatternRelationship,
      !statefulShortestPath.selector.isGroup
    )(shortestPathsPatternPart)
  }

  private def exactlyOne[A](as: Iterable[A]): Option[A] =
    if (as.size == 1) as.headOption
    else None

  private def withoutUniqueness(expressions: Seq[Expression]): Seq[Expression] =
    expressions.filter {
      case far: ForAllRepetitions =>
        far.originalInnerPredicate match {
          case _: RelationshipUniquenessPredicate => false
          case _                                  => true
        }
      case _: RelationshipUniquenessPredicate => false
      case _: VarLengthBound                  => false
      case _                                  => true
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
  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
