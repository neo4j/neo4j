/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart.SelectiveSelector
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodes.QppPredicatesCopiedToJuxtaposedNodes
import org.neo4j.cypher.internal.frontend.phases.factories.PlanPipelineTransformerFactory
import org.neo4j.cypher.internal.rewriting.conditions.AndRewrittenToAnds
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.NoNodeOrRelationshipPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.ParenthesizedPathUnwrapped
import org.neo4j.cypher.internal.rewriting.rewriters.QppsHavePaddedNodes
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

import scala.collection.immutable.ListSet

case object MoveBoundaryNodePredicates extends StatementRewriter with StepSequencer.Step
    with PlanPipelineTransformerFactory {

  case object BoundaryNodePredicatesMoved extends StepSequencer.Condition

  override def preConditions: Set[StepSequencer.Condition] = Set(
    NoNodeOrRelationshipPredicates,
    AndRewrittenToAnds,
    QppsHavePaddedNodes,
    QppPredicatesCopiedToJuxtaposedNodes,
    ParenthesizedPathUnwrapped
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(BoundaryNodePredicatesMoved)

  override def invalidatedConditions: Set[StepSequencer.Condition] = SemanticInfoAvailable

  private val rewriter: Rewriter = topDown(Rewriter.lift {
    case matchClause @ Match(_, _, pattern @ Pattern.ForMatch(parts), _, where) =>
      val (newParts, extractedPredicates) = parts.map {
        case patternPart @ PatternPartWithSelector(_: SelectiveSelector, part) =>
          val (newElement: PatternElement, extractedPredicates: ListSet[Expression]) = part.element match {
            case pp @ ParenthesizedPath(part, Some(where)) =>
              val element = part.element
              val boundaryNodes = PatternElement.boundaryNodes(element)
              val (extractedPredicates, notExtractedPredicates) = extractPredicates(where, boundaryNodes)
              (pp.copy(optionalWhereClause = notExtractedPredicates)(pp.position), extractedPredicates)
            case element => (element, ListSet.empty)
          }
          (patternPart.replaceElement(newElement), extractedPredicates)
        case other => (other, ListSet.empty)
      }.unzip
      val newPattern = Pattern.ForMatch(newParts)(pattern.position)
      matchClause.copy(
        pattern = newPattern,
        where = Where.combineOrCreate(where, extractedPredicates.view.flatten.to(ListSet))(matchClause.position)
      )(matchClause.position)
  })

  /**
   * Extract predicates from an expression that only depend on the given boundary nodes.
   *
   * @return a tuple of, first all extracted predicates as a ListSet, and then all not-extracted predicates, already combined into one Option[Expression].
   */
  private def extractPredicates(
    where: Expression,
    boundaryNodes: Set[LogicalVariable]
  ): (ListSet[Expression], Option[Expression]) = {
    where match {
      case Ands(innerPredicates) =>
        val (extracted, notExtracted) = innerPredicates.partition(_.dependencies.subsetOf(boundaryNodes))
        val notExtractedAnds = Option.when(notExtracted.nonEmpty)(Ands.create(ListSet.from(notExtracted)))
        (ListSet.from(extracted), notExtractedAnds)
      case singlePredicate if singlePredicate.dependencies.subsetOf(boundaryNodes) => (ListSet(singlePredicate), None)
      case _                                                                       => (ListSet.empty, Some(where))
    }
  }

  override def instance(from: BaseState, context: BaseContext): Rewriter = rewriter

  override def getTransformer(
    pushdownPropertyReads: Boolean,
    semanticFeatures: Seq[SemanticFeature]
  ): Transformer[_ <: BaseContext, _ <: BaseState, BaseState] = this
}
