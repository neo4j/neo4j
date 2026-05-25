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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.FreeProjection
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase.AST_REWRITE
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.frontend.phases.Transformer
import org.neo4j.cypher.internal.frontend.phases.factories.ParsePipelineTransformerFactory
import org.neo4j.cypher.internal.frontend.phases.factories.ParsingConfig
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.IsolateSubqueriesInMutatingPatterns.SubqueriesInMutatingPatternsIsolated
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping.UpToDateScopes
import org.neo4j.cypher.internal.rewriting.conditions.ContainsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.conditions.SemanticInfoAvailable
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.rewriting.rewriters.preparatoryRewriters.ReturnItemsAreAliased
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.topDown

/**
 * Replaces
 * {{{
 * [p = (n)--(m) WHERE n.prop > 0 | n.prop]
 * }}}
 * with
 * {{{
 * COLLECT { MATCH p = (n)--(m) WHERE n.prop > 0 RETURN n.prop AS anon_0 }
 * }}}
 */
case class ReplacePatternComprehensionWithCollectSubqueryRewriter(anonVarGen: AnonymousVariableNameGenerator) {

  private def applyExpressionRewriterInSimplePattern(
    pat: SimplePattern,
    expressionRewriter: Expression => Expression
  ): SimplePattern = {
    pat match {
      case rc: RelationshipChain =>
        rc.copy(
          element = applyExpressionRewriterInSimplePattern(rc.element, expressionRewriter),
          relationship = rc.relationship.copy(
            properties = rc.relationship.properties.map(expressionRewriter),
            predicate = rc.relationship.predicate.map(expressionRewriter)
          )(rc.relationship.position),
          rightNode = applyExpressionRewriterInSimplePattern(rc.rightNode, expressionRewriter).asInstanceOf[NodePattern]
        )(rc.position)
      case np: NodePattern =>
        np.copy(
          properties = np.properties.map(expressionRewriter),
          predicate = np.predicate.map(expressionRewriter)
        )(np.position)
    }
  }

  private val rewriter = Rewriter.lift {
    case pc @ PatternComprehension(namedPath, pattern, predicate, projection) =>
      val (patternPart, replaceNamedPathVar) = namedPath match {
        case Some(pathVar) =>
          // COLLECT subqueries are not allowed to shadow existing variables, but PatternComprehensions are allowed.
          // The only place where a PatternComprehension can shadow an existing variable is the named path.
          // Variables in the pattern of a PatternComprehension that have the same name as a
          // variable from outer scope do not shadowing that variable but simply reference it.
          // In order to allow shadowing for the named path variable, we replace it (and all references to it)
          // with a new anonymous variable.
          val replacementName = anonVarGen.nextName
          // a def so that we keep noReferenceEqualityAmongVariables
          def replacement = Variable(replacementName)(pathVar.position, Variable.isIsolatedDefault)
          val replaceNamedPathVar: Expression => Expression = _.replaceAllOccurrencesBy(pathVar, replacement, true)

          val newChain = applyExpressionRewriterInSimplePattern(pattern.element, replaceNamedPathVar)

          val part = NamedPatternPart(replacement, PathPatternPart(newChain))(pathVar.position)
          (part, replaceNamedPathVar)
        case None =>
          (PathPatternPart(pattern.element), (x: Expression) => x)
      }

      val patternForMatch =
        Pattern.ForMatch(Seq(PrefixedPatternPart(patternPart)(pattern.position)))(pattern.position)

      val where = predicate.map(p => Where(replaceNamedPathVar(p))(p.position))

      val alias = Variable(anonVarGen.nextName)(projection.position, Variable.isIsolatedDefault)
      val returnItem = AliasedReturnItem(replaceNamedPathVar(projection), alias)(projection.position)

      val query = SingleQuery(Seq(
        Match(
          optional = false,
          matchMode = MatchMode.default(pattern.position),
          pattern = patternForMatch,
          hints = Seq.empty,
          where = where,
          search = None
        )(pattern.position),
        Return(ReturnItems(
          FreeProjection,
          items = Seq(returnItem)
        )(projection.position))(projection.position)
      ))(pc.position)

      CollectExpression(query)(pc.position, None, None)
  }

  val instance: Rewriter = topDown(rewriter)
}

case object ReplacePatternComprehensionWithCollectSubqueryRewriter extends Phase[BaseContext, BaseState, BaseState]
    with StepSequencer.Step
    with ParsePipelineTransformerFactory {

  def phase: CompilationPhaseTracer.CompilationPhase = AST_REWRITE

  def process(from: BaseState, context: BaseContext): BaseState =
    from.withStatement(
      from.statement().endoRewrite(
        ReplacePatternComprehensionWithCollectSubqueryRewriter(from.anonymousVariableNameGenerator).instance
      )
    )

  override def getTransformer(config: ParsingConfig): Transformer[BaseContext, BaseState, BaseState] = this

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // When rewriting `RETURN [...]`, we need to have given the ReturnItem an alias before rewriting it to COLLECT
    ReturnItemsAreAliased,
    SubqueriesInMutatingPatternsIsolated
  )

  override def postConditions: Set[StepSequencer.Condition] =
    Set(ContainsNoNodesOfType[PatternComprehension]())

  override def invalidatedConditions: Set[StepSequencer.Condition] =
    Set(
      // It can invalidate this condition by rewriting things inside WITH/RETURN.
      ExpressionsHaveComputedDependencies,
      UpToDateScopes
    ) ++ SemanticInfoAvailable

}
