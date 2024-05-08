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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.AliasedReturnItem
import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PatternPartWithSelector
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoNodesOfType
import org.neo4j.cypher.internal.rewriting.rewriters.computeDependenciesForExpressions.ExpressionsHaveComputedDependencies
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
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
case class ReplacePatternComprehensionWithCollectSubquery(
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator
) {

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
          // It can also introduce new variables in the pattern, but if those have the same replacementName as an existing
          // variable, it is not shadowing but simply a reference.
          // In order to allow shadowing for the named path variable, we replace it (and all references to it)
          // with a new anonymous variable.
          val replacementName = anonymousVariableNameGenerator.nextName
          // a def so that we keep noReferenceEqualityAmongVariables
          def replacement = Variable(replacementName)(pathVar.position)
          val replaceNamedPathVar: Expression => Expression = _.replaceAllOccurrencesBy(pathVar, replacement)

          val newChain = applyExpressionRewriterInSimplePattern(pattern.element, replaceNamedPathVar)

          val part = NamedPatternPart(replacement, PathPatternPart(newChain))(pathVar.position)
          (part, replaceNamedPathVar)
        case None =>
          (PathPatternPart(pattern.element), (x: Expression) => x)
      }

      val patternForMatch = Pattern.ForMatch(Seq(
        PatternPartWithSelector(AllPaths()(pattern.position), patternPart)
      ))(pattern.position)

      val where = predicate.map(p => Where(replaceNamedPathVar(p))(p.position))

      val alias = Variable(anonymousVariableNameGenerator.nextName)(projection.position)
      val returnItem = AliasedReturnItem(replaceNamedPathVar(projection), alias)(projection.position)

      val query = SingleQuery(Seq(
        Match(
          optional = false,
          matchMode = MatchMode.default(pattern.position),
          pattern = patternForMatch,
          hints = Seq.empty,
          where = where
        )(pattern.position),
        Return(ReturnItems(
          includeExisting = false,
          items = Seq(returnItem)
        )(projection.position))(projection.position)
      ))(pc.position)

      CollectExpression(query)(pc.position, pc.computedIntroducedVariables, pc.computedScopeDependencies)
  }

  val instance: Rewriter = topDown(rewriter)
}

case object ReplacePatternComprehensionWithCollectSubquery extends Step with ASTRewriterFactory {

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = ReplacePatternComprehensionWithCollectSubquery(anonymousVariableNameGenerator).instance

  override def preConditions: Set[StepSequencer.Condition] = Set(
    // When rewriting `RETURN [...]`, we need to have given the ReturnItem an alias before rewriting it to COLLECT
    ReturnItemsAreAliased,
    // The call into Expression.replaceAllOccurrencesBy needs scopeDependencies to be computed.
    ExpressionsHaveComputedDependencies
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(containsNoNodesOfType[PatternComprehension]())

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    // It can invalidate this condition by rewriting things inside WITH/RETURN.
    ProjectionClausesHaveSemanticInfo
  )
}
