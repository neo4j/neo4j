/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.ast.convert.plannerQuery

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.CountExpression
import org.neo4j.cypher.internal.expressions.CountStar
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.ExistsExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ast.CountIRExpression
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.PatternDestructor
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.PatternElementDestructor
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.RelationshipChainDestructor
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.PredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

case class CreateIrExpressions(anonymousVariableNameGenerator: AnonymousVariableNameGenerator) extends Rewriter {
  private val pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression
  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)
  private val patternNormalizer = PredicateNormalizer.defaultNormalizer(anonymousVariableNameGenerator)
  private val addUniquenessPredicates = AddUniquenessPredicates

  /**
   * MatchPredicateNormalizer invalidates some conditions that are usually fixed by later rewriters.
   * The only one that is crucial to fix is that And => Ands and Or => Ors because that is assumed in IR creation.
   */
  private val fixExtractedPredicatesRewriter = flattenBooleanOperators

  private def createPathExpression(pattern: PatternExpression): PathExpression = {
    val pos = pattern.position
    val path = EveryPath(pattern.pattern.element)
    val step: PathStep = pathStepBuilder(path)
    PathExpression(step)(pos)
  }

  /**
   * Get the [[PlannerQuery]] for a pattern from a [[PatternExpression]] or [[PatternComprehension]] or [[ExistsExpression]].
   *
   * @param pattern        the pattern
   * @param dependencies   the dependencies or the expression
   * @param maybePredicate a WHERE clause predicate
   * @param horizon        the horizon to put into the query.
   */
  private def getPlannerQuery(
    pattern: ASTNode,
    dependencies: Set[String],
    maybePredicate: Option[Expression],
    horizon: QueryHorizon
  ): PlannerQuery = {
    val patternContent = pattern match {
      case relationshipsPattern: RelationshipsPattern =>
        relationshipsPattern.element.destructedRelationshipChain
      case patternList: Pattern =>
        patternList.destructed(anonymousVariableNameGenerator)
      case patternElement: PatternElement =>
        patternElement.destructed
      case _ =>
        throw new IllegalArgumentException(s"Cannot get planner query, unexpected pattern: $pattern")
    }

    // Create predicates for relationship uniqueness
    val uniqueRels = addUniquenessPredicates.collectUniqueRels(pattern)
    val uniquePredicates = addUniquenessPredicates.createPredicatesFor(uniqueRels, pattern.position)
    // Extract inlined predicates
    val extractedPredicates: Seq[Expression] =
      patternNormalizer.extractAllFrom(pattern).endoRewrite(fixExtractedPredicatesRewriter)

    val qg = QueryGraph(
      argumentIds = dependencies,
      patternNodes = patternContent.nodeIds.toSet,
      patternRelationships = patternContent.rels.toSet,
      quantifiedPathPatterns = patternContent.quantifiedPathPatterns.toSet,
      shortestPathPatterns =
        patternContent.shortestPaths.toSet, // Not really needed, PatternExpressions/PatternComprehension can't express shortestPath
      selections = Selections.from(uniquePredicates ++ extractedPredicates ++ maybePredicate)
    )

    val query = RegularSinglePlannerQuery(
      queryGraph = qg,
      horizon = horizon
    )
    val finalizedQuery = PlannerQueryBuilder.finalizeQuery(query)
    PlannerQuery(finalizedQuery)
  }

  private val instance: Rewriter = topDown(Rewriter.lift {
    /**
     * Rewrites exists( (n)-[anon_0]->(anon_1:M) ) into
     * IR for MATCH (n)-[anon_0]->(anon_1:M)
     */
    case exists @ Exists(pe @ PatternExpression(pattern)) =>
      val existsVariableName = anonymousVariableNameGenerator.nextName
      val query = getPlannerQuery(pattern, pe.dependencies.map(_.name), None, RegularQueryProjection())
      ExistsIRExpression(query, existsVariableName, stringifier(exists))(exists.position)

    /**
     * Rewrites exists{ (n)-[anon_0]->(anon_1:M)} into
     * IR for MATCH (n)-[anon_0]->(anon_1:M)
     *
     */
    case existsExpression @ ExistsExpression(pattern, optionalWhereExpression) =>
      val existsVariableName = anonymousVariableNameGenerator.nextName
      val query = getPlannerQuery(
        pattern,
        existsExpression.dependencies.map(_.name),
        optionalWhereExpression,
        RegularQueryProjection()
      )
      ExistsIRExpression(query, existsVariableName, stringifier(existsExpression))(existsExpression.position)

    /**
     * Rewrites (n)-[anon_0]->(anon_1:M) into
     * IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN PathExpression(NodePathStep(n, RelationshipPathStep(anon_0, NodePathStep(anon_1, NilPathStep))))
     */
    case pe @ PatternExpression(pattern) =>
      val variableToCollectName = anonymousVariableNameGenerator.nextName
      val collectionName = anonymousVariableNameGenerator.nextName

      val pathExpression = createPathExpression(pe)
      val query = getPlannerQuery(
        pattern,
        pe.dependencies.map(_.name),
        None,
        RegularQueryProjection(Map(variableToCollectName -> pathExpression))
      )
      ListIRExpression(query, variableToCollectName, collectionName, stringifier(pe))(pe.position)

    /**
     * namedPaths in PatternComprehensions have already been rewritten away by [[inlineNamedPathsInPatternComprehensions]].
     * Rewrites [(n)-->(:M) WHERE n.prop > 0 | n.foo] into
     * IR for MATCH (n)-->(:M) WHERE n.prop > 0 RETURN n.foo
     */
    case pc @ PatternComprehension(None, pattern, predicate, projection) =>
      val variableToCollectName = anonymousVariableNameGenerator.nextName
      val collectionName = anonymousVariableNameGenerator.nextName

      val query = getPlannerQuery(
        pattern,
        pc.dependencies.map(_.name),
        predicate,
        RegularQueryProjection(Map(variableToCollectName -> projection))
      )
      ListIRExpression(query, variableToCollectName, collectionName, stringifier(pc))(pc.position)

    /**
     * Rewrites COUNT { (n)-[anon_0]->(anon_1:M) } into
     * IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN count(*)
     */
    case ce @ CountExpression(pattern, where) =>
      val countVariableName = anonymousVariableNameGenerator.nextName

      val query = getPlannerQuery(
        pattern,
        ce.dependencies.map(_.name),
        where,
        AggregatingQueryProjection(aggregationExpressions = Map(countVariableName -> CountStar()(ce.position)))
      )

      CountIRExpression(
        query = query,
        countVariableName = countVariableName,
        solvedExpressionAsString = stringifier(ce)
      )(ce.position)

    case PatternComprehension(Some(_), _, _, _) =>
      throw new IllegalStateException(
        "namedPaths in PatternComprehensions should already have been rewritten away by inlineNamedPathsInPatternComprehensions."
      )
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
