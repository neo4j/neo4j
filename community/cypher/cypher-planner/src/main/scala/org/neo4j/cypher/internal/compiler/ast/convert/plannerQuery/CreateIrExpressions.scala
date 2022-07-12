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
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.PathStep
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternComprehension
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.frontend.phases.rewriting.cnf.flattenBooleanOperators
import org.neo4j.cypher.internal.ir.PlannerQuery
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryHorizon
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ast.ExistsIRExpression
import org.neo4j.cypher.internal.ir.ast.ListIRExpression
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.PatternDestructor
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.PatternElementDestructor
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.RelationshipChainDestructor
import org.neo4j.cypher.internal.rewriting.rewriters.AddUniquenessPredicates
import org.neo4j.cypher.internal.rewriting.rewriters.MatchPredicateNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.inlineNamedPathsInPatternComprehensions
import org.neo4j.cypher.internal.rewriting.rewriters.projectNamedPaths
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.topDown

case class CreateIrExpressions(anonymousVariableNameGenerator: AnonymousVariableNameGenerator) extends Rewriter {
  private val pathStepBuilder: EveryPath => PathStep = projectNamedPaths.patternPartPathExpression
  private val stringifier = ExpressionStringifier(_.asCanonicalStringVal)
  private val patternNormalizer = MatchPredicateNormalizer.defaultNormalizer(anonymousVariableNameGenerator)
  private val addUniquenessPredicates = AddUniquenessPredicates(anonymousVariableNameGenerator)

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
   * Get the [[PlannerQuery]] for a pattern from a [[PatternExpression]] or [[PatternComprehension]] or [[ExistsSubClause]].
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
      shortestPathPatterns =
        patternContent.shortestPaths.toSet, // Not really needed, PatternExpressions/PatternComprehension can't express shortestPath
      selections = Selections.from(uniquePredicates ++ extractedPredicates ++ maybePredicate)
    )

    PlannerQuery(RegularSinglePlannerQuery(
      queryGraph = qg,
      horizon = horizon
    ))
  }

  private val instance: Rewriter = topDown(Rewriter.lift {
    /**
     * Rewrites exists( (n)-[anon_0]->(anon_1:M) ) into
     * IR for MATCH (n)-[anon_0]->(anon_1:M)
     */
    case exists @ Exists(pe @ PatternExpression(pattern)) =>
      val query = getPlannerQuery(pattern, pe.dependencies.map(_.name), None, RegularQueryProjection())
      ExistsIRExpression(query, stringifier(exists))(exists.position)

    /**
     * Rewrites exists{ (n)-[anon_0]->(anon_1:M)} into
     * IR for MATCH (n)-[anon_0]->(anon_1:M)
     *
     */
    case existsSubClause @ ExistsSubClause(pattern, optionalWhereExpression) =>
      val query = getPlannerQuery(
        pattern,
        existsSubClause.dependencies.map(_.name),
        optionalWhereExpression,
        RegularQueryProjection()
      )
      ExistsIRExpression(query, stringifier(existsSubClause))(existsSubClause.position)

    /**
     * Rewrites (n)-[anon_0]->(anon_1:M) into
     * IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN PathExpression(NodePathStep(n, RelationshipPathStep(anon_0, NodePathStep(anon_1, NilPathStep))))
     */
    case pe @ PatternExpression(pattern) =>
      val pathExpression = createPathExpression(pe)
      val query = getPlannerQuery(
        pattern,
        pe.dependencies.map(_.name),
        None,
        RegularQueryProjection(Map(pe.variableToCollectName -> pathExpression))
      )
      ListIRExpression(query, pe.variableToCollectName, pe.collectionName, stringifier(pe))(pe.position)

    /**
     * namedPaths in PatternComprehensions have already been rewritten away by [[inlineNamedPathsInPatternComprehensions]].
     * Rewrites [(n)-->(:M) WHERE n.prop > 0 | n.foo] into
     * IR for MATCH (n)-->(:M) WHERE n.prop > 0 RETURN n.foo
     */
    case pc @ PatternComprehension(None, pattern, predicate, projection) =>
      val query = getPlannerQuery(
        pattern,
        pc.dependencies.map(_.name),
        predicate,
        RegularQueryProjection(Map(pc.variableToCollectName -> projection))
      )
      ListIRExpression(query, pc.variableToCollectName, pc.collectionName, stringifier(pc))(pc.position)

    /**
     * Rewrites COUNT { (n)-[anon_0]->(anon_1:M) } into
     * Size(ListIRExpression(IR for MATCH (n)-[anon_0]->(anon_1:M) RETURN 1))
     */
    case ce @ CountExpression(patternElement, where) =>
      val variableToCollect = anonymousVariableNameGenerator.nextName
      val collectionName = anonymousVariableNameGenerator.nextName
      val projection = SignedDecimalIntegerLiteral("1")(ce.position)

      val query = getPlannerQuery(
        patternElement,
        ce.dependencies.map(_.name),
        where,
        RegularQueryProjection(Map(variableToCollect -> projection))
      )

      Size(
        ListIRExpression(
          query = query,
          variableToCollectName = variableToCollect,
          collectionName = collectionName,
          solvedExpressionAsString = stringifier(ce)
        )(ce.position)
      )(ce.position)

    case PatternComprehension(Some(_), _, _, _) =>
      throw new IllegalStateException(
        "namedPaths in PatternComprehensions should already have been rewritten away by inlineNamedPathsInPatternComprehensions."
      )
  })

  override def apply(input: AnyRef): AnyRef = instance.apply(input)
}
