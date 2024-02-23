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

import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.expressions._
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter.RewritableMatchClause
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter.RewritableParenthesizedPath
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter.RewritablePredicate
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter.RewritableQuantifiedPath
import org.neo4j.cypher.internal.frontend.phases.CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter.extractRewritableQppPredicates
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.topDown

/**
 * Before
 *   MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b)
 *   RETURN *
 *
 * After
 *   MATCH (a) ((n)-[r]->(m) WHERE n.p = 0)+ (b)
 *   WHERE a.p = 0
 *   RETURN *
 *
 * The contract of a QPP dictates that the juxtaposed nodes must be equal to the inner nodes of the QPP. Thus, we know
 * that if predicates apply to the inner nodes of a QPP, then the predicates can also be applied to the juxtaposed
 * nodes of a QPP. Under such circumstances, it is an optimisation to copy the predicates to the juxtaposed nodes as it
 * enables logical planning to pushdown predicates into the LHS of Trail.
 *
 * It is tempting to rewrite the cases below, but such cases are unlikely to be an optimisation given that there is no
 * efficient way to plan TrailInto.
 *
 *   MATCH (a) ((n)-[r]->(m) WHERE n.p = m.p)+ (b)
 *   // MATCH (a) ((n)-[r]->(m) WHERE n.p = m.p)+ (b) WHERE a.p = b.p
 *
 * We do not rewrite QPPs that can be empty (eg if it has a lower bound of 0). This is because if the QPP can be empty,
 * then the inner boundary nodes are no longer guaranteed to always be equal to the juxtaposed nodes of the QPP given
 * that the inner boundary nodes may not exist.
 *
 * We are able to rewrite most predicates even if they introduce scoped variables that either shadow the existing
 * variables, or would shadow variables after being rewritten. The method Expression.replaceAllOccurrencesBy, in
 * conjunction with the namespacer, do a lot of the heavy lifting for us.
 */
case class CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter private () {

  val rewriter: Rewriter = topDown(Rewriter.lift {
    case RewritableMatchClause(matchClause, allRewritableQPPs) =>
      val rewrittenPredicates = rewritePredicates(allRewritableQPPs)
      val rewrittenWhere = Where.combineOrCreate(matchClause.where, rewrittenPredicates)(matchClause.position)
      matchClause.copy(where = rewrittenWhere)(matchClause.position)

    case RewritableParenthesizedPath(parenthesizedPath, allRewritableQPPs) =>
      val rewrittenPredicates = rewritePredicates(allRewritableQPPs)
      val rewrittenWhere = Where.combineOrCreate(parenthesizedPath.optionalWhereClause, rewrittenPredicates)
      parenthesizedPath.copy(optionalWhereClause = rewrittenWhere)(parenthesizedPath.position)
  })

  private def rewritePredicates(allRewritableQpps: Seq[RewritableQuantifiedPath]): ListSet[Expression] = {
    allRewritableQpps
      .to(ListSet)
      .flatMap(extractRewritableQppPredicates)
      .map(rewritePredicate)
  }

  private def rewritePredicate(predicate: RewritablePredicate): Expression = {
    val RewritablePredicate(outer, inner, innerPredicate) = predicate
    innerPredicate.replaceAllOccurrencesBy(inner, outer)
  }
}

case object CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter {

  val instance: Rewriter = CopyQuantifiedPathPatternPredicatesToJuxtaposedNodesRewriter().rewriter

  case class RewritableQuantifiedPath(
    left: LogicalVariable,
    right: LogicalVariable,
    innerPattern: RelationshipChain,
    innerPredicate: Expression
  )

  case class RewritablePredicate(
    outer: LogicalVariable,
    inner: LogicalVariable,
    innerPredicate: Expression
  )

  case object RewritableMatchClause {

    def unapply(matchClause: Match): Option[(Match, Seq[RewritableQuantifiedPath])] = {
      val allRewritableQpps = matchClause.folder.treeFold(Seq.empty[RewritableQuantifiedPath]) {
        case pathConcatenation: PathConcatenation =>
          acc =>
            val rewritableQpps = extractRewritableQpps(pathConcatenation)
            SkipChildren(acc ++ rewritableQpps)

        // parenthesized paths may contain their own QPP that we do not want to copy to this MATCH clause
        case _: ParenthesizedPath =>
          acc =>
            SkipChildren(acc)

        // scope expressions may contain their own QPP that we do not want to copy to this MATCH clause
        case _: ScopeExpression =>
          acc =>
            SkipChildren(acc)
      }
      Some((matchClause, allRewritableQpps))
    }
  }

  case object RewritableParenthesizedPath {

    def unapply(parenthesizedPath: ParenthesizedPath): Option[(ParenthesizedPath, Seq[RewritableQuantifiedPath])] = {
      val allRewritableQpps = parenthesizedPath.folder.treeFold(Seq.empty[RewritableQuantifiedPath]) {
        case pathConcatenation: PathConcatenation =>
          acc =>
            val rewritableQpps = extractRewritableQpps(pathConcatenation)
            SkipChildren(acc ++ rewritableQpps)

        // scope expressions may contain their own QPP that we do not want to copy to this MATCH clause
        case _: ScopeExpression =>
          acc =>
            SkipChildren(acc)
      }
      Some((parenthesizedPath, allRewritableQpps))
    }
  }

  private def extractRewritableQpps(pathConcatenation: PathConcatenation): Seq[RewritableQuantifiedPath] =
    pathConcatenation.factors.sliding(3).toSeq.collect {
      case Seq(
          leftPattern: SimplePattern,
          QuantifiedPath(PathPatternPart(innerPattern: RelationshipChain), quantifier, Some(innerPredicate), _),
          rightPattern: SimplePattern
        ) if !quantifier.canBeEmpty =>
        val left = leftPattern.allTopLevelVariablesLeftToRight.last
        val right = rightPattern.allTopLevelVariablesLeftToRight.head
        RewritableQuantifiedPath(left, right, innerPattern, innerPredicate)
    }

  private def extractRewritableQppPredicates(rewritableQpp: RewritableQuantifiedPath): ListSet[RewritablePredicate] = {
    val RewritableQuantifiedPath(left, right, innerPattern, innerPredicates) = rewritableQpp
    val innerLeft = innerPattern.allTopLevelVariablesLeftToRight.head
    val innerRight = innerPattern.allTopLevelVariablesLeftToRight.last
    val innerVariables = innerPattern.allVariables

    innerPredicates.folder.treeFold(ListSet.empty[RewritablePredicate]) {
      case _: Ands =>
        acc =>
          TraverseChildren(acc)
      case predicate: Expression if isPredicateRewritable(predicate, innerVariables, innerLeft) =>
        acc =>
          SkipChildren(acc + RewritablePredicate(left, innerLeft, predicate))
      case predicate: Expression if isPredicateRewritable(predicate, innerVariables, innerRight) =>
        acc =>
          SkipChildren(acc + RewritablePredicate(right, innerRight, predicate))
      case _: Expression =>
        acc =>
          SkipChildren(acc)
    }
  }

  private def isPredicateRewritable(
    predicate: Expression,
    allInnerVariables: Set[LogicalVariable],
    innerSideVariable: LogicalVariable
  ): Boolean = {
    predicate.dependencies.intersect(allInnerVariables) == Set(innerSideVariable) && !predicate.folder.treeExists {
      case _: FullSubqueryExpression => true
    }
  }
}
