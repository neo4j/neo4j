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
import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.SingleQuery
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.rewriting.conditions.containsNoReturnAll
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CancellationChecker
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.DefaultPostCondition
import org.neo4j.cypher.internal.util.helpers.fixedPoint
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.ParameterTypeInfo
import org.neo4j.cypher.internal.util.topDown

case object moveWithPastMatch extends StepSequencer.Step with DefaultPostCondition with ASTRewriterFactory {

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, ParameterTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
    cancellationChecker: CancellationChecker
  ): Rewriter = moveWithPastMatch(cancellationChecker).instance

  override def preConditions: Set[StepSequencer.Condition] = Set(
    containsNoReturnAll // It's better to know the variables in WITH already
  )

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    ProjectionClausesHaveSemanticInfo // It can invalidate this condition by copying WITH clauses
  )
}

/**
 * Rewrites `WITH 1 AS foo MATCH (x)` => `MATCH (x) WITH 1 AS foo, x`.
 *
 * This is beneficial for ordering optimizations of the planner which are harder to apply for all but the first QueryGraph.
 *
 * This could potentially move projections to a point of higher cardinality, but the cached properties mechanism
 * should take care that expensive projections are pushed down again.
 */
case class moveWithPastMatch(cancellationChecker: CancellationChecker) {

  private val subqueryRewriter: Rewriter = topDown(Rewriter.lift {
    case s: SubqueryCall =>
      s.copy(innerQuery = s.innerQuery.endoRewrite(innerRewriter(insideSubquery = true)))(s.position)
  })

  val instance: Rewriter = inSequence(innerRewriter(insideSubquery = false), subqueryRewriter)

  sealed private trait QuerySection {
    def clauses: Seq[Clause]
  }

  private case class MatchGroup(clauses: Seq[Match]) extends QuerySection {
    def containsOptionalMatch: Boolean = clauses.exists(_.optional)

    def usesVariableFromWith(mw: MovableWith): Boolean = {
      // We can be sure all return items are aliased at this point
      val withVars = mw.currentScope ++ mw.previousScope
      clauses.exists {
        m => m.folder.findAllByClass[LogicalVariable].exists(withVars)
      }
    }

    def allExportedVariablesAsReturnItems: Seq[AliasedReturnItem] =
      clauses.flatMap(_.allExportedVariables.map(v => AliasedReturnItem(v))).distinct
  }

  private case class MovableWith(`with`: With, previousScope: Set[LogicalVariable]) extends QuerySection {
    override def clauses: Seq[Clause] = Seq(`with`)
    def currentScope: Set[LogicalVariable] = `with`.returnItems.items.flatMap(_.alias).toSet
  }

  private case class OtherClause(clause: Clause) extends QuerySection {
    override def clauses: Seq[Clause] = Seq(clause)
  }

  private def innerRewriter(insideSubquery: Boolean): Rewriter = fixedPoint(cancellationChecker)(topDown(
    Rewriter.lift {
      case q: SingleQuery =>
        // Partition the clauses into sections
        val sections = q.clauses.foldLeft(Seq.empty[QuerySection]) {
          case (previousSections :+ (mg @ MatchGroup(matches)), m: Match) if !mg.containsOptionalMatch || m.optional =>
            // Add MATCH to previous MatchGroup
            previousSections :+ MatchGroup(matches :+ m)
          case (previousSections, m: Match) =>
            // New MatchGroup
            previousSections :+ MatchGroup(Seq(m))
          case (previousSections, w: With)
            if isMovableWith(w) && previousSections.forall(_.isInstanceOf[MovableWith]) =>
            // A with clause that can potentially be moved. Only if at beginning or after other movable WITHs.
            val previousScope = previousSections
              .lastOption
              .map(_.asInstanceOf[MovableWith].currentScope)
              .getOrElse(Set.empty)
            previousSections :+ MovableWith(w, previousScope)
          case (previousSections, clause) =>
            // New OtherClause
            previousSections :+ OtherClause(clause)
        }

        // Move WITHs around
        val newSections = sections.foldLeft(Seq.empty[QuerySection]) {
          case (previousSections :+ (mw @ MovableWith(w, _)), mg: MatchGroup)
            if !(insideSubquery && q.partitionedClauses.importingWith.contains(w)) &&
              !mg.usesVariableFromWith(mw) =>
            // The WITH can be moved past the MatchGroup
            val newWith = w.copy(returnItems =
              w.returnItems.copy(items =
                w.returnItems.items ++ mg.allExportedVariablesAsReturnItems
              )(w.returnItems.position)
            )(w.position)

            previousSections :+ mg :+ mw.copy(`with` = newWith)
          case (previousSections, section) =>
            previousSections :+ section
        }

        // Extract individual clauses again
        val newClauses = newSections.flatMap(_.clauses)
        q.copy(clauses = newClauses)(q.position)
    },
    stopper = _.isInstanceOf[SubqueryCall]
  ))

  private def isMovableWith(w: With): Boolean = {
    w.skip.isEmpty &&
    w.limit.isEmpty &&
    w.orderBy.isEmpty &&
    w.where.isEmpty &&
    !w.returnItems.includeExisting &&
    !w.returnItems.containsAggregate &&
    w.returnItems.isSimple &&
    !w.distinct
  }
}
